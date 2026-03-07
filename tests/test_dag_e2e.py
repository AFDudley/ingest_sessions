"""E2E test for the DAG round-trip.

Seeds messages, runs summarize (with mock LLM), verifies context assembly.
Uses direct DB access for the summarization step (since the server runs
in a subprocess where mocks can't be applied), then verifies context
retrieval through the MCP server.
"""

import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from ingest_sessions.core import create_tables, ingest_jsonl
from ingest_sessions.dag import assemble_context_for_session, run_summarize_session
from tests.test_server import _text, run_server


def _make_records(session_id: str, count: int) -> list[dict]:
    """Generate count realistic JSONL records."""
    records = []
    for i in range(count):
        role = "user" if i % 2 == 0 else "assistant"
        records.append(
            {
                "uuid": f"{session_id}-{i:04d}",
                "sessionId": session_id,
                "type": role,
                "timestamp": f"2026-03-01T{10 + i // 60:02d}:{i % 60:02d}:00.000Z",
                "parentUuid": None,
                "message": {
                    "role": role,
                    "content": f"{'User asks about' if role == 'user' else 'Assistant explains'} topic {i}. "
                    f"This involves file src/module_{i}.py and function process_{i}().",
                },
            }
        )
    return records


def test_full_dag_roundtrip(tmp_path: Path):
    """Seed 50 messages, summarize with mocked LLM, verify context assembly."""
    session_id = "sess-dag-test"
    records = _make_records(session_id, 50)

    # Write JSONL and ingest directly into a DB
    jsonl_path = tmp_path / f"{session_id}.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(r) for r in records) + "\n")

    db = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(db)
    ingest_jsonl(db, jsonl_path)

    # Verify records were ingested
    row = db.execute(
        "SELECT count(*) FROM records WHERE session_id = ?", [session_id]
    ).fetchone()
    assert row is not None
    assert row[0] == 50

    # Summarize with mocked LLM
    with patch(
        "ingest_sessions.dag.summarize_messages",
        return_value="Mock sprig summary of conversation segment.",
    ):
        result = run_summarize_session(db, session_id)
    assert result["sprigs_created"] >= 1

    # Run again — if enough sprigs, condense into bindle
    with patch(
        "ingest_sessions.dag.summarize_messages",
        return_value="Mock sprig summary of conversation segment.",
    ):
        with patch(
            "ingest_sessions.dag.condense_summaries",
            return_value="Mock bindle: condensed overview of all work.",
        ):
            run_summarize_session(db, session_id)

    # Verify context assembly
    ctx = assemble_context_for_session(db, session_id)
    assert ctx is not None
    assert "continued after a /clear" in ctx
    assert "Summary ID:" in ctx
    db.close()


@pytest.mark.asyncio
async def test_context_empty_session(tmp_path: Path):
    """Context for a session with no summaries returns None."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool("context", {"session_id": "nonexistent"})
        body = json.loads(_text(result))
        assert body["context"] is None
