"""Tests for DAG maintenance logic.

Tests the database operations (insert/query summaries) without LLM calls.
The summarization functions are mocked.
"""

import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from ingest_sessions.core import create_tables
from ingest_sessions.dag import (
    SPRIG_CHUNK_SIZE,
    get_latest_summary_for_session,
    get_sprigs_for_session,
    get_unsummarized_messages,
    insert_bindle,
    insert_sprig,
    run_summarize_session,
)


@pytest.fixture
def db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(conn)
    return conn


def _seed_records(
    db: duckdb.DuckDBPyConnection, session_id: str, count: int
) -> list[str]:
    """Insert count records and return their UUIDs."""
    uuids = []
    for i in range(count):
        uuid = f"rec-{session_id}-{i:04d}"
        raw = json.dumps(
            {
                "message": {
                    "role": "user" if i % 2 == 0 else "assistant",
                    "content": f"Message {i} content here",
                },
            }
        )
        db.execute(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            [
                uuid,
                session_id,
                "user" if i % 2 == 0 else "assistant",
                f"2026-03-01T{10 + i // 60:02d}:{i % 60:02d}:00.000Z",
                None,
                raw,
            ],
        )
        uuids.append(uuid)
    return uuids


def test_get_unsummarized_messages_all_new(db: duckdb.DuckDBPyConnection):
    """All messages are unsummarized when no summaries exist."""
    uuids = _seed_records(db, "sess-1", 10)
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 10
    assert result[0]["uuid"] == uuids[0]


def test_get_unsummarized_messages_some_covered(db: duckdb.DuckDBPyConnection):
    """Messages covered by a sprig are excluded."""
    uuids = _seed_records(db, "sess-1", 10)
    insert_sprig(db, "sess-1", "test summary", uuids[:5], [])
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 5
    result_uuids = [r["uuid"] for r in result]
    assert uuids[5] in result_uuids
    assert uuids[0] not in result_uuids


def test_insert_sprig(db: duckdb.DuckDBPyConnection):
    """insert_sprig creates a sprig row with correct fields."""
    uuids = _seed_records(db, "sess-1", 5)
    sid = insert_sprig(db, "sess-1", "summary content", uuids, ["file_abc"])
    assert sid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [sid]).fetchone()
    assert row is not None
    assert row[1] == "sess-1"  # session_id
    assert row[2] == "sprig"  # kind


def test_insert_bindle(db: duckdb.DuckDBPyConnection):
    """insert_bindle creates a bindle row referencing parent sprigs."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:], [])
    bid = insert_bindle(db, "sess-1", "condensed content", [s1, s2], [])
    assert bid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [bid]).fetchone()
    assert row is not None
    assert row[2] == "bindle"  # kind
    assert row[3] == 2  # condensation_order


def test_get_sprigs_for_session(db: duckdb.DuckDBPyConnection):
    """get_sprigs_for_session returns sprigs not yet condensed into bindles."""
    uuids = _seed_records(db, "sess-1", 15)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:10], [])
    s3 = insert_sprig(db, "sess-1", "summary 3", uuids[10:], [])
    # Condense s1 and s2 into a bindle
    insert_bindle(db, "sess-1", "condensed", [s1, s2], [])
    # Only s3 should be uncondensed
    uncondensed = get_sprigs_for_session(db, "sess-1", uncondensed_only=True)
    assert len(uncondensed) == 1
    assert uncondensed[0]["summary_id"] == s3


def test_get_latest_summary_for_session(db: duckdb.DuckDBPyConnection):
    """Returns the highest-order summary (bindle > sprig) for context assembly."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "sprig 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "sprig 2", uuids[5:], [])
    insert_bindle(db, "sess-1", "the bindle", [s1, s2], [])
    latest = get_latest_summary_for_session(db, "sess-1")
    assert latest is not None
    assert latest["kind"] == "bindle"
    assert latest["content"] == "the bindle"


def test_run_summarize_session_creates_sprigs(db: duckdb.DuckDBPyConnection):
    """run_summarize_session chunks messages into sprigs."""
    _seed_records(db, "sess-1", SPRIG_CHUNK_SIZE * 2 + 3)
    with patch("ingest_sessions.dag.summarize_messages", return_value="mock summary"):
        result = run_summarize_session(db, "sess-1")
    assert result["sprigs_created"] == 2  # 2 full chunks, remainder < chunk size
    sprigs = get_sprigs_for_session(db, "sess-1")
    assert len(sprigs) == 2
