#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c6.

Places a subagent transcript file named after its agent id beside its
parent omp session file, without telling the ingester about it directly,
and observes: running ingestion over the parent session finds that
nested subagent file on its own and ingests its records. Judges nothing
-- prints one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import discover_subagent_files, ingest_omp_session


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        parent_header = {
            "type": "session",
            "version": 3,
            "id": "omp-parent-sess",
            "cwd": "/repo",
            "title": "parent",
        }
        parent_entry = {
            "id": "omp-parent-e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "delegate to a subagent"},
        }
        parent_path = tmp_path / "omp-parent-sess.jsonl"
        parent_path.write_text(
            "\n".join(json.dumps(x) for x in [parent_header, parent_entry]) + "\n"
        )

        # Subagent transcript at <session>/<AgentId>.jsonl -- discovered
        # implicitly, never referenced by the caller below.
        subagent_dir = tmp_path / "omp-parent-sess"
        subagent_dir.mkdir()
        agent_id = "agent-xyz"
        subagent_header = {
            "type": "session",
            "version": 3,
            "id": agent_id,
            "cwd": "/repo",
            "title": "subagent",
            "parentSession": parent_header["id"],
        }
        subagent_entries = [
            {
                "id": "omp-sub-e1",
                "parentId": None,
                "type": "message",
                "timestamp": "2026-01-01T00:00:30.000Z",
                "message": {"role": "assistant", "content": "subagent working"},
            },
            {
                "id": "omp-sub-e2",
                "parentId": "omp-sub-e1",
                "type": "message",
                "timestamp": "2026-01-01T00:01:00.000Z",
                "message": {"role": "assistant", "content": "subagent done"},
            },
        ]
        subagent_path = subagent_dir / f"{agent_id}.jsonl"
        subagent_path.write_text(
            "\n".join(json.dumps(x) for x in [subagent_header, *subagent_entries])
            + "\n"
        )

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        discovered = discover_subagent_files(parent_path)
        subagent_file_discovered = subagent_path in discovered

        ingest_omp_session(db, parent_path)

        subagent_records_ingested = db.execute(
            "SELECT count(*) FROM records WHERE session_id = ?", [agent_id]
        ).fetchone()
        assert subagent_records_ingested is not None
        subagent_records_ingested_count = subagent_records_ingested[0]

        db.close()

    print(
        json.dumps(
            {
                "subagent_file_discovered": subagent_file_discovered,
                "subagent_records_ingested": subagent_records_ingested_count,
            }
        )
    )


if __name__ == "__main__":
    main()
