#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c1.

Ingests one small Claude-Code-style session file and one small omp-style
session file into the same DuckDB database, then observes the schema: are
there still exactly one `records` table and one `sessions` table, and do
both sessions' records land in that same shared `records` table. Judges
nothing itself -- prints one stdout_json object of observations for the
runner's `then` predicates to evaluate.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables, ingest_jsonl
from ingest_sessions.omp import ingest_omp_session


def _scalar(db: duckdb.DuckDBPyConnection, sql: str) -> object:
    row = db.execute(sql).fetchone()
    assert row is not None
    return row[0]


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        claude_record = {
            "uuid": "claude-r1",
            "sessionId": "claude-sess-1",
            "type": "user",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "parentUuid": None,
            "message": {"role": "user", "content": "hello from claude code"},
        }
        claude_path = tmp_path / "claude-sess-1.jsonl"
        claude_path.write_text(json.dumps(claude_record) + "\n")

        omp_header = {
            "type": "session",
            "version": 3,
            "id": "omp-sess-1",
            "cwd": "/repo",
            "title": "omp session",
        }
        omp_entry = {
            "id": "omp-e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hello from omp"},
        }
        omp_path = tmp_path / "omp-sess-1.jsonl"
        omp_path.write_text(
            "\n".join(json.dumps(x) for x in [omp_header, omp_entry]) + "\n"
        )

        db_path = tmp_path / "shared.duckdb"
        db = duckdb.connect(str(db_path))
        create_tables(db)

        ingest_jsonl(db, claude_path)
        ingest_omp_session(db, omp_path)

        records_table_count = _scalar(
            db,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'records'",
        )
        sessions_table_count = _scalar(
            db,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = 'sessions'",
        )

        session_ids_in_records = {
            row[0]
            for row in db.execute("SELECT DISTINCT session_id FROM records").fetchall()
        }
        claude_and_omp_records_share_table = (
            "claude-sess-1" in session_ids_in_records
            and "omp-sess-1" in session_ids_in_records
        )

        db.close()

    print(
        json.dumps(
            {
                "records_table_count": records_table_count,
                "sessions_table_count": sessions_table_count,
                "claude_and_omp_records_share_table": claude_and_omp_records_share_table,
            }
        )
    )


if __name__ == "__main__":
    main()
