#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c2.

Feeds the omp adapter a two-line session file (header + one ordinary chat
message) and observes: exactly one record lands in the database, the
header line itself never became a record, and the session's session_id
equals the header's id. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import ingest_omp_jsonl, ingest_omp_session_metadata


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        header = {
            "type": "session",
            "version": 3,
            "id": "omp-header-sess",
            "cwd": "/repo",
            "title": "header test",
        }
        entry = {
            "id": "omp-header-e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hello"},
        }
        omp_path = tmp_path / "omp-header-sess.jsonl"
        omp_path.write_text("\n".join(json.dumps(x) for x in [header, entry]) + "\n")

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        record_count, _malformed_count, parsed_header = ingest_omp_jsonl(db, omp_path)
        assert parsed_header is not None
        ingest_omp_session_metadata(db, parsed_header)

        record_ids = {
            row[0] for row in db.execute("SELECT uuid FROM records").fetchall()
        }
        record_types = {
            row[0] for row in db.execute("SELECT type FROM records").fetchall()
        }
        header_inserted_as_record = (
            header["id"] in record_ids or "session" in record_types
        )

        session_row = db.execute(
            "SELECT session_id FROM sessions WHERE session_id = ?",
            [header["id"]],
        ).fetchone()
        session_id_from_header_id = session_row is not None

        db.close()

    print(
        json.dumps(
            {
                "record_count": record_count,
                "header_inserted_as_record": header_inserted_as_record,
                "session_id_from_header_id": session_id_from_header_id,
            }
        )
    )


if __name__ == "__main__":
    main()
