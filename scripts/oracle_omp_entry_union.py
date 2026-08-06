#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c3.

Feeds the omp adapter a session file with one line for every entry kind
omp defines plus one deliberately broken line, and observes: ingestion
finishes normally, the broken line is counted as skipped, and all 13
well-formed kinds are recognized. Judges nothing -- prints one
stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import OMP_ENTRY_KINDS, ingest_omp_jsonl


def _entry_for_kind(kind: str, entry_id: str, ts: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": entry_id,
        "parentId": None,
        "type": kind,
        "timestamp": ts,
    }
    if kind == "message":
        base["message"] = {"role": "user", "content": "hi"}
    elif kind == "compaction":
        base["firstKeptEntryId"] = entry_id
    elif kind in ("branch_summary",):
        base["summary"] = "a branch summary"
    elif kind == "custom_message":
        base["content"] = "a custom message"
    return base


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        header = {
            "type": "session",
            "version": 3,
            "id": "omp-union-sess",
            "cwd": "/repo",
            "title": "entry union test",
        }
        kinds = sorted(OMP_ENTRY_KINDS)
        entries = [
            _entry_for_kind(kind, f"e-{i}", f"2026-01-01T00:{i:02d}:00.000Z")
            for i, kind in enumerate(kinds)
        ]
        lines = [json.dumps(header)] + [json.dumps(e) for e in entries]
        lines.append("{this is not valid json")  # deliberately broken line

        omp_path = tmp_path / "omp-union-sess.jsonl"
        omp_path.write_text("\n".join(lines) + "\n")

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        ingestion_aborted = False
        try:
            _record_count, malformed_count, _header_out = ingest_omp_jsonl(db, omp_path)
        except Exception:
            ingestion_aborted = True
            malformed_count = 0

        recognized_kinds = {
            row[0]
            for row in db.execute(
                "SELECT DISTINCT json_extract_string(raw, '$.type') FROM records"
            ).fetchall()
        }
        recognized_entry_kind_count = len(recognized_kinds & OMP_ENTRY_KINDS)

        db.close()

    print(
        json.dumps(
            {
                "ingestion_aborted": ingestion_aborted,
                "malformed_lines_skipped": malformed_count,
                "recognized_entry_kind_count": recognized_entry_kind_count,
            }
        )
    )


if __name__ == "__main__":
    main()
