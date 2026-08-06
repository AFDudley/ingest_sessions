#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.3 acceptance test c4.

Builds the same shape of synthetic omp session as c3: a parent transcript
file with its own id, and a subagent transcript file named after a plain
agent LABEL ("MeasureFixtureFraction") rather than an id, whose own header
declares a THIRD, different id. Routes both files separately and observes
which id ended up as the subagent's stored session_id: its own header id,
the parent's id, or the literal filename stem. Judges nothing -- prints
one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables, ingest_routed_file

PARENT_ID = "provenance-parent-sess"
SUBAGENT_HEADER_ID = "provenance-subagent-header-id"
SUBAGENT_FILENAME_STEM = "MeasureFixtureFraction"


def _build_files(tmp_path: Path) -> tuple[Path, Path]:
    parent_header = {
        "type": "session",
        "version": 3,
        "id": PARENT_ID,
        "cwd": "/repo",
    }
    parent_entry = {
        "id": "parent-e1",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "delegate to a subagent"},
    }
    parent_path = tmp_path / f"{PARENT_ID}.jsonl"
    parent_path.write_text(
        "\n".join(json.dumps(x) for x in [parent_header, parent_entry]) + "\n"
    )

    subagent_dir = tmp_path / PARENT_ID
    subagent_dir.mkdir()
    subagent_header = {
        "type": "session",
        "version": 3,
        # Deliberately its OWN id -- distinct from both the parent's id and
        # the filename label below.
        "id": SUBAGENT_HEADER_ID,
        "cwd": "/repo",
        "parentSession": PARENT_ID,
    }
    subagent_entry = {
        "id": "sub-e0",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "subagent working"},
    }
    subagent_path = subagent_dir / f"{SUBAGENT_FILENAME_STEM}.jsonl"
    subagent_path.write_text(
        "\n".join(json.dumps(x) for x in [subagent_header, subagent_entry]) + "\n"
    )
    return parent_path, subagent_path


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        parent_path, subagent_path = _build_files(tmp_path)

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        ingest_routed_file(db, parent_path)
        ingest_routed_file(db, subagent_path)

        row = db.execute(
            "SELECT DISTINCT session_id FROM records WHERE uuid = 'sub-e0'"
        ).fetchone()
        assert row is not None
        stored_session_id = row[0]

        db.close()

    print(
        json.dumps(
            {
                "subagent_session_id_matches_own_header": (
                    stored_session_id == SUBAGENT_HEADER_ID
                ),
                "subagent_session_id_matches_filename_stem": (
                    stored_session_id == SUBAGENT_FILENAME_STEM
                ),
                "subagent_session_id_matches_parent_id": (
                    stored_session_id == PARENT_ID
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
