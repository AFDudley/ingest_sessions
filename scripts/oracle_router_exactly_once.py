#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.3 acceptance test c3.

Builds a synthetic omp session with a parent transcript file and one
subagent transcript file (3 entries) beside it, then hands BOTH paths to
the shared router separately -- the same way recursive discovery (is-5a7.2)
hands a subagent transcript over as its own path, never bundled with its
parent. Observes how many rows the subagent file's own entries produced,
so a router that also re-triggers ``omp.ingest_omp_session``'s own
``discover_subagent_files`` fan-out would show double (6, not 3). Judges
nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables, ingest_routed_file

PARENT_ID = "router-once-parent-sess"
SUBAGENT_ID = "router-once-subagent-id"


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
        "id": SUBAGENT_ID,
        "cwd": "/repo",
        "parentSession": PARENT_ID,
    }
    subagent_entries = [
        {
            "id": f"sub-e{i}",
            "parentId": f"sub-e{i - 1}" if i else None,
            "type": "message",
            "timestamp": f"2026-01-01T00:0{i}:00.000Z",
            "message": {"role": "assistant", "content": f"subagent turn {i}"},
        }
        for i in range(3)
    ]
    # A subagent transcript is named after its agent LABEL, not an id.
    subagent_path = subagent_dir / "MeasureFixtureFraction.jsonl"
    subagent_path.write_text(
        "\n".join(json.dumps(x) for x in [subagent_header, *subagent_entries]) + "\n"
    )
    return parent_path, subagent_path


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        parent_path, subagent_path = _build_files(tmp_path)

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        # Discovery already found both files as separate paths; hand each
        # one to the router on its own, exactly as the cold scan / watchdog
        # would.
        ingest_routed_file(db, parent_path)
        ingest_routed_file(db, subagent_path)

        row = db.execute(
            "SELECT count(*) FROM records WHERE session_id = ?", [SUBAGENT_ID]
        ).fetchone()
        assert row is not None
        subagent_row_count = row[0]

        db.close()

    print(json.dumps({"subagent_row_count": subagent_row_count}))


if __name__ == "__main__":
    main()
