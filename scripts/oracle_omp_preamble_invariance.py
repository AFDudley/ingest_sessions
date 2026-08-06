#!/usr/bin/env python3
"""Metamorphic probe for derived-is-5a7.4 acceptance test c6 (conservation).

Reads the SAME omp transcript twice through `omp.ingest_omp_jsonl`: once
with its header already on line 1, once with a `title` preamble line
glued on top of it. Asserts the two readings produce identical entries --
same identities, parents, kinds, content, order and malformed count --
proving the preamble changes nothing about how the rest of the file is
understood. This script IS the judge (metamorphic/conservation type): it
asserts the invariant itself and exits non-zero if it doesn't hold.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import ingest_omp_jsonl

SESSION_ID = "preamble-invariance-sess"


def _entries() -> list[dict[str, Any]]:
    header = {
        "type": "session",
        "version": 3,
        "id": SESSION_ID,
        "cwd": "/repo",
        "title": "invariance check",
    }
    e1 = {
        "id": "pi-1",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "start the check"},
    }
    e2 = {
        "id": "pi-2",
        "parentId": "pi-1",
        "type": "model_change",
        "timestamp": "2026-01-01T00:00:30.000Z",
        "model": "claude-sonnet-5",
    }
    e3 = {
        "id": "pi-3",
        "parentId": "pi-2",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "checked"},
    }
    e4 = {
        "id": "pi-4",
        "parentId": "pi-3",
        "type": "custom_message",
        "timestamp": "2026-01-01T00:01:30.000Z",
        "content": "a synthetic note",
    }
    return [header, e1, e2, e3, e4]


def _read(tmp_path: Path, name: str, preamble: bool) -> tuple[list[tuple], int]:
    lines = _entries()
    if preamble:
        title_line = {
            "type": "title",
            "v": 1,
            "title": "invariance check",
            "source": "auto",
            "updatedAt": "2026-01-01T00:00:00.000Z",
            "pad": " " * 40,
        }
        lines = [title_line, *lines]

    path = tmp_path / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(x) for x in lines) + "\n")

    db = duckdb.connect(str(tmp_path / f"{name}.duckdb"))
    create_tables(db)
    _, malformed_count, header = ingest_omp_jsonl(db, path)
    assert header is not None, f"{name}: expected a header to be found"

    rows = db.execute(
        "SELECT uuid, type, parent_uuid, raw FROM records "
        "WHERE session_id = ? ORDER BY timestamp",
        [SESSION_ID],
    ).fetchall()
    db.close()
    return rows, malformed_count


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        rows_no_preamble, malformed_no_preamble = _read(
            tmp_path, "no_preamble", preamble=False
        )
        rows_with_preamble, malformed_with_preamble = _read(
            tmp_path, "with_preamble", preamble=True
        )

    assert rows_no_preamble == rows_with_preamble, (
        "preamble changed the read entries: "
        f"{rows_no_preamble!r} != {rows_with_preamble!r}"
    )
    assert malformed_no_preamble == malformed_with_preamble, (
        "preamble changed the malformed-line count: "
        f"{malformed_no_preamble} != {malformed_with_preamble}"
    )

    print(
        json.dumps(
            {
                "entry_count": len(rows_no_preamble),
                "entries_identical": rows_no_preamble == rows_with_preamble,
                "malformed_count_identical": malformed_no_preamble
                == malformed_with_preamble,
            }
        )
    )


if __name__ == "__main__":
    main()
