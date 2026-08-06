#!/usr/bin/env python3
"""Metamorphic probe for derived-is-5a7.4 acceptance test c6 (conservation).

Reads the SAME omp transcript twice through `omp.ingest_omp_jsonl`: once
with its header already on line 1, once with a `title` preamble line
glued on top of it. Encodes the invariance claim as a signed-flow
conservation law (spec_oracle._relation_conservation): for every entry
produced by either reading, build the tuple (id, parentId, type, sha256
of the entry's normalized content) -- plus one ("__malformed__",) tuple
per malformed line -- and emit one flow per DISTINCT tuple t:
w(t) * (count_A(t) - count_B(t)), where w(t) is a 40-bit weight derived
from sha256(repr(t)). sum(flows) == 0 iff the two readings produced
identical multisets of entries; a surplus of one tuple in A cannot
silently cancel a deficit of a different tuple in B, since the per-tuple
weights differ. Judges nothing beyond that -- prints one stdout_json
object carrying `flows`.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import ingest_omp_jsonl

SESSION_ID = "preamble-invariance-sess"
_MALFORMED_TUPLE = ("__malformed__",)


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


def _content_hash(raw: str) -> str:
    """sha256 of *raw*'s normalized (sort_keys) JSON -- key order can't matter."""
    normalized = json.dumps(json.loads(raw), sort_keys=True)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _entry_tuples(
    rows: list[tuple[Any, ...]], malformed_count: int
) -> Counter[tuple[Any, ...]]:
    """One (id, parentId, type, content_hash) tuple per row, plus one
    `_MALFORMED_TUPLE` per malformed line -- the multiset a reading produced."""
    counter: Counter[tuple[Any, ...]] = Counter(
        (uuid, parent_uuid, type_, _content_hash(raw))
        for uuid, type_, parent_uuid, raw in rows
    )
    counter[_MALFORMED_TUPLE] += malformed_count
    return counter


def _weight(t: tuple[Any, ...]) -> int:
    """A 40-bit weight derived from *t*'s repr -- distinct tuples get distinct
    weights, so a surplus of one tuple can't cancel a deficit of another."""
    return int(hashlib.sha256(repr(t).encode("utf-8")).hexdigest()[:10], 16)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        rows_no_preamble, malformed_no_preamble = _read(
            tmp_path, "no_preamble", preamble=False
        )
        rows_with_preamble, malformed_with_preamble = _read(
            tmp_path, "with_preamble", preamble=True
        )

    count_a = _entry_tuples(rows_no_preamble, malformed_no_preamble)
    count_b = _entry_tuples(rows_with_preamble, malformed_with_preamble)

    distinct = sorted(set(count_a) | set(count_b), key=repr)
    flows = [_weight(t) * (count_a[t] - count_b[t]) for t in distinct]

    print(
        json.dumps(
            {
                "flows": flows,
                "entry_count": len(rows_no_preamble),
                "entries_identical": count_a == count_b,
                "malformed_count_identical": malformed_no_preamble
                == malformed_with_preamble,
            }
        )
    )


if __name__ == "__main__":
    main()
