#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c7.

Feeds the adapter three omp session files whose headers declare version
1, 2, and 3, and observes: all three ingest successfully, each
interpreted with the rule specific to its own declared version -- v1's
legacy `parent` link field, v2's renamed `parentId` field (without
compaction-truncation), and v3's `parentId` field WITH
compaction-truncation. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import (
    ingest_omp_jsonl,
    parse_entry,
    parse_header,
    reconstruct_conversation_order,
)


def _write_v1(tmp_path: Path) -> Path:
    header = {"type": "session", "version": 1, "id": "omp-v1-sess", "cwd": "/repo"}
    root = {
        "id": "v1-root",
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "root"},
    }
    child = {
        "id": "v1-child",
        # v1 legacy field name, NOT parentId
        "parent": "v1-root",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "child"},
    }
    path = tmp_path / "omp-v1-sess.jsonl"
    path.write_text("\n".join(json.dumps(x) for x in [header, root, child]) + "\n")
    return path


def _write_v2(tmp_path: Path) -> Path:
    header = {"type": "session", "version": 2, "id": "omp-v2-sess", "cwd": "/repo"}
    root = {
        "id": "v2-root",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "root"},
    }
    kept = {
        "id": "v2-kept",
        "parentId": "v2-root",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "kept"},
    }
    compaction = {
        "id": "v2-compaction",
        "parentId": "v2-kept",
        "type": "compaction",
        "timestamp": "2026-01-01T00:02:00.000Z",
        "firstKeptEntryId": "v2-kept",
    }
    leaf = {
        "id": "v2-leaf",
        "parentId": "v2-compaction",
        "type": "message",
        "timestamp": "2026-01-01T00:03:00.000Z",
        "message": {"role": "user", "content": "leaf"},
    }
    path = tmp_path / "omp-v2-sess.jsonl"
    path.write_text(
        "\n".join(json.dumps(x) for x in [header, root, kept, compaction, leaf]) + "\n"
    )
    return path


def _write_v3(tmp_path: Path) -> Path:
    header = {"type": "session", "version": 3, "id": "omp-v3-sess", "cwd": "/repo"}
    root = {
        "id": "v3-root",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "root"},
    }
    kept = {
        "id": "v3-kept",
        "parentId": "v3-root",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "kept"},
    }
    compaction = {
        "id": "v3-compaction",
        "parentId": "v3-kept",
        "type": "compaction",
        "timestamp": "2026-01-01T00:02:00.000Z",
        "firstKeptEntryId": "v3-kept",
    }
    leaf = {
        "id": "v3-leaf",
        "parentId": "v3-compaction",
        "type": "message",
        "timestamp": "2026-01-01T00:03:00.000Z",
        "message": {"role": "user", "content": "leaf"},
    }
    path = tmp_path / "omp-v3-sess.jsonl"
    path.write_text(
        "\n".join(json.dumps(x) for x in [header, root, kept, compaction, leaf]) + "\n"
    )
    return path


def _reconstruct_from_file(path: Path, leaf_id: str) -> list[dict]:
    lines = path.read_text().splitlines()
    header = parse_header(lines[0])
    assert header is not None
    version = header.get("version", 3)
    entries = [parse_entry(json.loads(line), version) for line in lines[1:]]
    return reconstruct_conversation_order(
        [e for e in entries if e is not None], leaf_id=leaf_id
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        v1_path = _write_v1(tmp_path)
        v2_path = _write_v2(tmp_path)
        v3_path = _write_v3(tmp_path)

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        _v1_count, v1_malformed, v1_header = ingest_omp_jsonl(db, v1_path)
        _v2_count, v2_malformed, v2_header = ingest_omp_jsonl(db, v2_path)
        _v3_count, v3_malformed, v3_header = ingest_omp_jsonl(db, v3_path)

        v1_child_parent = db.execute(
            "SELECT parent_uuid FROM records WHERE uuid = 'v1-child'"
        ).fetchone()
        v1_parsed_with_v1_rules = (
            v1_header is not None
            and v1_malformed == 0
            and v1_child_parent is not None
            and v1_child_parent[0] == "v1-root"
        )

        v2_order = _reconstruct_from_file(v2_path, "v2-leaf")
        v2_ids = [e["id"] for e in v2_order]
        v2_parsed_with_v2_rules = (
            v2_header is not None
            and v2_malformed == 0
            and "v2-root" in v2_ids  # v2 does NOT truncate at compaction
        )

        v3_order = _reconstruct_from_file(v3_path, "v3-leaf")
        v3_ids = [e["id"] for e in v3_order]
        v3_parsed_with_v3_rules = (
            v3_header is not None
            and v3_malformed == 0
            and "v3-root" not in v3_ids  # v3 DOES truncate at compaction
            and "v3-kept" in v3_ids
        )

        db.close()

    print(
        json.dumps(
            {
                "v1_parsed_with_v1_rules": v1_parsed_with_v1_rules,
                "v2_parsed_with_v2_rules": v2_parsed_with_v2_rules,
                "v3_parsed_with_v3_rules": v3_parsed_with_v3_rules,
            }
        )
    )


if __name__ == "__main__":
    main()
