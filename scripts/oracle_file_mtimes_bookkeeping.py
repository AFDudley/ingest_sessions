#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.3 acceptance test c5.

Two independent checks:

1. A small synthetic omp folder is cold-scanned (via the real
   ``ingest_routed_file`` router + ``core.file_changed`` / ``record_file``)
   three times: twice back-to-back with nothing changed (the second pass
   must find nothing to re-read), then once more after appending a new
   entry to the file (that file must now be picked up as changed). This
   proves omp files get the SAME file_mtimes treatment claude files
   already get.

2. The two REAL default discovery roots (~/.claude/projects,
   ~/.omp/agent/sessions) are cold-scanned into a disposable, throwaway
   DuckDB -- never the live one -- and every discovered path is run
   through ``file_changed`` + ``record_file``, the exact bookkeeping
   functions this clause is about. Content parsing (``ingest_routed_file``)
   is skipped for this real-corpus half purely to keep runtime bounded over
   thousands of real files; c1-c4 already prove routing correctness on
   synthetic fixtures, so this half only needs to prove file_mtimes
   COVERAGE isn't lost for either corpus. Judges nothing -- prints one
   stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import (
    create_tables,
    default_discovery_roots,
    discover_session_files,
    file_changed,
    ingest_routed_file,
    record_file,
)


def _synthetic_cold_scan(db: duckdb.DuckDBPyConnection, root: Path) -> int:
    """One cold-scan pass over *root*. Returns how many files were re-read."""
    changed_count = 0
    for path in discover_session_files([root]):
        changed, prev_size = file_changed(db, path)
        if not changed:
            continue
        changed_count += 1
        _count, bytes_read, _fmt = ingest_routed_file(db, path, byte_offset=prev_size)
        record_file(db, path, size_bytes=bytes_read)
    return changed_count


def _run_synthetic_checks(tmp_path: Path) -> tuple[int, int]:
    synth_dir = tmp_path / "synthetic_omp"
    synth_dir.mkdir()
    header = {"type": "session", "version": 3, "id": "mtimes-omp-sess", "cwd": "/repo"}
    entry = {
        "id": "e1",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "hello"},
    }
    omp_path = synth_dir / "mtimes-omp-sess.jsonl"
    omp_path.write_text("\n".join(json.dumps(x) for x in [header, entry]) + "\n")

    db = duckdb.connect(str(tmp_path / "synthetic.duckdb"))
    create_tables(db)

    first_pass = _synthetic_cold_scan(db, synth_dir)
    assert first_pass >= 1  # sanity: the new file must be seen once

    rescan_unchanged_count = _synthetic_cold_scan(db, synth_dir)

    entry2 = {
        "id": "e2",
        "parentId": "e1",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "hi back"},
    }
    with omp_path.open("a") as f:
        f.write(json.dumps(entry2) + "\n")

    after_append_changed_count = _synthetic_cold_scan(db, synth_dir)

    db.close()
    return rescan_unchanged_count, after_append_changed_count


def _run_real_corpus_check(tmp_path: Path) -> tuple[int, int]:
    roots = default_discovery_roots()
    claude_root, omp_root = roots[0], roots[1]

    db = duckdb.connect(str(tmp_path / "real_corpus.duckdb"))
    create_tables(db)

    for root in (claude_root, omp_root):
        for path in discover_session_files([root]):
            changed, _prev_size = file_changed(db, path)
            if changed:
                record_file(db, path)

    claude_paths_recorded = db.execute(
        "SELECT count(*) FROM file_mtimes WHERE file_path LIKE ?",
        [f"{claude_root}%"],
    ).fetchone()
    omp_paths_recorded = db.execute(
        "SELECT count(*) FROM file_mtimes WHERE file_path LIKE ?",
        [f"{omp_root}%"],
    ).fetchone()
    db.close()

    assert claude_paths_recorded is not None and omp_paths_recorded is not None
    return omp_paths_recorded[0], claude_paths_recorded[0]


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        rescan_unchanged_count, after_append_changed_count = _run_synthetic_checks(
            tmp_path
        )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        omp_paths_recorded, claude_paths_recorded = _run_real_corpus_check(tmp_path)

    print(
        json.dumps(
            {
                "rescan_unchanged_count": rescan_unchanged_count,
                "after_append_changed_count": after_append_changed_count,
                "omp_paths_recorded": omp_paths_recorded,
                "claude_paths_recorded": claude_paths_recorded,
            }
        )
    )


if __name__ == "__main__":
    main()
