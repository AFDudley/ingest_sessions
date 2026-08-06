#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance test c3.

For every real omp transcript (<real-home>/.omp/agent/sessions, resolved
from the invoking user's passwd entry -- never $HOME), runs BOTH
`core.probe_session_format` (the format-deciding step) and
`omp.ingest_omp_jsonl` (the omp reader itself, into a disposable throwaway
DuckDB) and observes whether their two answers to "is this an omp file?"
ever disagree. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import os
import pwd
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables, probe_session_format
from ingest_sessions.omp import ingest_omp_jsonl


def _real_omp_root() -> Path:
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return real_home / ".omp" / "agent" / "sessions"


def main() -> None:
    root = _real_omp_root()
    paths = sorted(root.rglob("*.jsonl")) if root.is_dir() else []

    disagreements = 0
    # In-memory throwaway db: this walks ingest_omp_jsonl's full parse+insert
    # over the whole real corpus, so it stays off disk entirely -- never the
    # live database, and no tempfile I/O to slow the full-corpus pass down.
    db = duckdb.connect(":memory:")
    create_tables(db)
    for path in paths:
        probe_says_omp = probe_session_format(path) == "omp"
        _, _, header = ingest_omp_jsonl(db, path)
        reader_says_omp = header is not None
        if probe_says_omp != reader_says_omp:
            disagreements += 1
    db.close()

    print(
        json.dumps(
            {
                "files_total": len(paths),
                "disagreements": disagreements,
            }
        )
    )


if __name__ == "__main__":
    main()
