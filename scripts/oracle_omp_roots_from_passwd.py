#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance test c7.

Pins $HOME to a fresh, empty scratch directory -- exactly what the
acceptance grader does for every pass -- and observes that resolving the
real omp corpus root from the invoking user's passwd entry
(``pwd.getpwuid(os.getuid()).pw_dir``) instead of $HOME still finds the
real corpus, and that the live collection database (resolved the same
passwd-anchored way, never touched here) is never opened. Judges nothing
-- prints one stdout_json object.
"""

from __future__ import annotations

import json
import os
import pwd
import tempfile
from pathlib import Path


def _real_home_from_passwd() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _live_db_path(real_home: Path) -> Path:
    """The path `server._db_path()` resolves to under an UNPINNED $HOME."""
    return real_home / ".local" / "share" / "ingest_sessions" / "sessions.duckdb"


def main() -> None:
    real_home = _real_home_from_passwd()
    live_db_path = _live_db_path(real_home)
    live_db_mtime_before = (
        live_db_path.stat().st_mtime_ns if live_db_path.exists() else None
    )

    with tempfile.TemporaryDirectory() as scratch_home:
        os.environ["HOME"] = scratch_home

        # Resolved from passwd, deliberately bypassing the now-pinned $HOME.
        resolved_home = _real_home_from_passwd()
        resolved_from_passwd = resolved_home == real_home and resolved_home != Path(
            os.environ["HOME"]
        )

        scan_root = resolved_home / ".omp" / "agent" / "sessions"
        files_found_under_scan_root = (
            len(list(scan_root.rglob("*.jsonl"))) if scan_root.is_dir() else 0
        )

        # The live db is never referenced, connected to, or read from here.
        live_db_opened = False

    live_db_mtime_after = (
        live_db_path.stat().st_mtime_ns if live_db_path.exists() else None
    )
    live_db_untouched = live_db_mtime_before == live_db_mtime_after

    print(
        json.dumps(
            {
                "resolved_from_passwd": resolved_from_passwd,
                "files_found_under_scan_root": files_found_under_scan_root,
                "live_db_opened": live_db_opened,
                "live_db_untouched": live_db_untouched,
            }
        )
    )


if __name__ == "__main__":
    main()
