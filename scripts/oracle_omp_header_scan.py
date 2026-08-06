#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance test c1.

Scans the real omp corpus (<real-home>/.omp/agent/sessions, resolved from
the invoking user's passwd entry -- never $HOME, which the grader pins to
an empty scratch dir) with `omp.find_header`'s bounded leading-line scan,
and observes: how many files it finds a header in, how many of those
headers sit at an index other than 0, and how deep the scan ever had to
look. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import os
import pwd
from pathlib import Path

from ingest_sessions.omp import HEADER_SCAN_BOUND, find_header


def _real_omp_root() -> Path:
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return real_home / ".omp" / "agent" / "sessions"


def main() -> None:
    root = _real_omp_root()
    paths = sorted(root.rglob("*.jsonl")) if root.is_dir() else []

    files_with_header_found = 0
    headers_at_index_gt_zero = 0
    max_header_index = 0

    for path in paths:
        with open(path, "rb") as f:
            leading = [
                f.readline().decode("utf-8", errors="replace")
                for _ in range(HEADER_SCAN_BOUND)
            ]
        header, index = find_header(leading)
        if header is None or index is None:
            continue
        files_with_header_found += 1
        if index > 0:
            headers_at_index_gt_zero += 1
        max_header_index = max(max_header_index, index)

    print(
        json.dumps(
            {
                "files_total": len(paths),
                "files_with_header_found": files_with_header_found,
                "headers_at_index_gt_zero": headers_at_index_gt_zero,
                "max_header_index": max_header_index,
            }
        )
    )


if __name__ == "__main__":
    main()
