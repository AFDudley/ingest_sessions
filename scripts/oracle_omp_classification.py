#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance test c8.

Asks `core.probe_session_format` what kind of file each of the real omp
transcripts is (<real-home>/.omp/agent/sessions, resolved from the
invoking user's passwd entry -- never $HOME) and tallies how many come
back "omp" versus "claude". Judges nothing -- prints one stdout_json
object.
"""

from __future__ import annotations

import json
import os
import pwd
from pathlib import Path

from ingest_sessions.core import probe_session_format


def _real_omp_root() -> Path:
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return real_home / ".omp" / "agent" / "sessions"


def main() -> None:
    root = _real_omp_root()
    paths = sorted(root.rglob("*.jsonl")) if root.is_dir() else []

    omp_classified = 0
    claude_classified = 0
    for path in paths:
        if probe_session_format(path) == "omp":
            omp_classified += 1
        else:
            claude_classified += 1

    print(
        json.dumps(
            {
                "files_total": len(paths),
                "omp_classified": omp_classified,
                "claude_classified": claude_classified,
            }
        )
    )


if __name__ == "__main__":
    main()
