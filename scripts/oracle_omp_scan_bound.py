#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance test c2.

Builds a synthetic Claude Code transcript with a `session`-shaped line
buried at index 5 -- well past the header scan's bound -- and observes
whether `core.probe_session_format` still classifies it as Claude. Also
runs the same probe over every real Claude Code transcript in the corpus
(<real-home>/.claude/projects, resolved from the invoking user's passwd
entry -- never $HOME) and counts any that get misclassified omp. Judges
nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import os
import pwd
import tempfile
from pathlib import Path

from ingest_sessions.core import probe_session_format
from ingest_sessions.omp import HEADER_SCAN_BOUND

DEEP_SESSION_LINE_INDEX = 5


def _real_claude_root() -> Path:
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return real_home / ".claude" / "projects"


def _build_deep_session_transcript(tmp_path: Path) -> Path:
    """A Claude-shaped transcript with a `session` line buried past the bound."""
    lines: list[dict[str, object]] = []
    for i in range(DEEP_SESSION_LINE_INDEX):
        lines.append(
            {
                "uuid": f"claude-deep-{i}",
                "sessionId": "claude-deep-sess",
                "type": "user",
                "timestamp": f"2026-01-01T00:0{i}:00.000Z",
                "parentUuid": None,
                "message": {"role": "user", "content": f"turn {i}"},
            }
        )
    # A `session`-shaped, omp-header-looking line, buried well past the bound.
    lines.append(
        {
            "type": "session",
            "version": 3,
            "id": "buried-session-id",
            "cwd": "/repo",
        }
    )
    path = tmp_path / "deep-session-line.jsonl"
    path.write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    return path


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        deep_path = _build_deep_session_transcript(Path(tmp))
        synthetic_classification = probe_session_format(deep_path)

    root = _real_claude_root()
    real_claude_paths = sorted(root.rglob("*.jsonl")) if root.is_dir() else []
    real_claude_files_classified_omp = sum(
        1 for p in real_claude_paths if probe_session_format(p) == "omp"
    )

    print(
        json.dumps(
            {
                "scan_bound": HEADER_SCAN_BOUND,
                "deep_session_line_index": DEEP_SESSION_LINE_INDEX,
                "synthetic_deep_session_classified_omp": synthetic_classification
                == "omp",
                "real_claude_files_total": len(real_claude_paths),
                "real_claude_files_classified_omp": real_claude_files_classified_omp,
            }
        )
    )


if __name__ == "__main__":
    main()
