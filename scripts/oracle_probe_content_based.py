#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.3 acceptance test c1.

Builds two adversarial samples that decouple format from path: an
omp-shaped first line written OUTSIDE any ``.omp``-looking directory, and
a Claude-Code-shaped first line written INSIDE a directory that mimics the
default omp folder name. Observes what ``core.probe_session_format``
classifies each as. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from ingest_sessions.core import probe_session_format


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # omp-shaped header, relocated well away from any ".omp" path.
        relocated_dir = tmp_path / "not_omp_at_all" / "elsewhere"
        relocated_dir.mkdir(parents=True)
        omp_header = {
            "type": "session",
            "version": 3,
            "id": "relocated-omp-sess",
            "cwd": "/repo",
        }
        relocated_omp_path = relocated_dir / "relocated.jsonl"
        relocated_omp_path.write_text(json.dumps(omp_header) + "\n")

        # Claude-Code-shaped first line, placed inside a directory that
        # looks exactly like the default omp folder.
        omp_looking_dir = tmp_path / ".omp" / "agent" / "sessions"
        omp_looking_dir.mkdir(parents=True)
        claude_entry = {
            "uuid": "claude-uuid-1",
            "sessionId": "claude-sess-1",
            "type": "user",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "parentUuid": None,
            "message": {"role": "user", "content": "hello"},
        }
        claude_in_omp_path = omp_looking_dir / "impostor.jsonl"
        claude_in_omp_path.write_text(json.dumps(claude_entry) + "\n")

        relocated_omp_sample_classification = probe_session_format(relocated_omp_path)
        claude_shaped_in_omp_path_classification = probe_session_format(
            claude_in_omp_path
        )

    print(
        json.dumps(
            {
                "relocated_omp_sample_classification": relocated_omp_sample_classification,
                "claude_shaped_in_omp_path_classification": (
                    claude_shaped_in_omp_path_classification
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
