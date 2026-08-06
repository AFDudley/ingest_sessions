#!/usr/bin/env python3
"""Oracle probe (is-5a7.2, cli.py discrimination coverage): the batch CLI's
per-project walk finds *.jsonl nested more than two levels under a named
project dir, not just files directly inside it.

None of the five spec oracles for this pebble exercise cli.py (they all
drive core.py / server.py directly), so this script is the committed
oracle that covers the cli.py hardcoded walk named in the pebble's "Today"
section (cli.py:116, the second of the two two-level walks being
collapsed). It is RED on the pre-work tree (cli.ingest -> proj_dir.glob
("*.jsonl"), non-recursive) and GREEN on this branch (cli.ingest ->
discover_session_files([proj_dir]), recursive).

Emits {"sessions_found": N}. Judges nothing itself — drives the real
cli.ingest() over a synthetic ~/.claude/projects/<name>/... tree (HOME
redirected to a tmp dir for isolation) and reports the session count the
real print output recorded for that project.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
from pathlib import Path


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp) / "home"
        proj_dir = home / ".claude" / "projects" / "proj1"
        nested = proj_dir / "a" / "b" / "c"
        nested.mkdir(parents=True, exist_ok=True)
        (nested / "session.jsonl").write_text(
            json.dumps(
                {"uuid": "u1", "type": "user", "timestamp": "2026-01-01T00:00:00.000Z"}
            )
            + "\n"
        )

        # Redirect HOME so cli._claude_dir() (Path.home() / ".claude") resolves
        # into the synthetic tree, isolated from the real ~/.claude.
        os.environ["HOME"] = str(home)

        from ingest_sessions import cli

        output = Path(tmp) / "out.duckdb"
        config = cli.IngestConfig(
            output=output, projects=["proj1"], include_history=False
        )

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cli.ingest(config)

        sessions_found = 0
        for line in buf.getvalue().splitlines():
            line = line.strip()
            if line.startswith("proj1:"):
                sessions_found = int(line.split(":", 1)[1].strip().split()[0])

        print(json.dumps({"sessions_found": sessions_found}))


if __name__ == "__main__":
    main()
