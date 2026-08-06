#!/usr/bin/env python3
"""Oracle probe (is-5a7.2 c1): recursive discovery finds *.jsonl at any depth.

Emits {"count": N} for the reducer's spec to assert against. Judges nothing
itself — drives the real discover_session_files() over a synthetic tree with
files at depths 1, 2, 4, and 5.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from ingest_sessions.core import discover_session_files


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "sessions"
        files = [
            root / "a.jsonl",
            root / "p1" / "b.jsonl",
            root / "p1" / "p2" / "p3" / "c.jsonl",
            root / "p1" / "p2" / "p3" / "p4" / "d.jsonl",
        ]
        for f in files:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text("{}\n")

        found = discover_session_files([root])
        print(json.dumps({"count": len(found)}))


if __name__ == "__main__":
    main()
