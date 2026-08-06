#!/usr/bin/env python3
"""Oracle probe (is-5a7.2 c3): env var sets the whole root list, os.pathsep-joined.

Emits {"count": N}. Judges nothing itself — sets two real folders (one
session file each), joins them with os.pathsep, and drives the real
resolve_discovery_roots() + discover_session_files().
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from ingest_sessions.core import discover_session_files, resolve_discovery_roots


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root_a = Path(tmp) / "a"
        root_b = Path(tmp) / "b"
        root_a.mkdir()
        root_b.mkdir()
        (root_a / "one.jsonl").write_text("{}\n")
        (root_b / "two.jsonl").write_text("{}\n")

        env_value = os.pathsep.join([str(root_a), str(root_b)])
        roots = resolve_discovery_roots(env_value)
        found = discover_session_files(roots)
        print(json.dumps({"count": len(found)}))


if __name__ == "__main__":
    main()
