#!/usr/bin/env python3
"""Oracle probe (is-5a7.2 c4): a missing configured root is skipped, not fatal.

Emits {"count": N} and exits 0. Judges nothing itself — drives the real
discover_session_files() over one real root and one nonexistent root.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from ingest_sessions.core import discover_session_files


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        real_root = Path(tmp) / "real"
        real_root.mkdir()
        (real_root / "one.jsonl").write_text("{}\n")
        missing_root = Path(tmp) / "does-not-exist"

        found = discover_session_files([real_root, missing_root])
        print(json.dumps({"count": len(found)}))


if __name__ == "__main__":
    main()
