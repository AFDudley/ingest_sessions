#!/usr/bin/env python3
"""Oracle probe (is-5a7.2 c5): the watchdog schedules recursive watches on the
same root list the cold scan walks.

Emits {"scheduled_count", "all_recursive"}. Judges nothing itself — drives a
real watchdog Observer through server._schedule_watches() over two real
folders (no mocking of internal functions).
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from watchdog.observers import Observer

from ingest_sessions import server


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        roots = [Path(tmp) / "a", Path(tmp) / "b"]
        observer = Observer()
        watches = server._schedule_watches(observer, roots)
        print(
            json.dumps(
                {
                    "scheduled_count": len(watches),
                    "all_recursive": all(w.is_recursive for w in watches),
                }
            )
        )


if __name__ == "__main__":
    main()
