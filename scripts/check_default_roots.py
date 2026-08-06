#!/usr/bin/env python3
"""Oracle probe (is-5a7.2 c2): default root list covers Claude + omp sources.

Emits {"count", "has_claude", "has_omp"}. Judges nothing itself — drives the
real resolve_discovery_roots() with no env override.
"""

from __future__ import annotations

import json
import os

from ingest_sessions.core import resolve_discovery_roots


def main() -> None:
    os.environ.pop("INGEST_SESSIONS_PROJECTS_DIR", None)
    roots = resolve_discovery_roots(os.environ.get("INGEST_SESSIONS_PROJECTS_DIR"))

    home = os.path.expanduser("~")
    claude_root = os.path.join(home, ".claude", "projects")
    omp_root = os.path.join(home, ".omp", "agent", "sessions")
    has_claude = any(str(r) == claude_root for r in roots)
    has_omp = any(str(r) == omp_root for r in roots)

    print(
        json.dumps(
            {
                "count": len(roots),
                "has_claude": has_claude,
                "has_omp": has_omp,
            }
        )
    )


if __name__ == "__main__":
    main()
