#!/usr/bin/env python3
"""LCM context recovery hook for Claude Code SessionStart.

Called by Claude Code after /clear (and on session start).
Reads hook input from stdin (JSON with session_id, transcript_path,
cwd, source). When source is "clear", queries the ingest_sessions
server for the previous session's summary DAG context and returns
it as additionalContext.

The hook input schema (from Claude Code bundle):
    {
        "session_id": "new-session-uuid",
        "transcript_path": "/path/to/new-session.jsonl",
        "cwd": "/current/working/directory",
        "hook_event_name": "SessionStart",
        "source": "clear" | "startup" | "resume"
    }

After /clear, Claude creates a new session ID. The old session's
transcript is in the same directory. We query DuckDB for the most
recent session in that project directory that has summaries.

Prerequisites:
  - ingest-sessions-server running (REST endpoint at /api/context)

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> None:
    # Read hook input from stdin
    raw = sys.stdin.read().strip()
    if not raw:
        return

    try:
        hook_input = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Failed to parse hook input: {e}", file=sys.stderr)
        return

    source = hook_input.get("source", "")
    if source != "clear":
        # Only inject context after /clear, not on fresh startup
        return

    # After /clear, the session_id is the NEW (empty) session.
    # We need the previous session's context. The transcript_path
    # tells us which project directory to search.
    transcript_path = hook_input.get("transcript_path", "")
    if not transcript_path:
        print("No transcript_path in hook input", file=sys.stderr)
        return

    # The project dir is the parent of the transcript file
    # e.g., ~/.claude/projects/-home-user-myproject/abc123.jsonl
    # We pass the project directory so the server can find the
    # most recent session with summaries in that project.
    project_dir = os.path.dirname(transcript_path)

    port = os.environ.get("INGEST_SESSIONS_PORT", "8741")
    url = f"http://127.0.0.1:{port}/api/context"

    payload = json.dumps({"project_dir": project_dir}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
    except urllib.error.URLError as e:
        print(
            f"ingest-sessions server not reachable at {url}: {e}",
            file=sys.stderr,
        )
        return

    ctx = body.get("context")
    if not ctx:
        return

    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": ctx,
                }
            }
        )
    )


if __name__ == "__main__":
    main()
