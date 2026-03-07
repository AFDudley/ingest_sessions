#!/usr/bin/env python3
"""LCM summarization trigger for Claude Code PreCompact.

Called by Claude Code before context compaction. Reads the session_id
from stdin hook input, triggers summarization via the REST API so that
summaries exist before /clear is used.

The hook input schema (from Claude Code bundle):
    {
        "session_id": "current-session-uuid",
        "transcript_path": "/path/to/session.jsonl",
        "cwd": "/current/working/directory",
        "hook_event_name": "PreCompact",
        "trigger": "auto" | "manual",
        "custom_instructions": "..."
    }

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> None:
    raw = sys.stdin.read().strip()
    if not raw:
        return

    try:
        hook_input = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Failed to parse hook input: {e}", file=sys.stderr)
        return

    session_id = hook_input.get("session_id", "")
    if not session_id:
        return

    port = os.environ.get("INGEST_SESSIONS_PORT", "8741")

    # First, refresh the session so new messages are ingested
    transcript_path = hook_input.get("transcript_path", "")
    if transcript_path:
        _api_post(port, "/api/refresh", {"path": transcript_path})

    # Then trigger summarization
    _api_post(port, "/api/summarize", {"session_id": session_id})


def _api_post(port: str, path: str, payload: dict) -> dict | None:
    """POST JSON to the ingest-sessions server. Returns response or None."""
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        print(f"ingest-sessions server error at {url}: {e}", file=sys.stderr)
        return None


if __name__ == "__main__":
    main()
