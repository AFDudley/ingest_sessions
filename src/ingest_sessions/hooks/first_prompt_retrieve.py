#!/usr/bin/env python3
"""UserPromptSubmit hook: guarantee relevant-prior-reasoning injection once/session.

This is the is-408 fallback for ``session_retrieve.py``. The SessionStart hook
emits valid ``additionalContext`` on a fresh ``claude`` start (``source=startup``)
but the harness DROPS it, so a fresh session never recalls prior reasoning. We
cannot fix the harness, so we make injection reliable from our side: on the
FIRST user prompt of a session, inject the same retrieval context — unless
SessionStart already delivered it on a trusted source.

De-dup (the load-bearing correctness decision — full contract in
``_retrieve_common.injection_is_known_good``):

    * Both hooks share a per-session marker file
      (``~/.cache/ingest-sessions/injected/<session_id>``, honoring
      ``XDG_CACHE_HOME``).
    * This hook injects iff there is NO known-good marker. A SessionStart marker
      is known-good only for a trusted source (resume/clear/compact) — NOT
      ``startup``, because the harness may have dropped that injection.
    * Net effect: exactly once per session in the normal case; ALWAYS at least
      once even when SessionStart-startup is silently dropped.

Cost: adds the ~2s warm retrieval round trip to ONLY the first prompt of a
session (and only when SessionStart did not already inject). Subsequent prompts
see the known-good UserPromptSubmit marker and return immediately with no
output.

FAILS OPEN: any error, timeout, unreachable server, or empty result yields no
output and exit 0 — never blocks the prompt. Logs a one-line trace to STDERR.

Hook input (stdin JSON, from Claude Code, UserPromptSubmit):
    {"session_id", "transcript_path", "cwd", "hook_event_name", "prompt"}

UserPromptSubmit additionalContext output shape (mirrors the documented
SessionStart shape; ``hookEventName`` set to ``UserPromptSubmit``):
    {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit",
                            "additionalContext": "<context>"}}

# See docs/plans/2026-06-22-retrieval-brain-is-565.md
"""

from __future__ import annotations

import json
import sys

from ingest_sessions.hooks._retrieve_common import (
    _retrieve,
    build_query,
    format_injection,
    injection_is_known_good,
    read_marker,
    write_marker,
)

__all__ = ["main"]


def _log(
    session_id: str, query_built: bool, injected_chars: int, skipped: bool
) -> None:
    """One-line stderr trace of this invocation (never touches stdout)."""
    state = "skipped" if skipped else f"injected={injected_chars}chars"
    print(
        f"[ingest-sessions] first_prompt_retrieve: session={session_id[:8] or '?'} "
        f"query={'yes' if query_built else 'no'} {state}",
        file=sys.stderr,
    )


def main() -> None:
    raw = sys.stdin.read().strip()
    if not raw:
        return
    try:
        hook_input = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Failed to parse hook input: {e}", file=sys.stderr)
        return

    session_id = str(hook_input.get("session_id", ""))
    cwd = str(hook_input.get("cwd", ""))

    # Already injected (known-good) this session → nothing to do.
    marker = read_marker(session_id)
    if marker is not None and injection_is_known_good(marker):
        _log(session_id, query_built=False, injected_chars=0, skipped=True)
        return

    query = build_query(cwd)
    results = _retrieve(query) if query else []
    context = format_injection(results) if results else None

    if not context:
        _log(session_id, query is not None, 0, skipped=False)
        return

    # Record THIS injection as known-good so later prompts in the session skip.
    write_marker(session_id, event="UserPromptSubmit")

    _log(session_id, query_built=True, injected_chars=len(context), skipped=False)

    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": context,
                }
            }
        )
    )


if __name__ == "__main__":
    main()
