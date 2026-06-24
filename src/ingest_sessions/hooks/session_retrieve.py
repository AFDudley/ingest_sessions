#!/usr/bin/env python3
"""SessionStart hook: inject RELEVANT PRIOR REASONING via retrieve_relevant.

This is the injection half of the ingest-sessions retrieval brain (pebble
is-565): storage was solved, injection wasn't. A fresh agent is
anterograde-amnesiac; this hook surfaces relevant reasoning from PRIOR
sessions so it recalls earlier work on the same topic by construction.

Distinct from ``session_start.py`` (which recovers the CURRENT session's LCM
summary DAG after /clear). This hook queries the on-device two-stage pipeline
(``POST /api/retrieve`` → vector + BM25 fused, cross-encoder rerank, then
recency / confidence / supersession / trust-tier ranking) over ALL sessions
and injects the top hits as ``additionalContext``.

Seed query: there is no user prompt at SessionStart, so the query is the
repo's *current focus* — the git branch + recent commit subjects in ``cwd``.
That retrieves prior sessions that reasoned about what is being worked on now.

Latency: SessionStart is a once-per-session synchronous hook (~2s warm — the
server warms the embed + rerank models at startup; see server.py). This hook
FAILS OPEN: any error, timeout, unreachable server, or empty result yields no
injection and never blocks the session.

is-408: the harness drops this hook's ``additionalContext`` on a fresh
``claude`` start (``source=startup``) even though the hook runs and emits valid
JSON. The ``first_prompt_retrieve.py`` UserPromptSubmit hook is the reliable
fallback that guarantees injection once per session. The two hooks share a
per-session marker (``_retrieve_common``) to de-duplicate; see
``injection_is_known_good`` for the contract. The pure retrieval pipeline and
the marker helpers live in ``_retrieve_common`` so there is ONE implementation.

Invocation logging: every run logs a one-line summary to STDERR (never stdout —
stdout carries the JSON injection). This makes it observable, from the journal
or ``claude --debug``, whether this hook RAN on ``source=startup`` (which
distinguishes "not invoked" from "invoked but output dropped by the harness").

Hook input (stdin JSON, from Claude Code):
    {"session_id", "transcript_path", "cwd", "hook_event_name", "source"}

# See docs/plans/2026-06-22-retrieval-brain-is-565.md
"""

from __future__ import annotations

import json
import sys

from ingest_sessions.hooks._retrieve_common import (
    _retrieve,
    build_query,
    format_injection,
    write_marker,
)

__all__ = ["build_query", "format_injection", "main"]


def _log(source: str, query_built: bool, injected_chars: int) -> None:
    """One-line stderr trace of this invocation (never touches stdout)."""
    print(
        f"[ingest-sessions] session_retrieve: source={source or '?'} "
        f"query={'yes' if query_built else 'no'} injected={injected_chars}chars",
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

    source = str(hook_input.get("source", ""))
    session_id = str(hook_input.get("session_id", ""))
    cwd = str(hook_input.get("cwd", ""))

    query = build_query(cwd)
    results = _retrieve(query) if query else []
    context = format_injection(results) if results else None

    _log(source, query is not None, len(context) if context else 0)

    if not context:
        return

    # Best-effort de-dup marker recording WHICH source injected, so the
    # UserPromptSubmit fallback knows whether this injection can be trusted to
    # have been delivered (see injection_is_known_good). startup markers are
    # recorded but do NOT suppress the fallback.
    write_marker(session_id, event="SessionStart", source=source)

    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "SessionStart",
                    "additionalContext": context,
                }
            }
        )
    )


if __name__ == "__main__":
    main()
