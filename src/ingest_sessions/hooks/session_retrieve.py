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

Hook input (stdin JSON, from Claude Code):
    {"session_id", "transcript_path", "cwd", "hook_event_name", "source"}

# See docs/plans/2026-06-22-retrieval-brain-is-565.md
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

# Stage-1 candidates to rerank. Bounded for the sub-few-second hook budget
# (cross-encoder rerank is ~39 ms/candidate; 20 ≈ ~2s warm).
CANDIDATE_K = 20
TOP_K = 5
SNIPPET_CHARS = 240
HTTP_TIMEOUT_S = 12


def _git(cwd: str, args: list[str]) -> str | None:
    """Run a git command in *cwd*; return stripped stdout or None on failure."""
    try:
        out = subprocess.run(
            ["git", "-C", cwd, *args],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def build_query(cwd: str) -> str | None:
    """Build the retrieval seed from the repo's current focus (pure-ish).

    Combines the current git branch with the last few commit subjects in
    *cwd*. Returns None when *cwd* is empty or not a git repo (→ no injection).
    """
    if not cwd:
        return None
    parts: list[str] = []
    branch = _git(cwd, ["branch", "--show-current"])
    if branch:
        parts.append(branch)
    subjects = _git(cwd, ["log", "-5", "--format=%s"])
    if subjects:
        parts.append(subjects.replace("\n", " "))
    query = " ".join(parts).strip()
    return query or None


def _content_text(value: Any) -> str:
    """Flatten a record message ``content`` (str | blocks) to text (pure)."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        out: list[str] = []
        for block in value:
            if isinstance(block, dict):
                if isinstance(block.get("text"), str):
                    out.append(block["text"])
                elif isinstance(block.get("content"), str):
                    out.append(block["content"])
        return " ".join(out)
    return ""


def _snippet(result: dict[str, Any]) -> str:
    """A short, single-line text snippet for a retrieval hit (pure)."""
    # Summary-tier candidates carry `content`; record-tier carry `raw`.
    text = ""
    if isinstance(result.get("content"), str):
        text = result["content"]
    else:
        raw = result.get("raw")
        if isinstance(raw, dict):
            msg = raw.get("message")
            if isinstance(msg, dict):
                text = _content_text(msg.get("content"))
    text = " ".join(text.split())
    return text[:SNIPPET_CHARS]


def format_injection(results: list[dict[str, Any]]) -> str | None:
    """Render retrieval hits as injectable context (pure). None if nothing useful."""
    lines: list[str] = []
    for r in results:
        snip = _snippet(r)
        if not snip:
            continue
        sid = str(r.get("session_id") or "")
        tier = "summary" if r.get("summary_id") else "session"
        flag = " [superseded]" if r.get("superseded") else ""
        score = r.get("final_score")
        score_s = f" {score:.2f}" if isinstance(score, (int, float)) else ""
        lines.append(f"- ({tier} {sid[:8]}{score_s}{flag}) {snip}")
    if not lines:
        return None
    header = (
        "## Relevant prior reasoning (ingest-sessions)\n"
        "Retrieved from past sessions by relevance to this repo's current "
        "focus, ranked with recency / confidence / supersession / trust-tier. "
        "A `[superseded]` item was later overturned — prefer the superseding "
        "view. Use the ingest-sessions `get_session(<id>)` tool to read any "
        "cited session in full.\n"
    )
    return header + "\n".join(lines)


def _retrieve(query: str) -> list[dict[str, Any]]:
    """POST the query to the retrieval API; [] on any failure (fail-open)."""
    port = os.environ.get("INGEST_SESSIONS_PORT", "8741")
    url = f"http://127.0.0.1:{port}/api/retrieve"
    payload = json.dumps(
        {"query": query, "k": TOP_K, "candidate_k": CANDIDATE_K}
    ).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        print(f"ingest-sessions retrieve unreachable at {url}: {e}", file=sys.stderr)
        return []
    return body if isinstance(body, list) else []


def main() -> None:
    raw = sys.stdin.read().strip()
    if not raw:
        return
    try:
        hook_input = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Failed to parse hook input: {e}", file=sys.stderr)
        return

    query = build_query(hook_input.get("cwd", ""))
    if not query:
        return

    results = _retrieve(query)
    if not results:
        return

    context = format_injection(results)
    if not context:
        return

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
