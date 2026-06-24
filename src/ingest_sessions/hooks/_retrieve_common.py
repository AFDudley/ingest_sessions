#!/usr/bin/env python3
"""Shared retrieval-injection core for the cross-session relevance hooks.

ONE implementation of the retrieve→format pipeline, imported by BOTH injection
hooks so they never drift:

- ``session_retrieve.py`` — SessionStart hook (fires on resume/clear/compact,
  and *should* fire on a fresh ``claude`` start but the harness drops the
  injected ``additionalContext`` on ``source=startup``; that is is-408).
- ``first_prompt_retrieve.py`` — UserPromptSubmit hook that GUARANTEES the
  relevant-prior-reasoning context lands exactly once per session by injecting
  on the first user prompt when SessionStart did not already deliver it.

This module also owns the per-session **injection marker** that the two hooks
share to de-duplicate. The de-dup contract is documented at
``injection_is_known_good`` below — it is the load-bearing correctness
decision for is-408.

Everything here FAILS OPEN: any error, timeout, unreachable server, or empty
result yields no injection (and, for markers, best-effort I/O) — a hook must
never block the session or the prompt.

# See docs/plans/2026-06-22-retrieval-brain-is-565.md
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# Stage-1 candidates to rerank. Bounded for the sub-few-second hook budget
# (cross-encoder rerank is ~39 ms/candidate; 20 ≈ ~2s warm).
CANDIDATE_K = 20
TOP_K = 5
SNIPPET_CHARS = 240
HTTP_TIMEOUT_S = 12

# Sources whose SessionStart injection the harness reliably delivers. NOTE the
# deliberate omission of "startup": the is-408 bug is that SessionStart
# ``additionalContext`` is silently dropped on a fresh ``claude`` start
# (``source=startup``). A marker written by a startup injection therefore does
# NOT prove the context reached the model, so it must NOT suppress the
# UserPromptSubmit fallback. See ``injection_is_known_good``.
TRUSTED_SESSION_START_SOURCES = frozenset({"resume", "clear", "compact"})


# ---------------------------------------------------------------------------
# Query construction (repo current-focus seed)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Result formatting (pure)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Retrieval round trip (IO boundary; fail-open)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Per-session injection marker (shared de-dup state)
# ---------------------------------------------------------------------------


def _cache_root() -> Path:
    """Base cache dir, honoring ``XDG_CACHE_HOME`` (default ~/.cache)."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "ingest-sessions" / "injected"


def marker_path(session_id: str) -> Path:
    """Path of the per-session injection marker for *session_id*."""
    return _cache_root() / session_id


def read_marker(session_id: str) -> dict[str, Any] | None:
    """Read the injection marker for *session_id*; None if absent/unreadable."""
    if not session_id:
        return None
    path = marker_path(session_id)
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def write_marker(session_id: str, event: str, source: str | None = None) -> None:
    """Record that *event* injected for *session_id* (best-effort, fail-open).

    *source* is the SessionStart ``source`` (resume/startup/clear/compact) when
    the injecting event is SessionStart; None for UserPromptSubmit. Stored so
    ``injection_is_known_good`` can decide whether the fallback may skip.
    """
    if not session_id:
        return
    path = marker_path(session_id)
    payload: dict[str, Any] = {"event": event}
    if source is not None:
        payload["source"] = source
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))
    except OSError as e:
        print(f"ingest-sessions: could not write marker {path}: {e}", file=sys.stderr)


def injection_is_known_good(marker: dict[str, Any]) -> bool:
    """Decide whether a recorded injection is trusted to have been delivered.

    THE is-408 DE-DUP CONTRACT. The UserPromptSubmit fallback skips (does NOT
    re-inject) only when this returns True. The requirement is: inject exactly
    once per session in the normal case, and ALWAYS at least once even when
    SessionStart-startup is silently dropped by the harness.

    - A UserPromptSubmit injection is always known-good (we observed its output
      reach the model on the prompt it ran for).
    - A SessionStart injection is known-good only for a TRUSTED source
      (resume/clear/compact) — i.e. NOT ``startup``. SessionStart runs and
      writes its marker on startup too, but the harness drops the output on
      ``source=startup`` (is-408), so a startup marker does not prove delivery;
      treat it as not-yet-injected and let the fallback inject.

    Consequence: on a fresh ``claude`` the fallback always injects once (the
    startup SessionStart output having been dropped) → "always at least once".
    On resume/clear/compact the SessionStart injection lands and the fallback
    skips → "exactly once". If a future harness DOES deliver startup injection,
    the fallback would inject a second time — a benign redundancy, deliberately
    preferred over the is-408 failure of zero injection.
    """
    if marker.get("event") == "UserPromptSubmit":
        return True
    return marker.get("source") in TRUSTED_SESSION_START_SOURCES
