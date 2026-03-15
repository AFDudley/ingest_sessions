"""LCM-style summary generation for ingest_sessions.

Ported from Volt's LCM module (voltcode/src/session/lcm/).
Uses `claude --print` for LLM calls (covered by Max subscription).

Summary hierarchy:
- sprig (d1): summarizes a block of raw messages
- bindle (d2): condenses multiple sprigs into a higher-level summary

Three-level escalation guarantees convergence (from Volt's
compaction-escalation.ts):
1. Normal summarization
2. Aggressive (shorter target, bullet-point style)
3. Deterministic truncation (no LLM, binary search for longest
   prefix under token budget — always terminates)

This is an algorithmic convergence guarantee, not error-handling.
Without it, a verbose LLM response deadlocks the context window.

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from typing import Any


# ---------------------------------------------------------------------------
# Prompts — copied verbatim from Volt upward/ prompts
# ---------------------------------------------------------------------------

SUMMARIZE_D1_PROMPT = """\
# Upward d1 Message Summarization Prompt

You are summarizing a chronological slice of a coding conversation.

## Goal

Produce one narrative summary that lets a future model continue work \
without reading the full underlying messages.

Write the summary as a coherent story of the turns in order:
- How this segment started (initial ask, context, constraints)
- What happened while working (key decisions, attempts, discoveries, changes)
- How this segment ended (resolved items, abandoned paths, and in-progress work)

## Required Content

Include:
- User intent and constraints that shaped implementation
- Key technical decisions and why they mattered
- Concrete code changes (files, functions, components, APIs)
- Important tool findings (errors, test results, search findings) that changed direction
- Current end-state: done, deferred, blocked, or still in progress

## Style

- Keep chronological flow explicit
- Prefer concrete details over abstract phrasing
- Keep prose compact and information-dense
- Use clear section headings and bullets where helpful

## Output Guidance

- Target length: 1800-2000 tokens
- Do not add synthetic metadata headers, frontmatter, IDs, token estimates, \
or placeholder fields
- Focus on durable context needed for continuation"""

CONDENSE_D2_PROMPT = """\
# Upward d2 Summary Condensation Prompt

You are condensing multiple chronological summaries into one higher-level \
narrative summary.

## Goal

Merge the input summaries into a single summary that preserves continuity \
and can be used to keep working without re-reading all source summaries.

Treat the input as a time-ordered sequence and produce a unified story:
- How the overall effort began
- How the work evolved across the grouped summaries
- Where the effort stands at the end

## Required Content

Include:
- Major decisions and how they shifted the implementation path
- Important code changes and touched areas of the codebase
- Key discoveries from tools/tests/errors that changed outcomes
- Explicit transitions: what was pursued, what was dropped, what replaced it
- Final state: completed work, open threads, and next likely actions

## Style

- Preserve chronology across the merged summaries
- Resolve repetition by merging overlapping points cleanly
- Keep language precise and implementation-oriented
- Use structured headings and concise bullets where useful

## Output Guidance

- Target length: 1800-2000 tokens
- Do not add synthetic metadata headers, frontmatter, IDs, token estimates, \
or placeholder fields
- Prioritize context that enables accurate continuation of work"""

AGGRESSIVE_DIRECTIVE = """\

## Aggressive Compression Override
- You are in escalation pass 2 because pass 1 was not shorter than input.
- Compress more aggressively than normal while preserving task-critical facts.
- Remove repetition, low-value narrative, and secondary detail.
- Output must still be coherent and safe for continuation."""


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Rough token count: ~4 chars per token. Good enough for budget checks."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Deterministic ID generation (from Volt Summary.generateId)
# ---------------------------------------------------------------------------


def generate_summary_id(content: str, timestamp: int) -> str:
    """Deterministic summary ID: sum_ + SHA-256(content + timestamp)[:16]."""
    h = hashlib.sha256((content + str(timestamp)).encode()).hexdigest()[:16]
    return f"sum_{h}"


# ---------------------------------------------------------------------------
# Message formatting (from Volt LcmSummarize.formatMessagesForSummary)
# ---------------------------------------------------------------------------


def _extract_content_text(raw_json: str) -> str:
    """Extract readable text content from a raw JSONL record.

    Raises json.JSONDecodeError on malformed input — callers at system
    boundaries must handle this.
    """
    record = json.loads(raw_json)
    msg = record.get("message", {})
    if not isinstance(msg, dict):
        return ""
    content = msg.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                name = block.get("name", "unknown")
                inp = block.get("input", {})
                inp_str = json.dumps(inp) if isinstance(inp, dict) else str(inp)
                if len(inp_str) > 500:
                    inp_str = inp_str[:500] + "..."
                parts.append(f"[Tool: {name}]\nInput: {inp_str}")
            elif block.get("type") == "tool_result":
                tool_id = block.get("tool_use_id", "")
                result_content = block.get("content", "")
                if isinstance(result_content, str):
                    if len(result_content) > 1000:
                        result_content = result_content[:1000] + "..."
                    parts.append(f"[Tool Result {tool_id}]\n{result_content}")
                elif isinstance(result_content, list):
                    for sub in result_content:
                        if isinstance(sub, dict) and sub.get("type") == "text":
                            text = sub.get("text", "")
                            if len(text) > 1000:
                                text = text[:1000] + "..."
                            parts.append(f"[Tool Result {tool_id}]\n{text}")
        return "\n".join(parts)
    return ""


def format_messages_for_summary(records: list[dict[str, Any]]) -> str:
    """Format DuckDB record rows into text for the summarization prompt.

    Each record dict should have keys: uuid, type, raw.
    Skips records with malformed JSON (log-worthy but not fatal at this layer).
    """
    parts: list[str] = []
    for rec in records:
        uuid = rec.get("uuid", "?")
        role = rec.get("type", "unknown")
        parts.append(f"[Message {uuid}] ({role})")
        try:
            text = _extract_content_text(rec.get("raw", "{}"))
        except json.JSONDecodeError:
            text = "[malformed record]"
        if text:
            parts.append(text)
        parts.append("")  # blank line between messages
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Three-level escalation (from Volt compaction-escalation.ts)
#
# This is a convergence guarantee, not error handling. Each level uses
# a distinct strategy; level 3 is deterministic and always terminates.
# Without this, a verbose LLM response would deadlock the context window.
# ---------------------------------------------------------------------------


def _should_accept_output(output: str, input_tokens: int) -> bool:
    """Accept if non-empty and strictly fewer tokens than input."""
    trimmed = output.strip()
    if not trimmed:
        return False
    if not isinstance(input_tokens, (int, float)) or input_tokens <= 1:
        return False
    return estimate_tokens(trimmed) < input_tokens


def build_deterministic_fallback(
    source_text: str,
    input_tokens: int,
) -> str:
    """Level 3: binary-search for longest prefix under token budget.

    Pure algorithm, no LLM. Guaranteed to produce output shorter than
    input_tokens (convergence guarantee for the summary DAG).
    """
    source = source_text.strip()
    if not source:
        return ""
    target = max(1, int(input_tokens))
    suffix = f"\n[LCM summarize fallback; truncated from {target} tokens]"

    lo, hi, best = 1, len(source), ""
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = source[:mid].rstrip() + suffix
        if estimate_tokens(candidate) < target:
            best = candidate
            lo = mid + 1
        else:
            hi = mid - 1

    if best:
        return best

    # Edge case: suffix alone exceeds budget. Truncate without suffix.
    lo, hi, best = 1, len(source), ""
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = source[:mid].rstrip()
        if candidate and estimate_tokens(candidate) < target:
            best = candidate
            lo = mid + 1
        else:
            hi = mid - 1

    return best or source[:1]


# ---------------------------------------------------------------------------
# File ID extraction — finds blob markers in summary content
# ---------------------------------------------------------------------------

_FILE_ID_PATTERN = re.compile(r"file_[0-9a-f]{16}")


def extract_file_ids(content: str) -> list[str]:
    """Extract deduplicated, sorted file IDs from text."""
    return sorted(set(_FILE_ID_PATTERN.findall(content)))


# ---------------------------------------------------------------------------
# LLM calls via claude --print (covered by Max subscription)
# ---------------------------------------------------------------------------


def _find_claude() -> str:
    """Resolve the ``claude`` binary path.

    Falls back to ``~/.local/bin/claude`` when PATH is minimal
    (e.g. systemd-managed server processes).
    """
    found = shutil.which("claude")
    if found:
        return found
    fallback = os.path.expanduser("~/.local/bin/claude")
    if os.path.isfile(fallback) and os.access(fallback, os.X_OK):
        return fallback
    raise FileNotFoundError("claude binary not found on PATH or at ~/.local/bin/claude")


def _call_claude(system_prompt: str, user_message: str) -> str:
    """Call claude --print with a system prompt and user message.

    Returns the text response. Raises on non-zero exit.
    Strips CLAUDECODE from env to allow nested subprocess calls.
    """
    claude_bin = _find_claude()
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    result = subprocess.run(
        [
            claude_bin,
            "--print",
            "--append-system-prompt",
            system_prompt,
            "-p",
            user_message,
            "--max-turns",
            "1",
        ],
        capture_output=True,
        text=False,
        timeout=120,
        env=env,
    )
    stderr = result.stderr.decode("utf-8", errors="replace").replace("\x00", "")
    if result.returncode != 0:
        raise RuntimeError(
            f"claude --print failed (exit {result.returncode}): {stderr[:500]}"
        )
    return result.stdout.decode("utf-8", errors="replace").replace("\x00", "").strip()


def summarize_messages(
    records: list[dict[str, Any]],
    previous_summary_context: str | None = None,
) -> str:
    """Summarize a block of messages into a sprig (d1) summary.

    Uses three-level escalation for guaranteed convergence.
    """
    formatted = format_messages_for_summary(records)
    input_tokens = estimate_tokens(formatted)

    if previous_summary_context:
        user_msg = (
            "The preceding summaries in this chain are as follows:\n\n"
            "<preceding_summaries>\n"
            f"{previous_summary_context}\n"
            "</preceding_summaries>\n\n"
            "The new segment is:\n\n"
            "<messages>\n"
            f"{formatted}\n"
            "</messages>\n\n"
            "Summarize only the new segment while maintaining narrative "
            "continuity with the preceding summaries.\n"
            "Do not continue or answer the source conversation directly."
        )
    else:
        user_msg = (
            "The following content is source material to summarize "
            "according to the system instructions above.\n\n"
            "<messages>\n"
            f"{formatted}\n"
            "</messages>\n\n"
            "Produce a chronological narrative summary of this source material.\n"
            "Do not continue or answer the source conversation directly."
        )

    # Level 1: normal
    try:
        output = _call_claude(SUMMARIZE_D1_PROMPT, user_msg)
        if _should_accept_output(output, input_tokens):
            return output
    except Exception as exc:
        print(f"[summarize] level 1 failed: {exc}", file=sys.stderr)

    # Level 2: aggressive
    try:
        output = _call_claude(SUMMARIZE_D1_PROMPT + AGGRESSIVE_DIRECTIVE, user_msg)
        if _should_accept_output(output, input_tokens):
            return output
    except Exception as exc:
        print(f"[summarize] level 2 failed: {exc}", file=sys.stderr)

    # Level 3: deterministic truncation (no LLM, always converges)
    return build_deterministic_fallback(formatted, input_tokens)


def condense_summaries(
    summaries: list[dict[str, Any]],
    previous_summary_context: str | None = None,
) -> str:
    """Condense multiple sprig summaries into a bindle (d2) summary.

    Each summary dict should have keys: summary_id, content.
    Uses three-level escalation for guaranteed convergence.
    """
    parts: list[str] = []
    parent_ids: list[str] = []
    for s in summaries:
        sid = s["summary_id"]
        parent_ids.append(sid)
        parts.append(f"--- Summary {sid} ---")
        parts.append(s["content"])
        parts.append("")
    formatted = "\n".join(parts)

    input_tokens = sum(
        s.get("token_count", estimate_tokens(s["content"])) for s in summaries
    )

    if previous_summary_context:
        user_msg = (
            "The preceding summaries in this chain are as follows:\n\n"
            "<preceding_summaries>\n"
            f"{previous_summary_context}\n"
            "</preceding_summaries>\n\n"
            "The new segment is:\n\n"
            "<source_summaries>\n"
            f"{formatted}\n"
            "</source_summaries>\n\n"
            "Summarize only the new segment while maintaining narrative "
            "continuity with the preceding summaries.\n"
            "Do not continue or answer the source material directly."
        )
    else:
        user_msg = (
            "## Input Summary IDs\n\n"
            f"{', '.join(parent_ids)}\n\n"
            "## Summaries to Condense\n\n"
            f"{formatted}\n\n"
            "Produce a chronological narrative summary from this source material.\n"
            "Do not continue or answer the source material directly."
        )

    # Level 1: normal
    try:
        output = _call_claude(CONDENSE_D2_PROMPT, user_msg)
        if _should_accept_output(output, input_tokens):
            return output
    except Exception as exc:
        print(f"[condense] level 1 failed: {exc}", file=sys.stderr)

    # Level 2: aggressive
    try:
        output = _call_claude(CONDENSE_D2_PROMPT + AGGRESSIVE_DIRECTIVE, user_msg)
        if _should_accept_output(output, input_tokens):
            return output
    except Exception as exc:
        print(f"[condense] level 2 failed: {exc}", file=sys.stderr)

    # Level 3: deterministic truncation (no LLM, always converges)
    return build_deterministic_fallback(formatted, input_tokens)
