# LCM Summary DAG for Context Recovery

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an LCM-style summary DAG to ingest_sessions so that after `/clear`, a SessionStart hook can inject a high-quality context summary — eliminating the "blank slate" problem.

**Architecture:** Messages already flow into DuckDB via the existing watchdog/ingestion pipeline. We add a `summaries` table that stores hierarchical summary nodes (sprigs summarize messages, bindles summarize sprigs). A new `summarize` MCP tool triggers summarization of unsummarized messages for a session using `claude --print` (covered by Max subscription — no API costs). A SessionStart hook script queries the DAG and returns `additionalContext` to Claude Code.

**Tech Stack:** Python 3.11+, DuckDB, `claude --print` for LLM calls, shell hook script, existing ingest_sessions MCP server infrastructure.

**Key design decisions from Volt's LCM (ported, not reinvented):**
- Summary hierarchy: **sprigs** (d1, summarize raw messages) → **bindles** (d2, condense multiple sprigs)
- Three-level escalation: normal → aggressive → deterministic truncation (guaranteed convergence)
- Deterministic IDs: `sum_` + SHA-256(content + timestamp)[:16]
- Prompts: copied verbatim from Volt `packages/voltcode/src/session/lcm/prompts/upward/`
- File IDs propagated through DAG (pattern: `[Large File Stored: file_xxx]`)

**What we are NOT porting:**
- Embedded PostgreSQL (we use DuckDB)
- Context assembly into Claude Code's message pipeline (we inject via hook)
- LCM tools as native Claude Code tools (we use MCP `query` tool)
- Large file explorers (out of scope)
- The `context.ts` / `strategy.ts` compaction loop (we trigger manually via MCP tool)

---

### Task 1: Add summaries table to DuckDB schema

**Files:**
- Modify: `src/ingest_sessions/core.py:45-93` (inside `create_tables`)
- Test: `tests/test_server.py`

**Step 1: Write the failing test**

Add to `tests/test_server.py`:

```python
@pytest.mark.asyncio
async def test_summaries_table_exists(tmp_path: Path):
    """The summaries table should exist after server startup."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT column_name FROM information_schema.columns WHERE table_name = 'summaries' ORDER BY ordinal_position"},
        )
        rows = json.loads(_text(result))
        col_names = [r["column_name"] for r in rows]
        assert "summary_id" in col_names
        assert "session_id" in col_names
        assert "kind" in col_names
        assert "content" in col_names
        assert "token_count" in col_names
        assert "parent_ids" in col_names
        assert "message_uuids" in col_names
        assert "file_ids" in col_names
        assert "created_at" in col_names
```

**Step 2: Run test to verify it fails**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_server.py::test_summaries_table_exists -v`
Expected: FAIL — `summaries` table doesn't exist.

**Step 3: Write minimal implementation**

In `src/ingest_sessions/core.py`, add to `create_tables()` after the `file_mtimes` table:

```python
    db.execute("""
        CREATE TABLE IF NOT EXISTS summaries (
            summary_id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            condensation_order INTEGER NOT NULL DEFAULT 1,
            content TEXT NOT NULL,
            token_count INTEGER NOT NULL,
            parent_ids VARCHAR[] DEFAULT [],
            message_uuids VARCHAR[] DEFAULT [],
            file_ids VARCHAR[] DEFAULT [],
            created_at BIGINT NOT NULL
        )
    """)
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_summaries_session_id ON summaries(session_id)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind)"
    )
```

Column semantics (from Volt's `Summary` namespace):
- `summary_id`: deterministic `sum_` + SHA-256(content + timestamp)[:16]
- `session_id`: which Claude Code session this covers
- `kind`: `sprig` (summarizes messages) or `bindle` (summarizes sprigs)
- `condensation_order`: 1 for sprigs, 2+ for bindles
- `content`: the summary text
- `token_count`: estimated token count of content
- `parent_ids`: for bindles, the `summary_id`s of child sprigs
- `message_uuids`: for sprigs, the `uuid`s of summarized records
- `file_ids`: LCM file IDs propagated through the DAG
- `created_at`: epoch milliseconds

**Step 4: Run test to verify it passes**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_server.py::test_summaries_table_exists -v`
Expected: PASS

**Step 5: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add src/ingest_sessions/core.py tests/test_server.py
git commit -m "[ingest-sessions] Add summaries table for LCM summary DAG"
```

---

### Task 2: Add summary generation module

This is the core algorithm, ported from Volt's `summarize.ts`, `condense.ts`, and `compaction-escalation.ts`. It calls `claude --print` instead of the Vercel AI SDK.

**Files:**
- Create: `src/ingest_sessions/summarize.py`
- Test: `tests/test_summarize.py`

**Step 1: Write the failing test**

Create `tests/test_summarize.py`:

```python
"""Tests for the summarize module.

These test the pure functions (formatting, ID generation, escalation)
without LLM calls. LLM-dependent tests are in test_summarize_e2e.py.
"""

import json
from ingest_sessions.summarize import (
    format_messages_for_summary,
    generate_summary_id,
    should_accept_output,
    build_deterministic_fallback,
    estimate_tokens,
)


def test_generate_summary_id_deterministic():
    """Same content + timestamp = same ID."""
    id1 = generate_summary_id("hello world", 1000)
    id2 = generate_summary_id("hello world", 1000)
    assert id1 == id2
    assert id1.startswith("sum_")
    assert len(id1) == 20  # "sum_" + 16 hex chars


def test_generate_summary_id_differs_on_content():
    """Different content = different ID."""
    id1 = generate_summary_id("hello", 1000)
    id2 = generate_summary_id("world", 1000)
    assert id1 != id2


def test_format_messages_for_summary():
    """Messages are formatted with role and content."""
    records = [
        {"uuid": "r1", "type": "user", "raw": json.dumps({
            "message": {"role": "user", "content": "hello"},
        })},
        {"uuid": "r2", "type": "assistant", "raw": json.dumps({
            "message": {"role": "assistant", "content": [{"type": "text", "text": "hi there"}]},
        })},
    ]
    result = format_messages_for_summary(records)
    assert "[Message r1] (user)" in result
    assert "hello" in result
    assert "[Message r2] (assistant)" in result
    assert "hi there" in result


def test_should_accept_output_shorter():
    """Accept output that is shorter than input."""
    assert should_accept_output("short summary", 100) is True


def test_should_accept_output_longer():
    """Reject output that is longer than input."""
    long_text = "word " * 500
    assert should_accept_output(long_text, 10) is False


def test_should_accept_output_empty():
    """Reject empty output."""
    assert should_accept_output("", 100) is False
    assert should_accept_output("   ", 100) is False


def test_build_deterministic_fallback():
    """Fallback truncates to fit under token budget."""
    source = "word " * 200
    result = build_deterministic_fallback(source, input_tokens=50)
    assert estimate_tokens(result) < 50
    assert "truncated" in result.lower()


def test_estimate_tokens():
    """Rough token estimation works."""
    tokens = estimate_tokens("hello world this is a test")
    assert 4 <= tokens <= 10  # ~6 tokens, rough estimate
```

**Step 2: Run test to verify it fails**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_summarize.py -v`
Expected: FAIL — module doesn't exist.

**Step 3: Write the implementation**

Create `src/ingest_sessions/summarize.py`:

```python
"""LCM-style summary generation for ingest_sessions.

Ported from Volt's LCM module (voltcode/src/session/lcm/).
Uses `claude --print` for LLM calls (covered by Max subscription).

Summary hierarchy:
- sprig (d1): summarizes a block of raw messages
- bindle (d2): condenses multiple sprigs into a higher-level summary

Three-level escalation guarantees convergence:
1. Normal summarization
2. Aggressive (shorter target, bullet-point style)
3. Deterministic truncation (no LLM, always converges)
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
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
    """Extract readable text content from a raw JSONL record."""
    try:
        record = json.loads(raw_json)
    except json.JSONDecodeError:
        return ""
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
    """
    parts: list[str] = []
    for rec in records:
        uuid = rec.get("uuid", "?")
        role = rec.get("type", "unknown")
        parts.append(f"[Message {uuid}] ({role})")
        text = _extract_content_text(rec.get("raw", "{}"))
        if text:
            parts.append(text)
        parts.append("")  # blank line between messages
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Three-level escalation (from Volt compaction-escalation.ts)
# ---------------------------------------------------------------------------


def should_accept_output(output: str, input_tokens: int) -> bool:
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
    """Level 3 fallback: binary-search for longest prefix under token budget."""
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

    # Try without suffix
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
# File ID extraction (from Volt LcmSummarize.extractFileIds)
# ---------------------------------------------------------------------------

import re

_FILE_ID_PATTERN = re.compile(
    r"\[Large File Stored:\s*(file_[0-9a-f]{16})\]"
    r"|\[Large User Text Stored:\s*(file_[0-9a-f]{16})\]"
    r"|LCM File ID:\s*(file_[0-9a-f]{16})"
    r'|file_id\s+"(file_[0-9a-f]{16})"'
)


def extract_file_ids(content: str) -> list[str]:
    """Extract deduplicated, sorted file IDs from text."""
    ids: set[str] = set()
    for match in _FILE_ID_PATTERN.finditer(content):
        fid = match.group(1) or match.group(2) or match.group(3) or match.group(4)
        if fid:
            ids.add(fid)
    return sorted(ids)


# ---------------------------------------------------------------------------
# LLM calls via claude --print (covered by Max subscription)
# ---------------------------------------------------------------------------


def _call_claude(system_prompt: str, user_message: str) -> str:
    """Call claude --print with a system prompt and user message.

    Returns the text response. Raises on non-zero exit.
    """
    # Build the full prompt with system context prepended
    full_prompt = f"<system>{system_prompt}</system>\n\n{user_message}"
    result = subprocess.run(
        ["claude", "--print", "-p", full_prompt, "--max-turns", "1"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"claude --print failed (exit {result.returncode}): {result.stderr[:500]}"
        )
    return result.stdout.strip()


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
    output = _call_claude(SUMMARIZE_D1_PROMPT, user_msg)
    if should_accept_output(output, input_tokens):
        return output

    # Level 2: aggressive
    output = _call_claude(SUMMARIZE_D1_PROMPT + AGGRESSIVE_DIRECTIVE, user_msg)
    if should_accept_output(output, input_tokens):
        return output

    # Level 3: deterministic fallback
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

    input_tokens = sum(s.get("token_count", estimate_tokens(s["content"])) for s in summaries)

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
    output = _call_claude(CONDENSE_D2_PROMPT, user_msg)
    if should_accept_output(output, input_tokens):
        return output

    # Level 2: aggressive
    output = _call_claude(CONDENSE_D2_PROMPT + AGGRESSIVE_DIRECTIVE, user_msg)
    if should_accept_output(output, input_tokens):
        return output

    # Level 3: deterministic fallback
    return build_deterministic_fallback(formatted, input_tokens)
```

**Step 4: Run test to verify it passes**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_summarize.py -v`
Expected: PASS (all tests exercise pure functions, no LLM calls)

**Step 5: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add src/ingest_sessions/summarize.py tests/test_summarize.py
git commit -m "[ingest-sessions] Add LCM summary generation module with three-level escalation"
```

---

### Task 3: Add `summarize` MCP tool

This tool triggers DAG maintenance for a session: summarize unsummarized messages into sprigs, condense sprigs into bindles when enough accumulate.

**Files:**
- Modify: `src/ingest_sessions/server.py` (add tool to `list_tools` and `call_tool`)
- Create: `src/ingest_sessions/dag.py` (DAG maintenance logic)
- Test: `tests/test_dag.py`

**Step 1: Write the failing test**

Create `tests/test_dag.py`:

```python
"""Tests for DAG maintenance logic.

Tests the database operations (insert/query summaries) without LLM calls.
The summarization functions are mocked.
"""

import json
import time
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from ingest_sessions.core import create_tables
from ingest_sessions.dag import (
    get_unsummarized_messages,
    insert_sprig,
    insert_bindle,
    get_sprigs_for_session,
    get_latest_summary_for_session,
    run_summarize_session,
    SPRIG_CHUNK_SIZE,
    BINDLE_THRESHOLD,
)
from ingest_sessions.summarize import generate_summary_id, estimate_tokens


@pytest.fixture
def db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(conn)
    return conn


def _seed_records(db: duckdb.DuckDBPyConnection, session_id: str, count: int) -> list[str]:
    """Insert count records and return their UUIDs."""
    uuids = []
    for i in range(count):
        uuid = f"rec-{session_id}-{i:04d}"
        raw = json.dumps({
            "message": {"role": "user" if i % 2 == 0 else "assistant",
                        "content": f"Message {i} content here"},
        })
        db.execute(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            [uuid, session_id, "user" if i % 2 == 0 else "assistant",
             f"2026-03-01T{10 + i // 60:02d}:{i % 60:02d}:00.000Z", None, raw],
        )
        uuids.append(uuid)
    return uuids


def test_get_unsummarized_messages_all_new(db: duckdb.DuckDBPyConnection):
    """All messages are unsummarized when no summaries exist."""
    uuids = _seed_records(db, "sess-1", 10)
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 10
    assert result[0]["uuid"] == uuids[0]


def test_get_unsummarized_messages_some_covered(db: duckdb.DuckDBPyConnection):
    """Messages covered by a sprig are excluded."""
    uuids = _seed_records(db, "sess-1", 10)
    insert_sprig(db, "sess-1", "test summary", uuids[:5], [])
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 5
    result_uuids = [r["uuid"] for r in result]
    assert uuids[5] in result_uuids
    assert uuids[0] not in result_uuids


def test_insert_sprig(db: duckdb.DuckDBPyConnection):
    """insert_sprig creates a sprig row with correct fields."""
    uuids = _seed_records(db, "sess-1", 5)
    sid = insert_sprig(db, "sess-1", "summary content", uuids, ["file_abc"])
    assert sid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [sid]).fetchone()
    assert row is not None
    assert row[1] == "sess-1"  # session_id
    assert row[2] == "sprig"   # kind


def test_insert_bindle(db: duckdb.DuckDBPyConnection):
    """insert_bindle creates a bindle row referencing parent sprigs."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:], [])
    bid = insert_bindle(db, "sess-1", "condensed content", [s1, s2], [])
    assert bid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [bid]).fetchone()
    assert row is not None
    assert row[2] == "bindle"  # kind
    assert row[3] == 2         # condensation_order


def test_get_sprigs_for_session(db: duckdb.DuckDBPyConnection):
    """get_sprigs_for_session returns sprigs not yet condensed into bindles."""
    uuids = _seed_records(db, "sess-1", 15)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:10], [])
    s3 = insert_sprig(db, "sess-1", "summary 3", uuids[10:], [])
    # Condense s1 and s2 into a bindle
    insert_bindle(db, "sess-1", "condensed", [s1, s2], [])
    # Only s3 should be uncondensed
    uncondensed = get_sprigs_for_session(db, "sess-1", uncondensed_only=True)
    assert len(uncondensed) == 1
    assert uncondensed[0]["summary_id"] == s3


def test_get_latest_summary_for_session(db: duckdb.DuckDBPyConnection):
    """Returns the highest-order summary (bindle > sprig) for context assembly."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "sprig 1", uuids[:5], [])
    s2 = insert_sprig(db, "sess-1", "sprig 2", uuids[5:], [])
    insert_bindle(db, "sess-1", "the bindle", [s1, s2], [])
    latest = get_latest_summary_for_session(db, "sess-1")
    assert latest is not None
    assert latest["kind"] == "bindle"
    assert latest["content"] == "the bindle"


def test_run_summarize_session_creates_sprigs(db: duckdb.DuckDBPyConnection):
    """run_summarize_session chunks messages into sprigs."""
    _seed_records(db, "sess-1", SPRIG_CHUNK_SIZE * 2 + 3)
    with patch("ingest_sessions.dag.summarize_messages", return_value="mock summary"):
        result = run_summarize_session(db, "sess-1")
    assert result["sprigs_created"] == 2  # 2 full chunks, remainder < chunk size
    sprigs = get_sprigs_for_session(db, "sess-1")
    assert len(sprigs) == 2
```

**Step 2: Run test to verify it fails**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_dag.py -v`
Expected: FAIL — module `dag` doesn't exist.

**Step 3: Write the implementation**

Create `src/ingest_sessions/dag.py`:

```python
"""DAG maintenance for LCM summary hierarchy.

Coordinates the creation and condensation of summary nodes:
- Chunks unsummarized messages into sprigs (d1)
- Condenses accumulated sprigs into bindles (d2)

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import time
from typing import Any

import duckdb

from ingest_sessions.summarize import (
    condense_summaries,
    estimate_tokens,
    extract_file_ids,
    generate_summary_id,
    summarize_messages,
)

# How many messages per sprig summary
SPRIG_CHUNK_SIZE = 20

# How many uncondensed sprigs before creating a bindle
BINDLE_THRESHOLD = 4


def get_unsummarized_messages(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> list[dict[str, Any]]:
    """Get records not yet covered by any sprig summary.

    Returns records ordered by timestamp, with keys: uuid, type, raw.
    """
    rows = db.execute(
        """
        SELECT r.uuid, r.type, r.raw
        FROM records r
        WHERE r.session_id = ?
          AND r.type IN ('user', 'assistant')
          AND r.uuid NOT IN (
              SELECT UNNEST(message_uuids) FROM summaries
              WHERE session_id = ? AND kind = 'sprig'
          )
        ORDER BY r.timestamp ASC
        """,
        [session_id, session_id],
    ).fetchall()
    return [{"uuid": r[0], "type": r[1], "raw": r[2]} for r in rows]


def insert_sprig(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    content: str,
    message_uuids: list[str],
    file_ids: list[str],
) -> str:
    """Insert a sprig (d1) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)
    db.execute(
        "INSERT OR IGNORE INTO summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary_id,
            session_id,
            "sprig",
            1,  # condensation_order
            content,
            token_count,
            [],  # parent_ids (sprigs have none)
            message_uuids,
            file_ids,
            ts,
        ],
    )
    return summary_id


def insert_bindle(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    content: str,
    parent_ids: list[str],
    file_ids: list[str],
) -> str:
    """Insert a bindle (d2) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)
    db.execute(
        "INSERT OR IGNORE INTO summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary_id,
            session_id,
            "bindle",
            2,  # condensation_order
            content,
            token_count,
            parent_ids,
            [],  # message_uuids (bindles link via parent sprigs)
            file_ids,
            ts,
        ],
    )
    return summary_id


def get_sprigs_for_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    uncondensed_only: bool = False,
) -> list[dict[str, Any]]:
    """Get sprig summaries for a session.

    If uncondensed_only=True, excludes sprigs already referenced
    as parents by a bindle.
    """
    if uncondensed_only:
        rows = db.execute(
            """
            SELECT summary_id, content, token_count, created_at
            FROM summaries
            WHERE session_id = ? AND kind = 'sprig'
              AND summary_id NOT IN (
                  SELECT UNNEST(parent_ids) FROM summaries
                  WHERE session_id = ? AND kind = 'bindle'
              )
            ORDER BY created_at ASC
            """,
            [session_id, session_id],
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT summary_id, content, token_count, created_at
            FROM summaries
            WHERE session_id = ? AND kind = 'sprig'
            ORDER BY created_at ASC
            """,
            [session_id],
        ).fetchall()
    return [
        {
            "summary_id": r[0],
            "content": r[1],
            "token_count": r[2],
            "created_at": r[3],
        }
        for r in rows
    ]


def get_latest_summary_for_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> dict[str, Any] | None:
    """Get the most recent highest-order summary for context assembly.

    Prefers bindles over sprigs. Returns None if no summaries exist.
    """
    row = db.execute(
        """
        SELECT summary_id, kind, condensation_order, content, token_count,
               parent_ids, created_at
        FROM summaries
        WHERE session_id = ?
        ORDER BY condensation_order DESC, created_at DESC
        LIMIT 1
        """,
        [session_id],
    ).fetchone()
    if row is None:
        return None
    return {
        "summary_id": row[0],
        "kind": row[1],
        "condensation_order": row[2],
        "content": row[3],
        "token_count": row[4],
        "parent_ids": row[5],
        "created_at": row[6],
    }


def assemble_context_for_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> str | None:
    """Assemble recovery context for injection after /clear.

    Returns a formatted string combining:
    1. The highest-order summary (bindle if available)
    2. Any uncondensed sprigs (more recent than the bindle)
    3. Pointers to the query tool for deeper retrieval

    Returns None if no summaries exist for the session.
    """
    # Get all bindles (ordered by recency)
    bindles = db.execute(
        """
        SELECT summary_id, content, token_count, created_at
        FROM summaries
        WHERE session_id = ? AND kind = 'bindle'
        ORDER BY created_at DESC
        """,
        [session_id],
    ).fetchall()

    # Get uncondensed sprigs
    uncondensed = get_sprigs_for_session(db, session_id, uncondensed_only=True)

    if not bindles and not uncondensed:
        return None

    parts: list[str] = []
    parts.append(
        "This session is being continued after a /clear. "
        "The following is a lossless summary of the prior conversation, "
        "generated by the LCM summary DAG. The full transcript is "
        "searchable via the mcp__ingest-sessions__query tool."
    )
    parts.append("")

    if bindles:
        parts.append("## Condensed Session History")
        parts.append("")
        # Most recent bindle first (usually just one)
        for b in bindles[:1]:
            parts.append(f"[Summary ID: {b[0]}]")
            parts.append(b[1])
            parts.append("")

    if uncondensed:
        parts.append("## Recent Activity (not yet condensed)")
        parts.append("")
        for s in uncondensed:
            parts.append(f"[Summary ID: {s['summary_id']}]")
            parts.append(s["content"])
            parts.append("")

    return "\n".join(parts)


def run_summarize_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> dict[str, int]:
    """Run one pass of DAG maintenance for a session.

    1. Chunk unsummarized messages into sprigs (SPRIG_CHUNK_SIZE each)
    2. If enough uncondensed sprigs exist, condense into a bindle

    Returns counts of what was created.
    """
    unsummarized = get_unsummarized_messages(db, session_id)
    sprigs_created = 0
    bindles_created = 0

    # Get the most recent sprig for narrative continuity
    existing_sprigs = get_sprigs_for_session(db, session_id)
    previous_context = existing_sprigs[-1]["content"] if existing_sprigs else None

    # Create sprigs from full chunks only (remainder stays for next run)
    full_chunks = len(unsummarized) // SPRIG_CHUNK_SIZE
    for i in range(full_chunks):
        chunk = unsummarized[i * SPRIG_CHUNK_SIZE : (i + 1) * SPRIG_CHUNK_SIZE]
        content = summarize_messages(chunk, previous_summary_context=previous_context)
        file_ids = extract_file_ids(
            "\n".join(r.get("raw", "") for r in chunk)
        )
        uuids = [r["uuid"] for r in chunk]
        insert_sprig(db, session_id, content, uuids, file_ids)
        previous_context = content
        sprigs_created += 1

    # Condense uncondensed sprigs into a bindle if threshold met
    uncondensed = get_sprigs_for_session(db, session_id, uncondensed_only=True)
    if len(uncondensed) >= BINDLE_THRESHOLD:
        # Get the most recent bindle for narrative continuity
        latest_bindle = db.execute(
            """
            SELECT content FROM summaries
            WHERE session_id = ? AND kind = 'bindle'
            ORDER BY created_at DESC LIMIT 1
            """,
            [session_id],
        ).fetchone()
        bindle_context = latest_bindle[0] if latest_bindle else None

        content = condense_summaries(uncondensed, previous_summary_context=bindle_context)
        parent_ids = [s["summary_id"] for s in uncondensed]
        all_file_ids: list[str] = []
        for s in uncondensed:
            all_file_ids.extend(extract_file_ids(s["content"]))
        insert_bindle(db, session_id, content, parent_ids, sorted(set(all_file_ids)))
        bindles_created += 1

    return {
        "sprigs_created": sprigs_created,
        "bindles_created": bindles_created,
        "unsummarized_remaining": len(unsummarized) - (full_chunks * SPRIG_CHUNK_SIZE),
    }
```

**Step 4: Run test to verify it passes**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_dag.py -v`
Expected: PASS

**Step 5: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add src/ingest_sessions/dag.py tests/test_dag.py
git commit -m "[ingest-sessions] Add DAG maintenance module for sprig/bindle lifecycle"
```

---

### Task 4: Wire `summarize` and `context` MCP tools into the server

**Files:**
- Modify: `src/ingest_sessions/server.py` (add to `list_tools` and `call_tool`)
- Test: `tests/test_server.py`

**Step 1: Write the failing test**

Add to `tests/test_server.py`:

```python
@pytest.mark.asyncio
async def test_server_lists_summarize_tool(tmp_path: Path):
    """The server should list 'summarize' and 'context' as tools."""
    async with run_server(tmp_path) as (client, _):
        result = await client.list_tools()
        tool_names = [t.name for t in result.tools]
        assert "summarize" in tool_names
        assert "context" in tool_names


@pytest.mark.asyncio
async def test_context_tool_returns_none_without_summaries(tmp_path: Path):
    """Context tool should indicate no summaries when DAG is empty."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool("context", {"session_id": "nonexistent"})
        body = json.loads(_text(result))
        assert body.get("context") is None
```

**Step 2: Run test to verify it fails**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_server.py::test_server_lists_summarize_tool tests/test_server.py::test_context_tool_returns_none_without_summaries -v`
Expected: FAIL — tools don't exist.

**Step 3: Write the implementation**

In `src/ingest_sessions/server.py`, add the import at the top:

```python
from ingest_sessions.dag import (
    assemble_context_for_session,
    run_summarize_session,
)
```

Add to `list_tools()`:

```python
        Tool(
            name="summarize",
            description=(
                "Trigger LCM summary DAG maintenance for a session. "
                "Summarizes unsummarized messages into sprigs, "
                "condenses sprigs into bindles. Uses claude --print "
                "(covered by Max). Returns counts of nodes created."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to summarize.",
                    },
                },
                "required": ["session_id"],
            },
        ),
        Tool(
            name="context",
            description=(
                "Assemble recovery context from the LCM summary DAG "
                "for a session. Returns the formatted context string "
                "suitable for injection after /clear."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to get context for.",
                    },
                },
                "required": ["session_id"],
            },
        ),
```

Add to `call_tool()`:

```python
    if name == "summarize":
        session_id = arguments["session_id"]
        try:
            result_data = await _db_execute(
                lambda db: run_summarize_session(db, session_id)
            )
            return [TextContent(type="text", text=json.dumps(result_data))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "context":
        session_id = arguments["session_id"]
        try:
            ctx = await _db_execute(
                lambda db: assemble_context_for_session(db, session_id)
            )
            return [TextContent(type="text", text=json.dumps({"context": ctx}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]
```

**Step 4: Run test to verify it passes**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_server.py::test_server_lists_summarize_tool tests/test_server.py::test_context_tool_returns_none_without_summaries -v`
Expected: PASS

**Step 5: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add src/ingest_sessions/server.py tests/test_server.py
git commit -m "[ingest-sessions] Wire summarize and context MCP tools"
```

---

### Task 5: Create the SessionStart hook script

This is the glue that makes `/clear` work. A shell script that:
1. Gets the current session ID
2. Calls the ingest_sessions `context` MCP tool
3. Returns `additionalContext` to Claude Code

**Files:**
- Create: `src/ingest_sessions/hooks/session_start.sh`
- Modify: Claude Code settings (documented, not automated)

**Step 1: Write the hook script**

Create `src/ingest_sessions/hooks/session_start.sh`:

```bash
#!/usr/bin/env bash
# LCM context recovery hook for Claude Code SessionStart.
#
# Called by Claude Code after /clear (and on session start).
# Queries the ingest_sessions MCP server for the summary DAG context
# and returns it as additionalContext for injection.
#
# Prerequisites:
#   - ingest-sessions-server running on localhost:8741
#   - curl available
#
# See docs/plans/2026-03-07-lcm-summary-dag.md

set -euo pipefail

# The session ID is passed via environment by Claude Code
SESSION_ID="${CLAUDE_SESSION_ID:-}"

if [ -z "$SESSION_ID" ]; then
    exit 0  # No session ID, nothing to do
fi

# Query the context tool via the MCP HTTP endpoint
RESPONSE=$(curl -s -X POST "http://127.0.0.1:8741/mcp" \
    -H "Content-Type: application/json" \
    -d "{
        \"jsonrpc\": \"2.0\",
        \"method\": \"tools/call\",
        \"params\": {
            \"name\": \"context\",
            \"arguments\": {\"session_id\": \"$SESSION_ID\"}
        },
        \"id\": 1
    }" 2>/dev/null || echo "")

if [ -z "$RESPONSE" ]; then
    exit 0  # Server not reachable, fail silently
fi

# Extract the context string from the response
CONTEXT=$(echo "$RESPONSE" | python3 -c "
import sys, json
try:
    resp = json.load(sys.stdin)
    content = resp.get('result', {}).get('content', [{}])
    if isinstance(content, list) and len(content) > 0:
        text = content[0].get('text', '{}')
        ctx = json.loads(text).get('context')
        if ctx:
            # Output the hook response with additionalContext
            print(json.dumps({
                'hookSpecificOutput': {
                    'hookEventName': 'SessionStart',
                    'additionalContext': ctx
                }
            }))
except Exception:
    pass
" 2>/dev/null || echo "")

if [ -n "$CONTEXT" ]; then
    echo "$CONTEXT"
fi
```

**Step 2: Make it executable**

```bash
chmod +x src/ingest_sessions/hooks/session_start.sh
```

**Step 3: Document the settings.json configuration**

The user adds this to `~/.claude/settings.json` under `"hooks"`:

```json
{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "/path/to/ingest_sessions/hooks/session_start.sh"
          }
        ]
      }
    ]
  }
}
```

**Step 4: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add src/ingest_sessions/hooks/session_start.sh
git commit -m "[ingest-sessions] Add SessionStart hook script for /clear context recovery"
```

---

### Task 6: E2E test — full DAG round-trip

Test the complete flow: seed messages → summarize → assemble context.

**Files:**
- Create: `tests/test_dag_e2e.py`

**Step 1: Write the test**

```python
"""E2E test for the DAG round-trip.

Seeds messages, runs summarize (with mock LLM), verifies context assembly.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.test_server import run_server, _text


def _make_records(session_id: str, count: int) -> list[dict]:
    """Generate count realistic JSONL records."""
    records = []
    for i in range(count):
        role = "user" if i % 2 == 0 else "assistant"
        records.append({
            "uuid": f"{session_id}-{i:04d}",
            "sessionId": session_id,
            "type": role,
            "timestamp": f"2026-03-01T{10 + i // 60:02d}:{i % 60:02d}:00.000Z",
            "parentUuid": None,
            "message": {
                "role": role,
                "content": f"{'User asks about' if role == 'user' else 'Assistant explains'} topic {i}. "
                           f"This involves file src/module_{i}.py and function process_{i}()."
            },
        })
    return records


@pytest.mark.asyncio
async def test_full_dag_roundtrip(tmp_path: Path):
    """Seed 50 messages, summarize, verify context assembly."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)

    session_id = "sess-dag-test"
    records = _make_records(session_id, 50)
    jsonl_path = proj_dir / f"{session_id}.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(r) for r in records) + "\n")

    with patch("ingest_sessions.dag.summarize_messages", return_value="Mock sprig summary of conversation segment."):
        with patch("ingest_sessions.dag.condense_summaries", return_value="Mock bindle: condensed overview of all work."):
            async with run_server(tmp_path) as (client, _):
                # Verify records were ingested
                result = await client.call_tool(
                    "query",
                    {"sql": f"SELECT count(*) AS n FROM records WHERE session_id = '{session_id}'"},
                )
                assert json.loads(_text(result)) == [{"n": 50}]

                # Trigger summarization
                result = await client.call_tool(
                    "summarize", {"session_id": session_id}
                )
                body = json.loads(_text(result))
                assert "error" not in body
                assert body["sprigs_created"] >= 1

                # If enough sprigs were created, a second pass should create bindles
                # (depends on SPRIG_CHUNK_SIZE and BINDLE_THRESHOLD)
                result = await client.call_tool(
                    "summarize", {"session_id": session_id}
                )

                # Verify context assembly
                result = await client.call_tool(
                    "context", {"session_id": session_id}
                )
                body = json.loads(_text(result))
                ctx = body.get("context")
                assert ctx is not None
                assert "continued after a /clear" in ctx
                assert "Summary ID:" in ctx


@pytest.mark.asyncio
async def test_context_empty_session(tmp_path: Path):
    """Context for a session with no summaries returns None."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "context", {"session_id": "nonexistent"}
        )
        body = json.loads(_text(result))
        assert body["context"] is None
```

**Step 2: Run tests**

Run: `cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_dag_e2e.py -v`
Expected: PASS

**Step 3: Commit**

```bash
cd ../ingest_sessions/ingest_sessions
git add tests/test_dag_e2e.py
git commit -m "[ingest-sessions] Add E2E test for full DAG round-trip"
```

---

### Task 7: Run full test suite and verify

**Step 1: Run all tests**

```bash
cd ../ingest_sessions/ingest_sessions && uv run pytest tests/ -v
```

Expected: All tests pass, including existing tests (no regressions).

**Step 2: Verify schema resource includes new table**

```bash
cd ../ingest_sessions/ingest_sessions && uv run pytest tests/test_server.py::test_schema_resource -v
```

The schema resource should now include the `summaries` table.

**Step 3: Commit any fixes**

If any test needed adjustment, commit the fix.

---

## Post-Implementation Notes

### How to use it

1. Start the ingest_sessions server: `ingest-sessions-server`
2. Work normally in Claude Code — messages flow into DuckDB automatically
3. Periodically (or via a cron/hook), call the `summarize` MCP tool for the active session
4. After `/clear`, the SessionStart hook injects the DAG context automatically

### What to verify manually

- The `SessionStart` hook actually fires after `/clear` and returns `additionalContext`
- `CLAUDE_SESSION_ID` is actually set in the hook environment (needs verification against the bundle — may need to extract from a different env var or the hook input JSON)
- The `claude --print` summarization calls produce reasonable output at the target token length
- The MCP HTTP endpoint format matches what `curl` expects (the streamable-HTTP transport may need different request framing)

### Future work (not in this plan)

- **Auto-summarize on watchdog events**: Instead of manual `summarize` calls, trigger DAG maintenance when the watchdog detects enough new messages
- **PreCompact hook**: Trigger summarization before Claude Code compacts, ensuring the DAG is current
- **Smarter chunking**: Use token counts rather than message counts for sprig boundaries
- **d3+ condensation**: Recursive bindle condensation for very long sessions (add upward/condense/d3 prompt)

---

Plan complete and saved to `docs/plans/2026-03-07-lcm-summary-dag.md`. Two execution options:

**1. Subagent-Driven (this session)** — I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** — Open new session in the ingest_sessions repo with executing-plans, batch execution with checkpoints

Which approach?
