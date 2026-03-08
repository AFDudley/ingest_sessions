"""DAG maintenance for LCM summary hierarchy.

Coordinates the creation and condensation of summary nodes:
- Chunks unsummarized messages into sprigs (d1)
- Condenses accumulated sprigs into bindles (d2)

The public API is split into DB-only functions (safe for the DB thread)
and orchestration functions that call the LLM (must NOT run in the DB
thread).  The server is responsible for calling them in the right context.

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import sys
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


# ---------------------------------------------------------------------------
# DB-only operations (safe for the DB thread)
# ---------------------------------------------------------------------------


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
) -> str:
    """Insert a sprig (d1) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)
    file_ids = extract_file_ids(content)
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
) -> str:
    """Insert a bindle (d2) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)

    # Collect file_ids from all parent summaries
    all_file_ids: list[str] = []
    for pid in parent_ids:
        row = db.execute(
            "SELECT file_ids FROM summaries WHERE summary_id = ?", [pid]
        ).fetchone()
        if row and row[0]:
            all_file_ids.extend(row[0])
    # Also extract from bindle content itself
    all_file_ids.extend(extract_file_ids(content))
    file_ids = sorted(set(all_file_ids))

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


def get_latest_summarized_session(
    db: duckdb.DuckDBPyConnection,
    project_dir: str,
) -> str | None:
    """Find the most recent session with summaries in a project directory.

    After /clear, Claude creates a new session ID. We need the previous
    session's context. This finds it by matching the project_path prefix
    from the sessions table against sessions that have summaries.

    Returns the session_id, or None if no summarized sessions exist.
    """
    row = db.execute(
        """
        SELECT s.session_id
        FROM sessions s
        INNER JOIN summaries sm ON s.session_id = sm.session_id
        WHERE s.project_path LIKE ? || '%'
           OR s.session_id IN (
               SELECT DISTINCT r.session_id FROM records r
               WHERE r.uuid IN (
                   SELECT f.file_path FROM file_mtimes f
                   WHERE f.file_path LIKE ? || '%'
               )
           )
        ORDER BY sm.created_at DESC
        LIMIT 1
        """,
        [project_dir, project_dir],
    ).fetchone()
    if row is None:
        # Fallback: find any session whose JSONL file is in this directory
        # by matching session_id against filenames in the dir
        row = db.execute(
            """
            SELECT s.session_id
            FROM summaries s
            INNER JOIN file_mtimes f ON f.file_path LIKE '%/' || s.session_id || '.jsonl'
            WHERE f.file_path LIKE ? || '%'
            ORDER BY s.created_at DESC
            LIMIT 1
            """,
            [project_dir],
        ).fetchone()
    return row[0] if row else None


def get_latest_bindle_content(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> str | None:
    """Get the content of the most recent bindle, or None."""
    row = db.execute(
        """
        SELECT content FROM summaries
        WHERE session_id = ? AND kind = 'bindle'
        ORDER BY created_at DESC LIMIT 1
        """,
        [session_id],
    ).fetchone()
    return row[0] if row else None


def assemble_context_for_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> str | None:
    """Assemble recovery context for injection after /clear.

    Returns a formatted string combining:
    1. The highest-order summary (bindle if available)
    2. Any uncondensed sprigs (more recent than the bindle)
    3. Unsummarized tail messages (not yet chunked into sprigs)
    4. Pointers to the query tool for deeper retrieval

    Returns None if no summaries and no messages exist for the session.
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

    # Get unsummarized tail messages
    tail_messages = get_unsummarized_messages(db, session_id)

    if not bindles and not uncondensed and not tail_messages:
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
        # Most recent bindle (usually just one)
        parts.append(f"[Summary ID: {bindles[0][0]}]")
        parts.append(bindles[0][1])
        parts.append("")

    if uncondensed:
        parts.append("## Recent Activity (not yet condensed)")
        parts.append("")
        for s in uncondensed:
            parts.append(f"[Summary ID: {s['summary_id']}]")
            parts.append(s["content"])
            parts.append("")

    if tail_messages:
        parts.append("## Unsummarized Tail (most recent messages)")
        parts.append("")
        for msg in tail_messages:
            role = msg.get("type", "unknown")
            uuid = msg.get("uuid", "?")
            parts.append(f"[{role} {uuid}]")
            try:
                record = __import__("json").loads(msg.get("raw", "{}"))
                content = record.get("message", {}).get("content", "")
                if isinstance(content, str):
                    if len(content) > 500:
                        content = content[:500] + "..."
                    parts.append(content)
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text = block.get("text", "")
                            if len(text) > 500:
                                text = text[:500] + "..."
                            parts.append(text)
            except Exception:
                parts.append("[could not extract content]")
            parts.append("")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Orchestration (calls LLM — must NOT run in the DB thread)
# ---------------------------------------------------------------------------


def run_summarize_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> dict[str, int]:
    """Run one pass of DAG maintenance for a session.

    1. Chunk unsummarized messages into sprigs (SPRIG_CHUNK_SIZE each)
    2. If enough uncondensed sprigs exist, condense into a bindle

    This function calls the LLM and therefore MUST NOT run inside the
    DB thread.  The server must run it via asyncio.to_thread or similar.

    Raises SummarizationError if the LLM fails to compress.

    Returns counts of what was created.
    """
    unsummarized = get_unsummarized_messages(db, session_id)
    sprigs_created = 0
    bindles_created = 0

    # Get the most recent sprig for narrative continuity
    existing_sprigs = get_sprigs_for_session(db, session_id)
    previous_context = existing_sprigs[-1]["content"] if existing_sprigs else None

    def _log(msg: str) -> None:
        print(f"[dag] {session_id}: {msg}", file=sys.stderr)

    _log(f"unsummarized={len(unsummarized)} existing_sprigs={len(existing_sprigs)}")

    # Create sprigs from full chunks only (remainder stays for next run)
    full_chunks = len(unsummarized) // SPRIG_CHUNK_SIZE
    for i in range(full_chunks):
        chunk = unsummarized[i * SPRIG_CHUNK_SIZE : (i + 1) * SPRIG_CHUNK_SIZE]
        content = summarize_messages(chunk, previous_summary_context=previous_context)
        uuids = [r["uuid"] for r in chunk]
        insert_sprig(db, session_id, content, uuids)
        previous_context = content
        sprigs_created += 1

    # Condense uncondensed sprigs into a bindle if threshold met
    uncondensed = get_sprigs_for_session(db, session_id, uncondensed_only=True)
    _log(f"uncondensed={len(uncondensed)} threshold={BINDLE_THRESHOLD}")
    if len(uncondensed) >= BINDLE_THRESHOLD:
        bindle_context = get_latest_bindle_content(db, session_id)
        _log(f"condensing {len(uncondensed)} sprigs into bindle")
        content = condense_summaries(
            uncondensed, previous_summary_context=bindle_context
        )
        parent_ids = [s["summary_id"] for s in uncondensed]
        insert_bindle(db, session_id, content, parent_ids)
        bindles_created += 1
        _log("bindle created")

    return {
        "sprigs_created": sprigs_created,
        "bindles_created": bindles_created,
        "unsummarized_remaining": len(unsummarized) - (full_chunks * SPRIG_CHUNK_SIZE),
    }
