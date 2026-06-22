"""Retrieval surface for ingest_sessions (pebble is-565 family).

This module is the on-device retrieval *brain*: the API the MCP server and
the Claude Code injection hook call to surface prior reasoning. It is kept
project-AGNOSTIC — no consumer's tracker / ADR semantics live here.

Today it provides the on-demand FETCH primitive (is-565.1):

``get_full_session(db, session_id)`` reassembles a complete transcript in
chronological order, rehydrating every ``[Large Content: file_...]`` marker
that ingestion offloaded to the blob store (see blobs.py) so a labbook
``session_id`` citation resolves to the actual content. The retrieve+rerank
pipeline (is-565.2) and supersession/trust-tier ranking (is-565.3) are the
siblings that will join it here.

# See .claude doctrine: functional core, imperative shell — pure helpers
# (marker parsing, transcript formatting) compute; disk reads happen at the
# get_full_session boundary.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.blobs import read_blob

# Mirrors _make_blob_marker() in core.py:
#   f"[Large Content: {file_id}] [Tokens: ~{token_count}]"
# file_id is "file_" + 16 hex chars (blobs.generate_blob_id).
BLOB_MARKER_RE = re.compile(r"\[Large Content: (file_[0-9a-f]{16})\] \[Tokens: ~\d+\]")


def find_blob_markers(text: str) -> list[str]:
    """Return the file_ids of every blob marker in *text* (pure)."""
    return BLOB_MARKER_RE.findall(text)


def _rehydrate_value(value: Any, blob_root: Path | None, missing: list[str]) -> Any:
    """Recursively replace blob markers in any JSON value with blob content.

    Walks strings, lists and dicts so the same logic covers every content
    shape ingestion can offload (plain string, text blocks, tool_results,
    nested tool_result text) without enumerating them. A marker whose blob
    is absent on disk is left in place and its file_id appended to *missing*
    — never silently blanked.
    """
    if isinstance(value, str):

        def _repl(match: re.Match[str]) -> str:
            file_id = match.group(1)
            content = read_blob(file_id, blob_root=blob_root)
            if content is None:
                missing.append(file_id)
                return match.group(0)
            return content

        return BLOB_MARKER_RE.sub(_repl, value)
    if isinstance(value, list):
        return [_rehydrate_value(v, blob_root, missing) for v in value]
    if isinstance(value, dict):
        return {k: _rehydrate_value(v, blob_root, missing) for k, v in value.items()}
    return value


def get_full_session(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    *,
    include_blobs: bool = True,
    blob_root: Path | None = None,
) -> dict[str, Any]:
    """Reassemble a full session by id (pebble is-565.1).

    Returns a dict::

        {
            "session_id": str,
            "record_count": int,
            "records": [
                {"uuid", "session_id", "type", "timestamp",
                 "parent_uuid", "raw": <parsed record, blobs rehydrated>},
                ...
            ],
            "missing_blobs": [file_id, ...],  # referenced but absent on disk
        }

    Records are ordered chronologically (timestamp, NULLs last; uuid as a
    deterministic tiebreak). When *include_blobs* is True, ``[Large Content:
    file_...]`` markers are replaced with the original content from the blob
    store; otherwise the compact markers are returned as stored.
    """
    rows = db.execute(
        "SELECT uuid, session_id, type, timestamp, parent_uuid, raw "
        "FROM records WHERE session_id = ? "
        "ORDER BY timestamp NULLS LAST, uuid",
        [session_id],
    ).fetchall()

    missing: list[str] = []
    records: list[dict[str, Any]] = []
    for uuid, sid, rtype, timestamp, parent_uuid, raw in rows:
        obj = json.loads(raw)
        if include_blobs:
            obj = _rehydrate_value(obj, blob_root, missing)
        records.append(
            {
                "uuid": uuid,
                "session_id": sid,
                "type": rtype,
                "timestamp": str(timestamp) if timestamp is not None else None,
                "parent_uuid": parent_uuid,
                "raw": obj,
            }
        )

    return {
        "session_id": session_id,
        "record_count": len(records),
        "records": records,
        "missing_blobs": sorted(set(missing)),
    }


def _content_to_text(content: Any) -> str:
    """Flatten a message ``content`` field to plain text (pure)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            if "text" in block and isinstance(block["text"], str):
                parts.append(block["text"])
            elif block.get("type") == "tool_result":
                inner = block.get("content")
                if isinstance(inner, str):
                    parts.append(inner)
                else:
                    parts.append(_content_to_text(inner))
            elif block.get("type") == "tool_use":
                parts.append(f"[tool_use: {block.get('name', '?')}]")
        return "\n".join(parts)
    return ""


def format_session_text(session: dict[str, Any]) -> str:
    """Render a get_full_session() result as a human-readable transcript (pure).

    Stable, chronological plaintext suitable for citation display — each
    record rendered as ``<role/type>: <content>``.
    """
    lines: list[str] = [f"# Session {session['session_id']}"]
    for rec in session["records"]:
        raw = rec.get("raw")
        role = rec.get("type", "?")
        text = ""
        if isinstance(raw, dict):
            msg = raw.get("message")
            if isinstance(msg, dict):
                role = msg.get("role", role)
                text = _content_to_text(msg.get("content"))
        lines.append(f"\n## {role}\n{text}")
    return "\n".join(lines)
