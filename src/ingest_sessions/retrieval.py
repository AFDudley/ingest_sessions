"""Retrieval surface for ingest_sessions (pebble is-565 family).

This module is the on-device retrieval *brain*: the API the MCP server and
the Claude Code injection hook call to surface prior reasoning. It is kept
project-AGNOSTIC — no consumer's tracker / ADR semantics live here.

It provides the on-demand FETCH primitive (is-565.1) and the lexical arm +
candidate fusion of the retrieve pipeline (is-565.2b):

``get_full_session(db, session_id)`` reassembles a complete transcript in
chronological order, rehydrating every ``[Large Content: file_...]`` marker
that ingestion offloaded to the blob store (see blobs.py) so a labbook
``session_id`` citation resolves to the actual content.

``search_lexical`` is the BM25 arm (DuckDB ``fts``, no model), the lexical
counterpart to ``embeddings.search_vector``. ``reciprocal_rank_fusion`` is a
pure RRF combiner, and ``retrieve_candidates`` fuses the vector + lexical
arms into the is-565.2 STAGE-1 bounded candidate set (the cross-encoder
rerank in is-565.2c reranks these). Supersession/trust-tier ranking
(is-565.3) is the sibling that will join it here.

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


def search_lexical(
    db: duckdb.DuckDBPyConnection, query: str, k: int = 20
) -> list[dict[str, Any]]:
    """Return the ``k`` best BM25 matches for *query* (lexical, no model).

    Scores the ``record_fts`` projection with DuckDB's ``match_bm25`` (higher
    score = better match), joining ``records`` for the parsed ``raw`` payload.
    Each hit is ``{uuid, session_id, score, raw}``.

    The FTS index is a snapshot built by ``core.rebuild_fts_index``; before it
    has ever been built the ``fts_main_record_fts`` schema does not exist, so
    this returns ``[]`` rather than raising — the empty-corpus / no-index case.
    A query that matches no document also yields ``[]`` (``match_bm25`` returns
    NULL, filtered out).
    """
    index_exists = db.execute(
        "SELECT 1 FROM information_schema.schemata "
        "WHERE schema_name = 'fts_main_record_fts'"
    ).fetchone()
    if not index_exists:
        return []
    rows = db.execute(
        "SELECT f.uuid, r.session_id, r.raw, "
        "fts_main_record_fts.match_bm25(f.uuid, ?) AS score "
        "FROM record_fts f JOIN records r ON f.uuid = r.uuid "
        "WHERE score IS NOT NULL ORDER BY score DESC LIMIT ?",
        [query, k],
    ).fetchall()
    return [
        {
            "uuid": uuid,
            "session_id": session_id,
            "score": score,
            "raw": json.loads(raw),
        }
        for uuid, session_id, raw, score in rows
    ]


def reciprocal_rank_fusion(
    rankings: list[list[str]], *, k_const: int = 60
) -> list[tuple[str, float]]:
    """Fuse multiple ranked id-lists into one ranking via RRF (pure).

    Classic Reciprocal Rank Fusion (Cormack et al. 2009): each id's fused
    score is ``Σ_i 1 / (k_const + rank_i)`` summed over every input list that
    contains it, where ``rank_i`` is the id's 1-based position in list ``i``.
    An id ranked highly in several lists thus beats one ranked highly in only
    one. Returns ``(id, fused_score)`` pairs sorted by score descending; ties
    break by first appearance across the input lists (stable). Empty input —
    no lists, or only empty lists — yields ``[]``.
    """
    scores: dict[str, float] = {}
    order: dict[str, int] = {}
    seq = 0
    for ranking in rankings:
        for rank, item in enumerate(ranking, start=1):
            scores[item] = scores.get(item, 0.0) + 1.0 / (k_const + rank)
            if item not in order:
                order[item] = seq
                seq += 1
    return sorted(scores.items(), key=lambda kv: (-kv[1], order[kv[0]]))


def retrieve_candidates(
    db: duckdb.DuckDBPyConnection, query: str, k: int = 20
) -> list[dict[str, Any]]:
    """Stage-1 candidate retrieval: fuse vector + lexical arms via RRF.

    Runs the vector arm (``embeddings.search_vector``, cosine distance) and the
    lexical arm (``search_lexical``, BM25), fuses their rank orders with
    ``reciprocal_rank_fusion``, and returns the top-``k`` fused candidates::

        {uuid, session_id, raw, fused_score,
         vector_distance?, lexical_score?}

    ``vector_distance`` / ``lexical_score`` are present only for the arm(s)
    that surfaced the candidate. This is the is-565.2 STAGE-1 output — a
    BOUNDED candidate set (never "all"), reranked by the cross-encoder in
    is-565.2c. Each arm contributes up to ``k`` candidates before fusion.
    """
    from ingest_sessions.embeddings import search_vector

    vec_hits = search_vector(db, query, k=k)
    lex_hits = search_lexical(db, query, k=k)

    by_uuid: dict[str, dict[str, Any]] = {}
    for h in vec_hits:
        by_uuid[h["uuid"]] = {
            "uuid": h["uuid"],
            "session_id": h["session_id"],
            "raw": h["raw"],
            "vector_distance": h["distance"],
        }
    for h in lex_hits:
        entry = by_uuid.setdefault(
            h["uuid"],
            {"uuid": h["uuid"], "session_id": h["session_id"], "raw": h["raw"]},
        )
        entry["lexical_score"] = h["score"]

    fused = reciprocal_rank_fusion(
        [[h["uuid"] for h in vec_hits], [h["uuid"] for h in lex_hits]]
    )
    candidates: list[dict[str, Any]] = []
    for uuid, score in fused[:k]:
        entry = dict(by_uuid[uuid])
        entry["fused_score"] = score
        candidates.append(entry)
    return candidates


def retrieve_relevant(
    db: duckdb.DuckDBPyConnection,
    query: str,
    *,
    k: int = 10,
    candidate_k: int = 40,
) -> list[dict[str, Any]]:
    """Stage-2 relevance retrieval: rerank stage-1 candidates by cross-encoder.

    The head of the is-565.2 retrieve pipeline. Pulls a BOUNDED stage-1
    candidate set via ``retrieve_candidates`` (vector + lexical fused),
    flattens each candidate to its rerank document text with the same
    ``embeddings.record_text`` used everywhere, scores them with the ONNX
    cross-encoder (``rerank.rerank``), attaches ``rerank_score``, and returns
    the top-``k`` sorted by relevance descending::

        {uuid, session_id, raw, fused_score, rerank_score,
         vector_distance?, lexical_score?}

    ``uuid`` / ``session_id`` are the integration point — a caller deepens any
    hit via ``get_full_session``. With zero candidates this returns ``[]``
    without invoking the model. This slice is PURE relevance rerank; the
    is-565.3 ranking composition (recency / confidence / supersession /
    trust-tier) is the sibling that joins it here.
    """
    from ingest_sessions import embeddings, rerank as rerank_mod

    candidates = retrieve_candidates(db, query, k=candidate_k)
    if not candidates:
        return []

    docs = [embeddings.record_text(c["raw"]) for c in candidates]
    scores = rerank_mod.rerank(query, docs)

    ranked = [
        {**candidate, "rerank_score": score}
        for candidate, score in zip(candidates, scores, strict=True)
    ]
    ranked.sort(key=lambda c: c["rerank_score"], reverse=True)
    return ranked[:k]


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
