"""Tests for the cross-encoder rerank stage + retrieve_relevant (is-565.2c).

These exercise the REAL fastembed ONNX cross-encoder (no model mock) — the
stage-2 head of the retrieve pipeline. ``test_rerank_*`` checks the pure(ish)
scorer; ``test_retrieve_relevant_*`` is the slice e2e: a tiny ingested corpus
with real embeddings + real fts, where a query must surface the most-relevant
record ranked first. No mocks in the path.
"""

from __future__ import annotations

import json

import duckdb

from ingest_sessions.core import rebuild_fts_index
from ingest_sessions.embeddings import backfill_embeddings
from ingest_sessions.rerank import RERANK_MODEL, rerank
from ingest_sessions.retrieval import retrieve_relevant


def _user_record(uuid: str, session_id: str, text: str) -> tuple:
    """Build a records row with a Claude-Code-shaped raw JSON."""
    raw = {
        "uuid": uuid,
        "sessionId": session_id,
        "type": "user",
        "message": {"role": "user", "content": text},
    }
    return (uuid, session_id, "user", None, None, json.dumps(raw))


def _insert(db: duckdb.DuckDBPyConnection, *rows: tuple) -> None:
    db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)", rows)


# ---------------------------------------------------------------------------
# rerank — real fastembed cross-encoder
# ---------------------------------------------------------------------------


def test_rerank_model_constant_is_set() -> None:
    assert isinstance(RERANK_MODEL, str)
    assert RERANK_MODEL


def test_rerank_returns_one_score_per_doc_order_aligned() -> None:
    docs = [
        "Paris is the capital of France.",
        "Bananas are a yellow tropical fruit.",
        "The Eiffel Tower stands in Paris, France.",
    ]
    scores = rerank("what is the capital of france", docs)
    assert len(scores) == len(docs)
    assert all(isinstance(s, float) for s in scores)


def test_rerank_relevant_scores_higher_than_irrelevant() -> None:
    docs = [
        "Paris is the capital of France.",
        "Bananas are a yellow tropical fruit.",
    ]
    scores = rerank("what is the capital of france", docs)
    assert scores[0] > scores[1]


def test_rerank_empty_documents() -> None:
    assert rerank("any query", []) == []


# ---------------------------------------------------------------------------
# retrieve_relevant — slice e2e (real embeddings + real fts + real reranker)
# ---------------------------------------------------------------------------


def test_retrieve_relevant_ranks_most_relevant_first(
    db: duckdb.DuckDBPyConnection,
) -> None:
    _insert(
        db,
        _user_record(
            "idx",
            "s1",
            "To speed up slow SQL queries, create a database index on the "
            "columns in the WHERE clause.",
        ),
        _user_record(
            "bake",
            "s2",
            "Knead the sourdough, let it proof overnight, then bake at 230C.",
        ),
        _user_record(
            "weather",
            "s3",
            "It will be sunny with a light breeze this afternoon.",
        ),
    )
    backfill_embeddings(db)
    rebuild_fts_index(db)

    results = retrieve_relevant(
        db, "how do I make my database queries run faster?", k=3
    )
    assert results, "expected ranked results"
    assert results[0]["uuid"] == "idx"
    top = results[0]
    assert "rerank_score" in top
    assert isinstance(top["raw"], dict)
    assert top["session_id"] == "s1"
    # scores sorted descending (best first).
    scores = [r["rerank_score"] for r in results]
    assert scores == sorted(scores, reverse=True)


def test_retrieve_relevant_bounded_by_k(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        *[
            _user_record(f"u{i}", "s", f"record number {i} about database indexing")
            for i in range(8)
        ],
    )
    backfill_embeddings(db)
    rebuild_fts_index(db)
    results = retrieve_relevant(db, "database indexing", k=3, candidate_k=8)
    assert len(results) == 3


def test_retrieve_relevant_empty_corpus(db: duckdb.DuckDBPyConnection) -> None:
    # No records, no indexes: must return [] gracefully (no rerank call).
    assert retrieve_relevant(db, "anything") == []
