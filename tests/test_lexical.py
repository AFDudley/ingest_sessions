"""Tests for lexical (BM25) candidate search + RRF fusion (is-565.2b).

These exercise REAL DuckDB ``fts`` BM25 (no mock) for the lexical arm, and
the pure reciprocal-rank-fusion combiner. ``test_retrieve_candidates_*`` is
the slice e2e: a query that is a semantic match for one record and a lexical
match for another must surface BOTH in the fused stage-1 candidate set —
real embeddings model, real fts index, no mocks in the path.
"""

from __future__ import annotations

import json

import duckdb

from ingest_sessions.core import rebuild_fts_index
from ingest_sessions.embeddings import backfill_embeddings
from ingest_sessions.retrieval import (
    reciprocal_rank_fusion,
    retrieve_candidates,
    search_lexical,
)


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
# reciprocal_rank_fusion — pure, hand-worked
# ---------------------------------------------------------------------------


def test_rrf_high_in_both_beats_high_in_one() -> None:
    # "b" is rank 1 in list two and rank 2 in list one; "a" is rank 1 in
    # list one but absent from list two. With k_const=60:
    #   a = 1/(60+1)                 = 0.016393
    #   b = 1/(60+2) + 1/(60+1)      = 0.032520
    # so b (high in both) must beat a (high in only one).
    fused = reciprocal_rank_fusion([["a", "b", "c"], ["b", "x", "y"]])
    assert fused[0][0] == "b"
    ids = [i for i, _ in fused]
    assert ids.index("b") < ids.index("a")
    # scores are sorted descending.
    scores = [s for _, s in fused]
    assert scores == sorted(scores, reverse=True)


def test_rrf_score_values_match_formula() -> None:
    fused = dict(reciprocal_rank_fusion([["a", "b"], ["b"]], k_const=60))
    assert abs(fused["a"] - (1 / 61)) < 1e-12
    assert abs(fused["b"] - (1 / 62 + 1 / 61)) < 1e-12


def test_rrf_k_const_changes_weighting() -> None:
    # A larger k_const flattens the rank advantage (scores shrink and
    # converge); a smaller k_const sharpens it.
    big = dict(reciprocal_rank_fusion([["a", "b"]], k_const=1000))
    small = dict(reciprocal_rank_fusion([["a", "b"]], k_const=1))
    assert small["a"] - small["b"] > big["a"] - big["b"]


def test_rrf_empty_input() -> None:
    assert reciprocal_rank_fusion([]) == []
    assert reciprocal_rank_fusion([[], []]) == []


# ---------------------------------------------------------------------------
# search_lexical — real DuckDB fts BM25
# ---------------------------------------------------------------------------


def test_search_lexical_empty_corpus(db: duckdb.DuckDBPyConnection) -> None:
    # No records, no index built yet: must return [] rather than throw.
    assert search_lexical(db, "anything") == []


def test_search_lexical_rare_keyword_top_hit(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        _user_record("a", "s1", "the quick brown fox jumps over the lazy dog"),
        _user_record("b", "s2", "configuring the zorblax cache eviction parameter"),
        _user_record("c", "s3", "a treatise on baking sourdough bread at home"),
    )
    rebuild_fts_index(db)

    hits = search_lexical(db, "zorblax", k=5)
    assert hits, "expected a lexical hit for the rare keyword"
    assert hits[0]["uuid"] == "b"
    assert hits[0]["session_id"] == "s2"
    assert hits[0]["score"] > 0
    assert isinstance(hits[0]["raw"], dict)
    # scores sorted descending (best first).
    scores = [h["score"] for h in hits]
    assert scores == sorted(scores, reverse=True)


def test_search_lexical_no_match(db: duckdb.DuckDBPyConnection) -> None:
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    rebuild_fts_index(db)
    assert search_lexical(db, "elephant") == []


# ---------------------------------------------------------------------------
# retrieve_candidates — slice e2e (real embeddings + real fts)
# ---------------------------------------------------------------------------


def test_retrieve_candidates_fuses_both_arms(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        # Semantic match for the query (no shared rare token).
        _user_record(
            "sem", "s1", "How do I create a database index to speed up slow queries?"
        ),
        # Lexical match for the query via the rare token 'zorblax'.
        _user_record(
            "lex", "s2", "Set the zorblax flag to tune cache eviction behaviour."
        ),
        # Distractor unrelated to either.
        _user_record("noise", "s3", "What time does the bakery open on Sunday?"),
    )
    backfill_embeddings(db)
    rebuild_fts_index(db)

    cands = retrieve_candidates(
        db, "speeding up database queries and the zorblax setting", k=3
    )
    ids = {c["uuid"] for c in cands}
    assert "sem" in ids, "semantic arm should surface the index record"
    assert "lex" in ids, "lexical arm should surface the zorblax record"
    # Stage-1 output is a bounded candidate set.
    assert len(cands) <= 3
    # Each candidate carries the fused score and its raw payload.
    top = cands[0]
    assert "fused_score" in top
    assert isinstance(top["raw"], dict)
    assert top["session_id"] in {"s1", "s2", "s3"}


def test_retrieve_candidates_bounded_by_k(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        *[
            _user_record(f"u{i}", "s", f"record number {i} about indexing")
            for i in range(8)
        ],
    )
    backfill_embeddings(db)
    rebuild_fts_index(db)
    cands = retrieve_candidates(db, "indexing", k=3)
    assert len(cands) == 3
