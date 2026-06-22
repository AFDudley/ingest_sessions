"""Tests for ranking composition (pebble is-565.3) — the differentiator.

Two layers:
  * ``compose_score`` / ``rank_candidates`` / ``default_confidence`` — the PURE
    scoring kernel and its pure orchestration (data-in/data-out, injected
    ``now_ms`` for determinism). Trust-tier is exercised here with synthetic
    mixed-tier candidates, because the live retrieve pipeline only indexes the
    primary ``records`` tier today (see the seam note in ranking.py).
  * The **build-then-revert arc** — the headline acceptance — runs the REAL
    retrieve_relevant pipeline (real embeddings + real fts + real cross-encoder)
    over a tiny corpus with a supersession link, and asserts the superseding
    conclusion outranks the reverted plan AND the reverted plan is flagged
    superseded, even when its raw rerank relevance is the higher of the two.
"""

from __future__ import annotations

import json

import duckdb

from ingest_sessions.core import rebuild_fts_index
from ingest_sessions.embeddings import backfill_embeddings
from ingest_sessions.ranking import (
    compose_score,
    default_confidence,
    rank_candidates,
)
from ingest_sessions.retrieval import retrieve_relevant
from ingest_sessions.supersession import add_supersession

# A fixed reference clock (2026-06-01T00:00:00Z in epoch ms) for deterministic
# recency math.
NOW_MS = 1_780_000_000_000
DAY_S = 86_400.0


# ---------------------------------------------------------------------------
# compose_score — the pure scoring kernel
# ---------------------------------------------------------------------------


def test_compose_score_fresh_primary_is_relevance_times_confidence() -> None:
    # age 0 → recency_decay 1.0; primary tier, not superseded → factors 1.0.
    score = compose_score(
        relevance=0.8,
        age_seconds=0.0,
        confidence=1.0,
        superseded=False,
        tier="primary",
    )
    assert score == 0.8


def test_compose_score_recency_half_life() -> None:
    # At exactly one half-life of age the score halves.
    half_life = 30 * DAY_S
    fresh = compose_score(
        relevance=1.0,
        age_seconds=0.0,
        confidence=1.0,
        superseded=False,
        tier="primary",
        half_life_seconds=half_life,
    )
    aged = compose_score(
        relevance=1.0,
        age_seconds=half_life,
        confidence=1.0,
        superseded=False,
        tier="primary",
        half_life_seconds=half_life,
    )
    assert aged == fresh * 0.5
    # Older still → lower still.
    older = compose_score(
        relevance=1.0,
        age_seconds=2 * half_life,
        confidence=1.0,
        superseded=False,
        tier="primary",
        half_life_seconds=half_life,
    )
    assert older < aged


def test_compose_score_superseded_down_weights() -> None:
    base = compose_score(
        relevance=1.0,
        age_seconds=0.0,
        confidence=1.0,
        superseded=False,
        tier="primary",
        superseded_penalty=0.05,
    )
    demoted = compose_score(
        relevance=1.0,
        age_seconds=0.0,
        confidence=1.0,
        superseded=True,
        tier="primary",
        superseded_penalty=0.05,
    )
    assert demoted == base * 0.05
    assert demoted > 0.0  # down-weighted, not deleted — still retrievable.


def test_compose_score_summary_tier_down_weights() -> None:
    primary = compose_score(
        relevance=1.0,
        age_seconds=0.0,
        confidence=1.0,
        superseded=False,
        tier="primary",
        summary_tier_penalty=0.5,
    )
    summary = compose_score(
        relevance=1.0,
        age_seconds=0.0,
        confidence=1.0,
        superseded=False,
        tier="summary",
        summary_tier_penalty=0.5,
    )
    assert summary == primary * 0.5
    assert summary < primary


def test_compose_score_factors_compound() -> None:
    half_life = 10 * DAY_S
    score = compose_score(
        relevance=0.9,
        age_seconds=10 * DAY_S,
        confidence=0.5,
        superseded=True,
        tier="summary",
        half_life_seconds=half_life,
        superseded_penalty=0.05,
        summary_tier_penalty=0.5,
    )
    expected = 0.9 * 0.5 * 0.5 * 0.05 * 0.5
    assert abs(score - expected) < 1e-12


# ---------------------------------------------------------------------------
# default_confidence — the documented heuristic
# ---------------------------------------------------------------------------


def test_default_confidence_substantive_message_is_full() -> None:
    raw = {"type": "assistant", "message": {"role": "assistant", "content": "x"}}
    assert default_confidence(raw) == 1.0


def test_default_confidence_noise_type_is_reduced() -> None:
    raw = {"type": "progress", "message": {"role": "assistant", "content": "x"}}
    assert default_confidence(raw) < 1.0


# ---------------------------------------------------------------------------
# rank_candidates — pure orchestration (trust-tier floor + determinism)
# ---------------------------------------------------------------------------


def _candidate(uuid: str, rerank_score: float, **extra: object) -> dict:
    raw = {"type": "assistant", "message": {"role": "assistant", "content": uuid}}
    return {"uuid": uuid, "rerank_score": rerank_score, "raw": raw, **extra}


def test_rank_candidates_trust_tier_floor() -> None:
    # Equal relevance/recency/confidence: the summary-tier candidate must rank
    # BELOW the primary one — the "summary never outranks a primary" floor.
    cands = [
        _candidate("sum", 2.0, tier="summary"),
        _candidate("prim", 2.0, tier="primary"),
    ]
    ranked = rank_candidates(cands, superseded_ids=set(), now_ms=NOW_MS)
    order = [c["uuid"] for c in ranked]
    assert order == ["prim", "sum"]
    summary_hit = next(c for c in ranked if c["uuid"] == "sum")
    assert (
        summary_hit["final_score"]
        < next(c for c in ranked if c["uuid"] == "prim")["final_score"]
    )


def test_rank_candidates_superseded_flagged_and_demoted() -> None:
    cands = [
        _candidate("old", 5.0),  # higher raw relevance, but superseded
        _candidate("new", 1.0),
    ]
    ranked = rank_candidates(cands, superseded_ids={"old"}, now_ms=NOW_MS)
    order = [c["uuid"] for c in ranked]
    assert order == ["new", "old"]
    old_hit = next(c for c in ranked if c["uuid"] == "old")
    assert old_hit["superseded"] is True
    new_hit = next(c for c in ranked if c["uuid"] == "new")
    assert new_hit["superseded"] is False


def test_rank_candidates_attaches_transparency_fields() -> None:
    ranked = rank_candidates(
        [_candidate("a", 1.0)], superseded_ids=set(), now_ms=NOW_MS
    )
    hit = ranked[0]
    assert "final_score" in hit
    assert "superseded" in hit
    assert "recency" in hit
    # original fields preserved.
    assert hit["uuid"] == "a"
    assert hit["rerank_score"] == 1.0


def test_rank_candidates_deterministic_with_injected_now() -> None:
    cands = [_candidate("a", 1.0), _candidate("b", 2.0)]
    first = rank_candidates(cands, superseded_ids=set(), now_ms=NOW_MS)
    second = rank_candidates(cands, superseded_ids=set(), now_ms=NOW_MS)
    assert [c["final_score"] for c in first] == [c["final_score"] for c in second]


def test_rank_candidates_recency_orders_recent_first() -> None:
    # Two equal-relevance candidates with explicit timestamps; the more recent
    # one ranks first.
    def ts_candidate(uuid: str, iso: str) -> dict:
        raw = {
            "type": "assistant",
            "timestamp": iso,
            "message": {"role": "assistant", "content": uuid},
        }
        return {"uuid": uuid, "rerank_score": 2.0, "raw": raw}

    cands = [
        ts_candidate("older", "2026-01-01T00:00:00.000Z"),
        ts_candidate("newer", "2026-05-01T00:00:00.000Z"),
    ]
    ranked = rank_candidates(cands, superseded_ids=set(), now_ms=NOW_MS)
    assert [c["uuid"] for c in ranked] == ["newer", "older"]


# ---------------------------------------------------------------------------
# build-then-revert arc — headline acceptance (real retrieve pipeline)
# ---------------------------------------------------------------------------


def _user_record(uuid: str, session_id: str, text: str) -> tuple:
    raw = {
        "uuid": uuid,
        "sessionId": session_id,
        "type": "user",
        "message": {"role": "user", "content": text},
    }
    return (uuid, session_id, "user", None, None, json.dumps(raw))


def test_retrieve_relevant_supersession_flips_and_flags(
    db: duckdb.DuckDBPyConnection,
) -> None:
    # Record B: the chosen-then-reverted plan. Record A: the superseding
    # conclusion. Both are about the same decision and both relevant to the
    # query — B is phrased to be at least as raw-relevant as A.
    _b = (
        "Decision: we will use polling to detect file changes because it is the "
        "simplest approach for change detection in the watcher."
    )
    _a = (
        "Reverting the polling decision: we now use inotify file watching for "
        "change detection in the watcher — polling was too slow."
    )
    db.executemany(
        "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
        [
            _user_record("B", "s1", _b),
            _user_record("A", "s1", _a),
        ],
    )
    backfill_embeddings(db)
    rebuild_fts_index(db)

    # A supersedes B (e.g. a git-revert adapter ingested this link).
    add_supersession(db, "A", "B", source="git-revert")

    results = retrieve_relevant(
        db, "what approach do we use for file change detection?", k=5
    )
    uuids = [r["uuid"] for r in results]
    assert "A" in uuids and "B" in uuids
    assert uuids.index("A") < uuids.index("B"), (
        "superseding conclusion A must outrank reverted plan B"
    )
    b_hit = next(r for r in results if r["uuid"] == "B")
    a_hit = next(r for r in results if r["uuid"] == "A")
    assert b_hit["superseded"] is True
    assert a_hit["superseded"] is False
    assert a_hit["final_score"] > b_hit["final_score"]
