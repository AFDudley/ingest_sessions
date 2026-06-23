"""Trust-tier END-TO-END tests (is-565.4) — the summaries auto-tier as a
retrieval candidate source.

The trust-tier floor lives in ``ranking.compose_score`` (a ``tier != 'primary'``
candidate keeps only ``summary_tier_penalty`` of its score, so a derived summary
cannot outrank a primary-source record of EQUAL raw relevance). Before this
slice that floor was exercised only by ``test_ranking`` with SYNTHETIC mixed-tier
candidates — the live ``retrieve_relevant`` pipeline emitted primary records
only, so the floor never fired end-to-end. These tests use REAL models
(fastembed ONNX + the cross-encoder reranker, no mocks) over a real DuckDB to
prove the floor now fires through the actual pipeline.
"""

from __future__ import annotations

import json

import duckdb

from ingest_sessions.dag import insert_sprig
from ingest_sessions.embeddings import (
    backfill_embeddings,
    backfill_summary_embeddings,
    search_summaries,
    summary_text,
)
from ingest_sessions.core import rebuild_fts_index
from ingest_sessions.retrieval import retrieve_relevant


def _user_record(uuid: str, session_id: str, text: str) -> tuple:
    """A records row with a Claude-Code-shaped raw JSON and NO timestamp.

    Omitting ``timestamp`` makes recency neutral (age 0 → decay 1.0) for the
    record, so the floor tests isolate the TRUST-TIER factor from recency.
    """
    raw = {
        "uuid": uuid,
        "sessionId": session_id,
        "type": "user",
        "message": {"role": "user", "content": text},
    }
    return (uuid, session_id, "user", None, None, json.dumps(raw))


def _insert_records(db: duckdb.DuckDBPyConnection, *rows: tuple) -> None:
    db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)", rows)


# ---------------------------------------------------------------------------
# Pure helper + summary embedding/search
# ---------------------------------------------------------------------------


def test_summary_text_is_verbatim_content() -> None:
    assert summary_text({"content": "the session decided X"}) == "the session decided X"
    assert summary_text({"content": "", "kind": "sprig"}) == ""
    assert summary_text({"kind": "bindle"}) == ""


def test_backfill_summary_embeddings_idempotent_and_incremental(
    db: duckdb.DuckDBPyConnection,
) -> None:
    insert_sprig(db, "s1", "indexing strategy for postgres queries", ["u1"])
    insert_sprig(db, "s1", "vector search with HNSW cosine distance", ["u2"])

    first = backfill_summary_embeddings(db)
    assert first == 2
    n = db.execute("SELECT count(*) FROM summary_embeddings").fetchone()
    assert n is not None and n[0] == 2

    # Idempotent: a second pass over an unchanged corpus embeds nothing.
    assert backfill_summary_embeddings(db) == 0

    # Incremental: a newly-created summary is picked up on the next pass.
    insert_sprig(db, "s2", "cooking risotto slowly", ["u3"])
    assert backfill_summary_embeddings(db) == 1
    n = db.execute("SELECT count(*) FROM summary_embeddings").fetchone()
    assert n is not None and n[0] == 3


def test_search_summaries_semantic_top_hit(db: duckdb.DuckDBPyConnection) -> None:
    db_sid = insert_sprig(
        db, "s1", "How to create a database index to speed up slow SQL queries.", ["u1"]
    )
    insert_sprig(db, "s2", "The best way to cook spaghetti pasta al dente.", ["u2"])
    backfill_summary_embeddings(db)

    hits = search_summaries(db, "speeding up SQL queries with indexes", k=2)
    assert hits, "expected summary candidate hits"
    assert hits[0]["summary_id"] == db_sid
    assert hits[0]["session_id"] == "s1"
    assert "index" in hits[0]["content"].lower()
    dists = [h["distance"] for h in hits]
    assert dists == sorted(dists)


# ---------------------------------------------------------------------------
# retrieve_relevant: mixed tiers + the trust-tier floor end-to-end
# ---------------------------------------------------------------------------


def test_retrieve_relevant_returns_mixed_tiers_with_correct_tags(
    db: duckdb.DuckDBPyConnection,
) -> None:
    """A record AND a summary both surface, each tagged with its own tier."""
    _insert_records(
        db,
        _user_record(
            "rec-1", "sess-a", "We added a B-tree database index to speed up queries."
        ),
    )
    insert_sprig(
        db,
        "sess-a",
        "Summary: the session added a B-tree database index to speed up slow queries.",
        ["rec-1"],
    )
    backfill_embeddings(db)
    backfill_summary_embeddings(db)
    rebuild_fts_index(db)

    results = retrieve_relevant(
        db, "what database index did we add to speed up queries?", k=10, now_ms=0
    )
    tiers = {r["tier"] for r in results}
    assert tiers == {"primary", "summary"}, f"expected both tiers, got {tiers}"

    summary_hits = [r for r in results if r["tier"] == "summary"]
    assert summary_hits, "the summary must be retrievable"
    sh = summary_hits[0]
    # A summary hit traces back via summary_id + content (records use uuid + raw).
    assert sh["summary_id"] == sh["uuid"]
    assert "content" in sh and "raw" not in sh

    record_hits = [r for r in results if r["tier"] == "primary"]
    assert record_hits[0]["uuid"] == "rec-1"


def test_trust_tier_floor_fires_end_to_end(db: duckdb.DuckDBPyConnection) -> None:
    """HEADLINE (is-565.4): a summary of EQUAL-OR-HIGHER raw relevance still
    ranks BELOW the primary record — the trust-tier floor fires through the
    real pipeline, not just the synthetic unit test.

    The summary content is a SUPERSET of the record's sentence plus extra
    on-topic context, so the cross-encoder gives it raw relevance
    (``rerank_score``) at least equal to the record's. Without the tier penalty
    the summary would tie or outrank the record; with it, the primary wins.
    Asserting ``summary_rerank >= record_rerank`` AND ``record_final >
    summary_final`` is the precise statement that the floor did the work.
    """
    record_text_content = (
        "We use a B-tree index on the created_at column to speed up the "
        "slow timestamp-range queries."
    )
    _insert_records(db, _user_record("rec-1", "sess-a", record_text_content))
    # Summary contains the record's claim verbatim plus extra relevant prose,
    # so its raw relevance to the query is at least the record's.
    insert_sprig(
        db,
        "sess-a",
        record_text_content
        + " This indexing decision came after profiling showed full table "
        "scans dominating query latency; the B-tree index resolved it.",
        ["rec-1"],
    )
    backfill_embeddings(db)
    backfill_summary_embeddings(db)
    rebuild_fts_index(db)

    results = retrieve_relevant(
        db,
        "what index do we use to speed up the slow timestamp-range queries?",
        k=10,
        now_ms=0,
    )
    by_tier = {r["tier"]: r for r in results}
    assert "primary" in by_tier and "summary" in by_tier, (
        f"both tiers must surface; got {[r['tier'] for r in results]}"
    )
    record_hit = by_tier["primary"]
    summary_hit = by_tier["summary"]

    # Premise: the summary's RAW relevance is at least the record's — the case
    # the floor exists to handle. (If this ever fails the floor is untested,
    # so it must fail loudly rather than pass vacuously.)
    assert summary_hit["rerank_score"] >= record_hit["rerank_score"], (
        "test premise broken: summary must be at least as raw-relevant as the "
        f"record (summary={summary_hit['rerank_score']}, "
        f"record={record_hit['rerank_score']})"
    )

    # Floor fires: despite equal-or-higher raw relevance, the primary wins.
    assert record_hit["final_score"] > summary_hit["final_score"], (
        "trust-tier floor failed: summary outranked primary of equal raw relevance"
    )
    uuids = [r["uuid"] for r in results]
    assert uuids.index("rec-1") < uuids.index(summary_hit["uuid"])
