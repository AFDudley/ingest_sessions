"""Tests for on-device embeddings + DuckDB-native vector search (is-565.2a).

These exercise REAL fastembed ONNX embeddings (no model mock) — the
search_vector test is the end-to-end of the slice: a semantically near
query must surface the matching record as the top hit.
"""

from __future__ import annotations

import json
import time

import duckdb

from ingest_sessions.embeddings import (
    EMBED_DIM,
    backfill_embeddings,
    embed_query,
    embed_texts,
    record_text,
    search_vector,
)


def _user_record(uuid: str, session_id: str, text: str) -> tuple:
    """Build a row for the records table with a Claude-Code-shaped raw JSON."""
    raw = {
        "uuid": uuid,
        "sessionId": session_id,
        "type": "user",
        "message": {"role": "user", "content": text},
    }
    return (uuid, session_id, "user", None, None, json.dumps(raw))


def _insert(db: duckdb.DuckDBPyConnection, *rows: tuple) -> None:
    db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)", rows)


def _scalar(db: duckdb.DuckDBPyConnection, sql: str) -> object:
    """Return the first column of the single result row (asserts it exists)."""
    row = db.execute(sql).fetchone()
    assert row is not None
    return row[0]


def test_embed_texts_dimension_and_determinism() -> None:
    vecs = embed_texts(["database indexing strategies", "boiling pasta al dente"])
    assert len(vecs) == 2
    assert all(len(v) == EMBED_DIM == 384 for v in vecs)
    assert all(isinstance(x, float) for x in vecs[0])
    # Deterministic for identical input.
    again = embed_texts(["database indexing strategies"])
    assert again[0] == vecs[0]


def test_embed_query_single() -> None:
    v = embed_query("how do HNSW indexes work")
    assert len(v) == EMBED_DIM


def test_record_text_extracts_role_and_content() -> None:
    raw = {
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Use a covering index."},
                {"type": "tool_use", "name": "Bash"},
            ],
        }
    }
    text = record_text(raw)
    assert "assistant" in text
    assert "Use a covering index." in text


def test_record_text_handles_missing_message() -> None:
    assert record_text({"no": "message"}) == ""


def test_backfill_idempotent_and_incremental(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        _user_record("u1", "s1", "indexing in postgres"),
        _user_record("u2", "s1", "vector search with HNSW"),
    )
    first = backfill_embeddings(db)
    assert first == 2
    assert _scalar(db, "SELECT count(*) FROM record_embeddings") == 2

    # Second call embeds nothing (idempotent).
    assert backfill_embeddings(db) == 0

    # Adding one new record then backfilling embeds only the new one.
    _insert(db, _user_record("u3", "s2", "cooking risotto"))
    assert backfill_embeddings(db) == 1
    assert _scalar(db, "SELECT count(*) FROM record_embeddings") == 3


def test_backfill_limit(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        _user_record("a", "s", "alpha"),
        _user_record("b", "s", "beta"),
        _user_record("c", "s", "gamma"),
    )
    assert backfill_embeddings(db, limit=2) == 2
    assert backfill_embeddings(db) == 1


def test_search_vector_semantic_top_hit(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        _user_record(
            "db", "s1", "How do I create a database index to speed up queries?"
        ),
        _user_record(
            "pasta", "s2", "What is the best way to cook spaghetti pasta al dente?"
        ),
        _user_record("weather", "s3", "Will it rain tomorrow in the mountains?"),
    )
    backfill_embeddings(db)

    hits = search_vector(db, "speeding up SQL queries with indexes", k=3)
    assert hits, "expected candidate hits"
    assert hits[0]["uuid"] == "db"
    # distances are sorted ascending (nearest first).
    dists = [h["distance"] for h in hits]
    assert dists == sorted(dists)
    # raw is parsed back to a dict and carries the session id.
    assert hits[0]["session_id"] == "s1"
    assert isinstance(hits[0]["raw"], dict)


def test_backfill_embedded_at_is_set(db: duckdb.DuckDBPyConnection) -> None:
    before = int(time.time() * 1000)
    _insert(db, _user_record("x", "s", "hello"))
    backfill_embeddings(db)
    ts = _scalar(db, "SELECT embedded_at FROM record_embeddings WHERE uuid='x'")
    assert isinstance(ts, int)
    assert ts >= before
