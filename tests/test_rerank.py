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
import pytest

from ingest_sessions.core import rebuild_fts_index
from ingest_sessions.embeddings import backfill_embeddings
from ingest_sessions.rerank import RERANK_MODEL, _rerank_engine, rerank
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


# ---------------------------------------------------------------------------
# engine selection (env INGEST_SESSIONS_RERANK_ENGINE) — mocked HTTP, no live
# vLLM server required. See rerank.py's docstring for the equivalence check
# run against a real GPU server: scores agree to ~0.004 logit units and rank
# order is identical to fastembed's ONNX output on matched inputs.
# ---------------------------------------------------------------------------


def test_rerank_engine_defaults_to_onnx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_SESSIONS_RERANK_ENGINE", raising=False)
    assert _rerank_engine() == "onnx"


def test_rerank_vllm_dispatches_to_http(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_SESSIONS_RERANK_ENGINE", "vllm")
    captured: dict = {}

    class _FakeResponse:
        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def read(self) -> bytes:
            # Out-of-order rows to prove the caller re-sorts by "index".
            body = {"data": [{"index": 1, "score": 0.7}, {"index": 0, "score": 0.9}]}
            return json.dumps(body).encode()

    def _fake_urlopen(req: object, timeout: float = 0) -> _FakeResponse:
        captured["url"] = req.full_url  # type: ignore[attr-defined]
        captured["body"] = json.loads(req.data)  # type: ignore[attr-defined]
        return _FakeResponse()

    import ingest_sessions.rerank as rerank_mod

    monkeypatch.setattr(rerank_mod.urllib.request, "urlopen", _fake_urlopen)

    scores = rerank("q", ["doc a", "doc b"])

    assert captured["url"] == "http://127.0.0.1:8003/score"
    assert captured["body"] == {
        "model": "rerank-msmarco",
        "text_1": "q",
        "text_2": ["doc a", "doc b"],
    }
    assert scores == [0.9, 0.7]


def test_rerank_vllm_respects_url_and_model_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGEST_SESSIONS_RERANK_ENGINE", "vllm")
    monkeypatch.setenv("INGEST_SESSIONS_RERANK_URL", "http://127.0.0.1:9999/score")
    monkeypatch.setenv("INGEST_SESSIONS_RERANK_MODEL_NAME", "custom-rerank")
    captured: dict = {}

    class _FakeResponse:
        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps({"data": [{"index": 0, "score": 1.0}]}).encode()

    def _fake_urlopen(req: object, timeout: float = 0) -> _FakeResponse:
        captured["url"] = req.full_url  # type: ignore[attr-defined]
        captured["body"] = json.loads(req.data)  # type: ignore[attr-defined]
        return _FakeResponse()

    import ingest_sessions.rerank as rerank_mod

    monkeypatch.setattr(rerank_mod.urllib.request, "urlopen", _fake_urlopen)

    rerank("q", ["doc"])

    assert captured["url"] == "http://127.0.0.1:9999/score"
    assert captured["body"]["model"] == "custom-rerank"


def test_rerank_vllm_empty_documents_skips_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGEST_SESSIONS_RERANK_ENGINE", "vllm")

    def _fail_urlopen(*args: object, **kwargs: object) -> None:
        raise AssertionError("must not call the network for an empty document list")

    import ingest_sessions.rerank as rerank_mod

    monkeypatch.setattr(rerank_mod.urllib.request, "urlopen", _fail_urlopen)

    assert rerank("q", []) == []
