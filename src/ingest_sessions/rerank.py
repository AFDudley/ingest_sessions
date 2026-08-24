"""On-device cross-encoder rerank — stage 2 of the is-565.2 retrieve pipeline.

Where ``embeddings.py`` (vector) and ``retrieval.search_lexical`` (BM25) feed
``retrieval.retrieve_candidates`` a BOUNDED stage-1 candidate set, this module
reranks those candidates by relevance with an ONNX cross-encoder (via
``fastembed`` — no torch). A cross-encoder jointly attends to (query, document)
so it scores relevance far more precisely than the bi-encoder cosine distance
used for candidate generation; it is too expensive to run over the whole
corpus, which is exactly why it runs only over the stage-1 shortlist.

Design (see .claude doctrine: functional core, imperative shell):
  * ``rerank`` is the relevance scorer — query + documents in, score-per-doc
    out (order-aligned, higher = more relevant).
  * The ONNX model is expensive to load, so it is instantiated lazily and
    cached as a module-level singleton (never at import time). ``fastembed``
    itself is imported inside ``_get_model`` so importing this module stays
    cheap for callers that only need ``RERANK_MODEL``.

This slice is PURE relevance rerank. The is-565.3 ranking composition
(recency / confidence / supersession / trust-tier) is the sibling that will
combine these scores with the other signals.
"""

from __future__ import annotations

import json
import os
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastembed.rerank.cross_encoder import TextCrossEncoder

# An ms-marco MiniLM cross-encoder supported by fastembed (verified against
# TextCrossEncoder.list_supported_models()). The L-6 variant is the
# general-purpose relevance reranker — pretrained, zero project training.
RERANK_MODEL = "Xenova/ms-marco-MiniLM-L-6-v2"

# Lazily-instantiated singleton — loading the ONNX model is expensive and
# must never happen at import time.
_MODEL: TextCrossEncoder | None = None


def _rerank_engine() -> str:
    """Which backend ``rerank`` uses (env ``INGEST_SESSIONS_RERANK_ENGINE``).

    ``onnx`` (default) runs fastembed's CPU ONNX cross-encoder in-process,
    unchanged behavior. ``vllm`` calls a vLLM ``/score`` endpoint over HTTP —
    the un-quantized ``cross-encoder/ms-marco-MiniLM-L-6-v2`` weights served
    on a GPU (pebble is-af8). Verified on matched inputs: scores agree to
    within ~0.004 logit units and rank order is identical, so swapping
    engines does not change which candidates surface.
    """
    return os.environ.get("INGEST_SESSIONS_RERANK_ENGINE", "onnx")


def _rerank_url() -> str:
    """vLLM score endpoint (env ``INGEST_SESSIONS_RERANK_URL``)."""
    return os.environ.get("INGEST_SESSIONS_RERANK_URL", "http://127.0.0.1:8003/score")


def _rerank_model_name() -> str:
    """Served model name at the vLLM endpoint (env ``INGEST_SESSIONS_RERANK_MODEL_NAME``)."""
    return os.environ.get("INGEST_SESSIONS_RERANK_MODEL_NAME", "rerank-msmarco")


def _get_model() -> TextCrossEncoder:
    """Return the cached cross-encoder, instantiating it on first use."""
    global _MODEL
    if _MODEL is None:
        from fastembed.rerank.cross_encoder import TextCrossEncoder

        _MODEL = TextCrossEncoder(model_name=RERANK_MODEL)
    return _MODEL


def _rerank_onnx(query: str, documents: list[str]) -> list[float]:
    """Score via the in-process fastembed ONNX cross-encoder (CPU)."""
    model = _get_model()
    return [float(score) for score in model.rerank(query, documents)]


def _rerank_vllm(query: str, documents: list[str]) -> list[float]:
    """Score via a vLLM ``/score`` endpoint (env ``INGEST_SESSIONS_RERANK_ENGINE=vllm``).

    Raises on any transport or protocol error — callers get a clear failure
    rather than a silent fall back to a different scorer.
    """
    body = json.dumps(
        {"model": _rerank_model_name(), "text_1": query, "text_2": documents}
    ).encode()
    req = urllib.request.Request(
        _rerank_url(),
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read())
    ordered = sorted(payload["data"], key=lambda d: d["index"])
    return [float(d["score"]) for d in ordered]


def rerank(query: str, documents: list[str]) -> list[float]:
    """Score each document's relevance to *query* (higher = more relevant).

    Returns one float per document, order-aligned with the input. An empty
    document list yields ``[]`` without loading the model. Backend selected
    by ``INGEST_SESSIONS_RERANK_ENGINE`` (see :func:`_rerank_engine`):
    ``onnx`` (default) or ``vllm``.
    """
    if not documents:
        return []
    if _rerank_engine() == "vllm":
        return _rerank_vllm(query, documents)
    return _rerank_onnx(query, documents)
