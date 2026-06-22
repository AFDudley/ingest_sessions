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


def _get_model() -> TextCrossEncoder:
    """Return the cached cross-encoder, instantiating it on first use."""
    global _MODEL
    if _MODEL is None:
        from fastembed.rerank.cross_encoder import TextCrossEncoder

        _MODEL = TextCrossEncoder(model_name=RERANK_MODEL)
    return _MODEL


def rerank(query: str, documents: list[str]) -> list[float]:
    """Score each document's relevance to *query* (higher = more relevant).

    Returns one float per document, order-aligned with the input. An empty
    document list yields ``[]`` without loading the model.
    """
    if not documents:
        return []
    model = _get_model()
    return [float(score) for score in model.rerank(query, documents)]
