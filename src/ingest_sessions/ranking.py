"""Ranking composition for ingest_sessions (pebble is-565.3) — the differentiator.

The cross-encoder rerank (is-565.2c) answers a single question: how relevant is
a record to the query? That is necessary but not sufficient. A retrieval brain
that surfaces prior reasoning must also know which reasoning is *current*, which
is *recent*, which is *trustworthy*, and which has been *superseded* by a later
conclusion. This module composes those signals on top of raw relevance:

    final = relevance × recency_decay × confidence × not_superseded × trust_tier

Every factor is a multiplier in ``[0, 1]`` (or, for recency, ``(0, 1]``), so the
composition only ever *down-weights* raw relevance — a record is never promoted
above its relevance ceiling, and a demoted record is never deleted from the
result set (it stays retrievable, just ranked lower and flagged).

Design (see .claude doctrine: functional core, imperative shell):
  * ``compose_score`` is the PURE scoring kernel — scalars in, scalar out.
  * ``rank_candidates`` is PURE orchestration — a list of candidate dicts and an
    injected ``now_ms`` in, a sorted list of annotated dicts out. The only DB
    read it needs (the superseded-id set) is done by the CALLER and passed in as
    ``superseded_ids``, keeping this module free of IO.
  * ``default_confidence`` is a small, swappable, honest heuristic.

SEAM NOTE (trust-tier is implemented + unit-tested, not yet end-to-end): the
live ``retrieval.retrieve_relevant`` pipeline only indexes the primary
``records`` tier today — the ``summaries`` auto-summary tier (sprigs/bindles) is
not yet a retrieval candidate source. So the trust-tier floor (a summary never
outranks a contradicting primary record of equal raw relevance) is exercised by
unit tests with synthetic mixed-tier candidates, NOT yet end-to-end. Wiring the
summary tier INTO retrieval is a follow-up (is-565.2 territory) — deliberately
out of scope here. The supersession + recency + confidence factors ARE wired
end-to-end via ``retrieve_relevant``.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

# --- Defensible defaults (exposed as params so they are tunable + testable) ---

# Recency half-life: a record's recency factor halves every 30 days. Prior
# reasoning stays useful for a long time (this is a knowledge base, not a news
# feed), so the decay is gentle — a month-old conclusion is worth half a fresh
# one, all else equal.
DEFAULT_HALF_LIFE_SECONDS = 30 * 86_400.0

# Supersession down-weight: a superseded record keeps 5% of its score. Strong
# enough that a superseded record almost always falls below its live
# replacement, but non-zero so it remains retrievable and visibly flagged
# (``superseded=True``) rather than silently dropped.
DEFAULT_SUPERSEDED_PENALTY = 0.05

# Trust-tier down-weight: a non-primary (auto-summary) record keeps 50% of its
# score. This is the floor that stops a derived summary from outranking a
# primary-source record of equal raw relevance — the summary must be clearly
# more relevant on its own merits to surface above the primary it summarizes.
DEFAULT_SUMMARY_TIER_PENALTY = 0.5

# The trust tier of a primary-source record (a real captured message). Any other
# tier value (e.g. 'summary') is treated as a derived/auto tier and incurs the
# trust-tier penalty.
PRIMARY_TIER = "primary"

# Confidence heuristic: record ``type`` values that are tool/process noise
# rather than substantive reasoning. These get a reduced confidence so a
# progress ping never outranks a real conclusion of equal relevance. This is an
# honest heuristic (record type), NOT a learned signal — swap ``default_confidence``
# for a better estimator when one exists.
_LOW_CONFIDENCE_TYPES = frozenset({"progress", "system"})
_LOW_CONFIDENCE_VALUE = 0.5


def _safe_sigmoid(x: float) -> float:
    """Logistic sigmoid mapping any real to ``(0, 1)`` without overflow.

    The cross-encoder emits relevance *logits* that can be negative; the
    multiplicative kernel needs a non-negative relevance magnitude (a negative
    multiplied by a ``<1`` penalty would *increase* toward zero — inverting the
    down-weight). Sigmoid turns the logit into a probability-like relevance in
    ``(0, 1)``, which composes correctly.
    """
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    z = math.exp(x)
    return z / (1.0 + z)


def recency_decay(age_seconds: float, half_life_seconds: float) -> float:
    """Exponential half-life decay: ``0.5 ** (age / half_life)`` (pure).

    Returns ``1.0`` for a record of age 0 and halves every ``half_life_seconds``.
    A negative age (a record timestamped in the future, e.g. clock skew) is
    clamped to 0 so it cannot be promoted above a fresh record.
    """
    if half_life_seconds <= 0:
        raise ValueError(f"half_life_seconds must be positive, got {half_life_seconds}")
    age = max(0.0, age_seconds)
    return 0.5 ** (age / half_life_seconds)


def compose_score(
    *,
    relevance: float,
    age_seconds: float,
    confidence: float,
    superseded: bool,
    tier: str,
    half_life_seconds: float = DEFAULT_HALF_LIFE_SECONDS,
    superseded_penalty: float = DEFAULT_SUPERSEDED_PENALTY,
    summary_tier_penalty: float = DEFAULT_SUMMARY_TIER_PENALTY,
) -> float:
    """Compose a final ranking score from the independent signals (pure kernel).

    ``final = relevance × recency_decay × confidence × superseded_factor ×
    tier_factor``, where:

      * ``relevance`` — a NON-NEGATIVE relevance magnitude (e.g. the sigmoid of a
        cross-encoder logit; ``rank_candidates`` does that conversion). The
        kernel assumes ``relevance >= 0`` so every other factor only ever
        down-weights it.
      * ``recency_decay`` — ``0.5 ** (age_seconds / half_life_seconds)``; older →
        smaller (see ``recency_decay``).
      * ``confidence`` — a ``[0, 1]`` trust-in-the-content multiplier (see
        ``default_confidence``).
      * ``superseded_factor`` — ``superseded_penalty`` when ``superseded`` else
        ``1.0``. Down-weights (does not delete) a record a later one replaced.
      * ``tier_factor`` — ``summary_tier_penalty`` when ``tier != 'primary'`` else
        ``1.0``. The floor that keeps an auto-summary from outranking a primary
        record of equal raw relevance.
    """
    decay = recency_decay(age_seconds, half_life_seconds)
    superseded_factor = superseded_penalty if superseded else 1.0
    tier_factor = summary_tier_penalty if tier != PRIMARY_TIER else 1.0
    return relevance * decay * confidence * superseded_factor * tier_factor


def default_confidence(raw: dict[str, Any]) -> float:
    """Heuristic confidence in a record's content from its ``type`` (pure).

    HONEST HEURISTIC, not a learned signal: substantive turns (a real user /
    assistant message) get full confidence ``1.0``; tool/process-noise record
    types (``progress``, ``system``) get a reduced ``0.5`` so they cannot
    outrank a real conclusion of equal relevance. Unknown types default to
    ``1.0`` (do not penalize what we do not understand). Swap this function for
    a better estimator when one exists — it is intentionally small and pure.
    """
    rtype = raw.get("type")
    if isinstance(rtype, str) and rtype in _LOW_CONFIDENCE_TYPES:
        return _LOW_CONFIDENCE_VALUE
    return 1.0


def _record_timestamp_ms(raw: dict[str, Any]) -> int | None:
    """Parse a record's ``timestamp`` (ISO-8601) to epoch ms, or None (pure).

    Claude Code records carry an ISO-8601 ``timestamp`` (e.g.
    ``"2026-03-01T12:00:00.000Z"``). A record with no parseable timestamp
    returns None — the caller then treats it as age 0 (no recency penalty),
    documented in ``rank_candidates``.
    """
    ts = raw.get("timestamp")
    if not isinstance(ts, str) or not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def rank_candidates(
    candidates: list[dict[str, Any]],
    *,
    superseded_ids: set[str],
    now_ms: int,
    superseded_session_ids: set[str] | None = None,
    half_life_seconds: float = DEFAULT_HALF_LIFE_SECONDS,
    superseded_penalty: float = DEFAULT_SUPERSEDED_PENALTY,
    summary_tier_penalty: float = DEFAULT_SUMMARY_TIER_PENALTY,
) -> list[dict[str, Any]]:
    """Rank reranked candidates by composed score (pure, data-in/data-out).

    Each candidate dict must carry ``uuid``, ``rerank_score`` (the cross-encoder
    relevance logit), and ``raw`` (the parsed record). It may carry
    ``session_id`` (the transcript it belongs to), ``tier`` (defaults to
    ``'primary'``) and ``confidence`` (defaults to ``default_confidence(raw)``).
    For each candidate this:

      * converts ``rerank_score`` to a non-negative ``relevance`` via sigmoid,
      * derives ``age_seconds`` from the record timestamp vs the injected
        ``now_ms`` (a record with no parseable timestamp is treated as age 0 —
        no recency penalty),
      * marks ``superseded`` when the candidate's ``uuid`` is in
        ``superseded_ids`` (RECORD-level supersession) OR its ``session_id`` is
        in ``superseded_session_ids`` (SESSION-level supersession — every
        record of a superseded session is flagged, even one whose own uuid is
        not directly listed),
      * calls ``compose_score``,

    and returns a NEW list (originals untouched) of candidate dicts each
    augmented with ``final_score``, ``superseded`` (bool flag), and ``recency``
    (the decay factor, for transparency), sorted by ``final_score`` descending.
    ``now_ms`` is injected (not read here) so ranking is deterministic in tests.
    Ties break by ``uuid`` for stable, reproducible ordering.
    """
    now_s = now_ms / 1000.0
    session_ids = superseded_session_ids or set()
    ranked: list[dict[str, Any]] = []
    for cand in candidates:
        raw = cand.get("raw")
        raw_dict: dict[str, Any] = raw if isinstance(raw, dict) else {}
        relevance = _safe_sigmoid(float(cand["rerank_score"]))
        ts_ms = _record_timestamp_ms(raw_dict)
        age_seconds = 0.0 if ts_ms is None else now_s - (ts_ms / 1000.0)
        tier = cand.get("tier", PRIMARY_TIER)
        confidence = cand.get("confidence")
        if confidence is None:
            confidence = default_confidence(raw_dict)
        session_id = cand.get("session_id")
        superseded = cand["uuid"] in superseded_ids or (
            session_id is not None and session_id in session_ids
        )
        decay = recency_decay(age_seconds, half_life_seconds)
        final = compose_score(
            relevance=relevance,
            age_seconds=age_seconds,
            confidence=float(confidence),
            superseded=superseded,
            tier=tier,
            half_life_seconds=half_life_seconds,
            superseded_penalty=superseded_penalty,
            summary_tier_penalty=summary_tier_penalty,
        )
        ranked.append(
            {
                **cand,
                "final_score": final,
                "superseded": superseded,
                "recency": decay,
            }
        )
    ranked.sort(key=lambda c: (-c["final_score"], c["uuid"]))
    return ranked
