"""On-device embeddings + DuckDB-native vector candidate search (is-565.2a).

This module is the vector arm of the is-565.2 retrieve pipeline. It embeds
record text with a small ONNX sentence model (via ``fastembed`` — no torch)
and stores 384-dim vectors in the ``record_embeddings`` sidecar table, over
which DuckDB's ``vss`` HNSW index serves cosine-distance candidate search.

Design (see .claude doctrine: functional core, imperative shell):
  * Pure helpers compute — ``record_text`` flattens a record's parsed ``raw``
    JSON to embeddable text; ``embed_texts`` / ``embed_query`` turn text into
    vectors.
  * IO at the boundary — ``backfill_embeddings`` and ``search_vector`` are the
    only functions that touch the DuckDB connection.
  * The ONNX model is expensive to load, so it is instantiated lazily and
    cached as a module-level singleton (never at import time). ``fastembed``
    itself is imported inside ``_get_model`` so importing this module stays
    cheap for callers (e.g. core.create_tables) that only need ``EMBED_DIM``.

Schema, vss extension load, and the HNSW index live in ``core.py`` so they are
created on every connection alongside the rest of the tables.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

import duckdb

from ingest_sessions.retrieval import _content_to_text

if TYPE_CHECKING:
    from fastembed import TextEmbedding

# A 384-dim ONNX sentence model supported by fastembed (verified against
# TextEmbedding.list_supported_models()). MiniLM-L6-v2 is the default
# general-purpose choice; BAAI/bge-small-en-v1.5 is the 384-dim fallback.
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBED_DIM = 384

# Lazily-instantiated singleton — loading the ONNX model is expensive and
# must never happen at import time.
_MODEL: TextEmbedding | None = None


def _get_model() -> TextEmbedding:
    """Return the cached embedding model, instantiating it on first use."""
    global _MODEL
    if _MODEL is None:
        from fastembed import TextEmbedding

        _MODEL = TextEmbedding(model_name=EMBED_MODEL)
    return _MODEL


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts into ``EMBED_DIM``-length float vectors."""
    if not texts:
        return []
    model = _get_model()
    return [[float(x) for x in vec] for vec in model.embed(texts)]


def embed_query(text: str) -> list[float]:
    """Embed a single query string into an ``EMBED_DIM``-length vector."""
    return embed_texts([text])[0]


def record_text(raw: dict[str, Any]) -> str:
    """Flatten a record's parsed ``raw`` JSON to embeddable text (pure).

    Combines the message role with its flattened content (reusing
    ``retrieval._content_to_text`` so blob markers, tool blocks and nested
    tool_results are handled identically to the rest of the retrieval
    surface). Returns ``""`` for records without a usable message.
    """
    msg = raw.get("message")
    if not isinstance(msg, dict):
        return ""
    role = msg.get("role", "")
    text = _content_to_text(msg.get("content"))
    if role and text:
        return f"{role}: {text}"
    return text or (str(role) if role else "")


def backfill_embeddings(
    db: duckdb.DuckDBPyConnection,
    *,
    batch_size: int = 256,
    limit: int | None = None,
) -> int:
    """Embed records that lack a ``record_embeddings`` row. Returns count added.

    Resumable and incremental: selects only records without an embedding via an
    anti-join, so a second call over an unchanged corpus embeds 0 and a newly
    ingested record is picked up on the next call. ``limit`` caps the number of
    records considered (for tests / operational throttling); ``batch_size``
    bounds how many texts are embedded per model call.
    """
    sql = (
        "SELECT r.uuid, r.raw FROM records r "
        "LEFT JOIN record_embeddings e ON r.uuid = e.uuid "
        "WHERE e.uuid IS NULL ORDER BY r.uuid"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    rows = db.execute(sql).fetchall()

    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        texts = [record_text(json.loads(raw)) for _uuid, raw in batch]
        vectors = embed_texts(texts)
        now_ms = int(time.time() * 1000)
        db.executemany(
            "INSERT OR IGNORE INTO record_embeddings VALUES (?, ?, ?)",
            [
                (uuid, vector, now_ms)
                for (uuid, _raw), vector in zip(batch, vectors, strict=True)
            ],
        )
        total += len(batch)
    return total


def search_vector(
    db: duckdb.DuckDBPyConnection, query: str, k: int = 20
) -> list[dict[str, Any]]:
    """Return the ``k`` nearest records to *query* by cosine distance.

    Embeds the query and ranks ``record_embeddings`` by
    ``array_cosine_distance`` (lower = more similar), joining ``records`` to
    return the parsed ``raw`` payload. Each hit is
    ``{uuid, session_id, distance, raw}``.
    """
    qvec = embed_query(query)
    rows = db.execute(
        "SELECT uuid, session_id, raw, "
        f"array_cosine_distance(embedding, ?::FLOAT[{EMBED_DIM}]) AS distance "
        "FROM record_embeddings JOIN records USING (uuid) "
        "ORDER BY distance LIMIT ?",
        [qvec, k],
    ).fetchall()
    return [
        {
            "uuid": uuid,
            "session_id": session_id,
            "distance": distance,
            "raw": json.loads(raw),
        }
        for uuid, session_id, raw, distance in rows
    ]
