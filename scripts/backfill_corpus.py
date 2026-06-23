"""One-shot full-corpus backfill: embeddings + BM25 FTS (pebble is-565.2).

Operational tool, run ONCE over the live DB after the retrieval brain
(is-565.1/.2/.3) lands, so ``retrieve_relevant`` returns real hits instead of
[] (the embedding + FTS indexes are NOT built at server startup by design).

Run with the ingest-sessions venv while the MCP server is STOPPED (DuckDB is
single-writer; the running service holds the file lock):

    systemctl --user stop ingest-sessions.service
    uv run python scripts/backfill_corpus.py
    systemctl --user start ingest-sessions.service

Performance notes (measured on a 32-core CPU box, ~862k records):
  * Embedding is the floor at ~140 records/s (ONNX MiniLM, memory-bandwidth
    bound — neither extra onnxruntime threads nor fastembed ``parallel`` move
    it materially), so a full run is ~1.5-2h.
  * We must not ADD to that floor. The naive ``backfill_embeddings`` anti-join
    (``LEFT JOIN record_embeddings ... ORDER BY uuid``) re-scans + re-sorts the
    whole corpus on every chunk — that tripled wall-clock (24/s vs 140/s). This
    script instead KEYSET-paginates ``records`` by its ``uuid`` primary key
    (``WHERE uuid > :cursor ORDER BY uuid LIMIT page``), an index range scan
    with no anti-join, embedding back-to-back at the model's ceiling.
  * Bulk-load the vector index: the HNSW index is dropped before the inserts
    and rebuilt once afterward (per-insert HNSW maintenance over the corpus is
    far slower than one bulk build).

Resumable: the keyset cursor starts at ``max(uuid)`` already in
``record_embeddings``, so an interrupted run continues where it stopped (both
this script and the records scan order by uuid, so the cursor is exact).
``INSERT OR IGNORE`` keeps re-inserts idempotent; the FTS rebuild is idempotent.
"""

from __future__ import annotations

import json
import sys
import time

import duckdb

from ingest_sessions.core import create_tables, load_vss, rebuild_fts_index
from ingest_sessions.embeddings import embed_texts, record_text
from ingest_sessions.server import _db_path

# Mirrors the HNSW DDL in core.create_tables() — replicated here only so the
# bulk-load path can drop/recreate it around the inserts.
_HNSW_CREATE = (
    "CREATE INDEX IF NOT EXISTS idx_record_embeddings_hnsw "
    "ON record_embeddings USING HNSW (embedding) WITH (metric = 'cosine')"
)

_PAGE = 2048  # records fetched + embedded per keyset page


def _log(msg: str) -> None:
    print(f"[backfill] {msg}", file=sys.stderr, flush=True)


def _backfill_embeddings_keyset(db: duckdb.DuckDBPyConnection) -> int:
    """Embed every record missing an embedding, keyset-paginated by uuid.

    Resumes from the highest already-embedded uuid. Returns rows embedded.
    """
    cursor_row = db.execute("SELECT max(uuid) FROM record_embeddings").fetchone()
    cursor = cursor_row[0] if cursor_row and cursor_row[0] is not None else ""

    total_row = db.execute("SELECT count(*) FROM records").fetchone()
    assert total_row is not None
    total = total_row[0]
    done_row = db.execute("SELECT count(*) FROM record_embeddings").fetchone()
    assert done_row is not None
    embedded = done_row[0]
    _log(f"{total} records; {embedded} already embedded; resuming after {cursor!r}")

    start = time.time()
    while True:
        rows = db.execute(
            "SELECT uuid, raw FROM records WHERE uuid > ? ORDER BY uuid LIMIT ?",
            [cursor, _PAGE],
        ).fetchall()
        if not rows:
            break
        texts = [record_text(json.loads(raw)) for _uuid, raw in rows]
        vectors = embed_texts(texts)
        now_ms = int(time.time() * 1000)
        db.executemany(
            "INSERT OR IGNORE INTO record_embeddings VALUES (?, ?, ?)",
            [
                (uuid, vector, now_ms)
                for (uuid, _raw), vector in zip(rows, vectors, strict=True)
            ],
        )
        embedded += len(rows)
        cursor = rows[-1][0]
        rate = (embedded - done_row[0]) / max(1e-6, time.time() - start)
        eta_min = (total - embedded) / max(1e-6, rate) / 60.0
        _log(f"embedded {embedded}/{total} ({rate:.0f}/s, ETA ~{eta_min:.0f}m)")
    return embedded


def main() -> int:
    path = _db_path()
    _log(f"opening {path}")
    db = duckdb.connect(path)
    try:
        create_tables(db)
        load_vss(db)

        _log("dropping HNSW index for bulk load")
        db.execute("DROP INDEX IF EXISTS idx_record_embeddings_hnsw")

        total_embedded = _backfill_embeddings_keyset(db)

        _log("rebuilding HNSW index (bulk)")
        db.execute(_HNSW_CREATE)

        _log("rebuilding FTS (BM25) index")
        fts_rows = rebuild_fts_index(db)

        _log(f"DONE: {total_embedded} embeddings, {fts_rows} FTS rows")
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
