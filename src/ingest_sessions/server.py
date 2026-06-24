"""ingest_sessions MCP server.

Runs as a long-lived HTTP process. Multiple Claude Code instances connect to
the same server over streamable-HTTP at /mcp.  Stdio transport is retained
for single-client use and testing.

All database access goes through a single thread that owns the only DuckDB
connection.  Callers submit work to a queue; the thread processes it
sequentially.  DuckDB does not allow mixing read-only and read-write
connections in the same process, so one thread, one connection, one queue.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, TypeVar

import duckdb
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import AnyUrl, Resource, TextContent, Tool
from watchdog.events import (
    DirModifiedEvent,
    FileModifiedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

from ingest_sessions.dag import (
    BINDLE_THRESHOLD,
    SPRIG_CHUNK_SIZE,
    assemble_context_for_session,
    get_latest_bindle_content,
    get_latest_summarized_session,
    get_sprigs_for_session,
    get_unsummarized_messages,
    insert_bindle,
    insert_sprig,
)
from ingest_sessions.retrieval import (
    format_session_text,
    get_full_session,
    retrieve_relevant,
)
from ingest_sessions.summarize import (
    condense_summaries,
    summarize_messages,
)
from ingest_sessions.supersession import (
    add_supersession,
    add_supersessions,
)
from ingest_sessions.core import (
    build_session_metadata,
    create_tables,
    file_changed,
    ingest_history,
    ingest_jsonl,
    ingest_session_metadata,
    fetch_record_raws,
    project_record_texts,
    record_file,
    register_functions,
    write_fts_index,
)
from ingest_sessions.embeddings import embed_texts, record_text, summary_text

T = TypeVar("T")

DEFAULT_PORT = 8741

server = Server("ingest-sessions")

# ---------------------------------------------------------------------------
# Database queue.  A single thread owns the DuckDB connection and processes
# all requests — reads and writes — sequentially.
# ---------------------------------------------------------------------------


@dataclass
class _DbRequest:
    """A unit of work for the database thread."""

    fn: Callable[[duckdb.DuckDBPyConnection], Any]
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: Exception | None = None
    log_errors: bool = False


# None is the shutdown sentinel.
_db_queue: queue.Queue[_DbRequest | None] = queue.Queue()
_startup_done = threading.Event()
_db_thread: threading.Thread | None = None


def _db_submit(fn: Callable[[duckdb.DuckDBPyConnection], Any]) -> _DbRequest:
    """Submit work to the database thread.  Does not wait."""
    req = _DbRequest(fn=fn)
    _db_queue.put(req)
    return req


async def _db_execute(fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
    """Submit work to the database thread and await its completion."""
    req = _db_submit(fn)
    await asyncio.to_thread(req.done.wait)
    if req.error is not None:
        raise req.error
    return req.result  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _db_path() -> str:
    default = str(
        Path.home() / ".local" / "share" / "ingest_sessions" / "sessions.duckdb"
    )
    return os.environ.get("INGEST_SESSIONS_DB", default)


def _projects_dir() -> Path:
    env = os.environ.get("INGEST_SESSIONS_PROJECTS_DIR")
    if env:
        return Path(env)
    return Path.home() / ".claude" / "projects"


def _history_file() -> Path:
    env = os.environ.get("INGEST_SESSIONS_HISTORY_FILE")
    if env:
        return Path(env)
    return Path.home() / ".claude" / "history.jsonl"


# ---------------------------------------------------------------------------
# Ingestion helpers (server-specific orchestration around core functions)
# ---------------------------------------------------------------------------


def _ingest_file_full(db: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
    """Ingest records AND session metadata for a single JSONL file."""
    from ingest_sessions.blobs import blob_dir

    _, prev_size = file_changed(db, jsonl_path)
    session_id = jsonl_path.stem
    count, bytes_read = ingest_jsonl(
        db, jsonl_path, byte_offset=prev_size, blob_root=blob_dir()
    )
    session_meta = build_session_metadata([jsonl_path.parent])
    ingest_session_metadata(db, session_id, session_meta)
    record_file(db, jsonl_path, size_bytes=bytes_read)
    return count


def _get_session_payload(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    *,
    include_blobs: bool,
    fmt: str,
) -> dict[str, Any]:
    """Fetch a full session and shape it for the requested output format.

    fmt="records" (default) returns the structured get_full_session() result;
    fmt="text" adds a rendered plaintext transcript under "text".
    """
    from ingest_sessions.blobs import blob_dir

    session = get_full_session(
        db, session_id, include_blobs=include_blobs, blob_root=blob_dir()
    )
    if fmt == "text":
        session["text"] = format_session_text(session)
    return session


def _ingest_all(db: duckdb.DuckDBPyConnection) -> None:
    """Incremental ingestion: only re-process files modified since last run."""
    from ingest_sessions.blobs import blob_dir

    projects_dir = _projects_dir()
    if not projects_dir.exists():
        return

    project_dirs = sorted(d for d in projects_dir.iterdir() if d.is_dir())
    session_meta = build_session_metadata(project_dirs)
    _blob_root = blob_dir()

    for proj_dir in project_dirs:
        for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
            changed, prev_size = file_changed(db, jsonl_path)
            if not changed:
                continue
            session_id = jsonl_path.stem
            _, bytes_read = ingest_jsonl(
                db, jsonl_path, byte_offset=prev_size, blob_root=_blob_root
            )
            ingest_session_metadata(db, session_id, session_meta)
            record_file(db, jsonl_path, size_bytes=bytes_read)

    history = _history_file()
    if history.exists():
        changed, _ = file_changed(db, history)
        if changed:
            ingest_history(db, history)
            record_file(db, history)


# ---------------------------------------------------------------------------
# Async summarization (shared by MCP tool handler and REST endpoint)
# ---------------------------------------------------------------------------


async def _run_summarize_async(session_id: str) -> dict[str, int]:
    """Run one pass of DAG maintenance for a session.

    DB reads/writes go through _db_execute (DB thread).
    LLM calls go through asyncio.to_thread (NOT the DB thread).

    Returns counts of what was created.
    """
    unsummarized = await _db_execute(
        partial(get_unsummarized_messages, session_id=session_id)
    )
    existing_sprigs = await _db_execute(
        partial(get_sprigs_for_session, session_id=session_id)
    )
    previous_context = existing_sprigs[-1]["content"] if existing_sprigs else None

    def _log(msg: str) -> None:
        print(f"[ingest-sessions] summarize {session_id}: {msg}", file=sys.stderr)

    _log(f"unsummarized={len(unsummarized)} existing_sprigs={len(existing_sprigs)}")

    sprigs_created = 0
    full_chunks = len(unsummarized) // SPRIG_CHUNK_SIZE
    for i in range(full_chunks):
        chunk = unsummarized[i * SPRIG_CHUNK_SIZE : (i + 1) * SPRIG_CHUNK_SIZE]
        content = await asyncio.to_thread(summarize_messages, chunk, previous_context)
        uuids = [r["uuid"] for r in chunk]
        await _db_execute(
            partial(
                insert_sprig,
                session_id=session_id,
                content=content,
                message_uuids=uuids,
            )
        )
        previous_context = content
        sprigs_created += 1

    uncondensed = await _db_execute(
        partial(get_sprigs_for_session, session_id=session_id, uncondensed_only=True)
    )
    _log(f"uncondensed={len(uncondensed)} threshold={BINDLE_THRESHOLD}")
    bindles_created = 0
    if len(uncondensed) >= BINDLE_THRESHOLD:
        bindle_context = await _db_execute(
            partial(get_latest_bindle_content, session_id=session_id)
        )
        _log(f"condensing {len(uncondensed)} sprigs into bindle")
        content = await asyncio.to_thread(
            condense_summaries, uncondensed, bindle_context
        )
        parent_ids = [s["summary_id"] for s in uncondensed]
        await _db_execute(
            partial(
                insert_bindle,
                session_id=session_id,
                content=content,
                parent_ids=parent_ids,
            )
        )
        bindles_created += 1
        _log("bindle created")

    return {
        "sprigs_created": sprigs_created,
        "bindles_created": bindles_created,
        "unsummarized_remaining": len(unsummarized) - (full_chunks * SPRIG_CHUNK_SIZE),
    }


# ---------------------------------------------------------------------------
# In-server background backfill (is-565.2): embed not-yet-embedded records and
# rebuild the BM25 FTS index WHILE the server keeps serving — the no-downtime
# replacement for the exclusive-lock scripts/backfill_corpus.py. Mirrors the
# summarize background pattern: expensive embedding work runs OFF the DB thread
# via asyncio.to_thread (onnxruntime releases the GIL, so the event loop + DB
# thread stay responsive); every DB read/write goes through the single DB
# thread via _db_execute (DuckDB forbids a second in-process connection).
# ---------------------------------------------------------------------------

# Module-level guard: only ONE backfill runs at a time. The single event loop
# makes a check-then-set across no await atomic, so the driver flips this True
# synchronously before its first await; a concurrent trigger sees it set and
# returns "already_running" instead of starting a parallel run (which would
# double-embed + thrash the HNSW index).
_backfill_running: bool = False


# ---------------------------------------------------------------------------
# Off-thread, atomic, throttled FTS rebuild (pebble is-e10).
#
# core.rebuild_fts_index ran ENTIRELY on the single DB thread: fetch every
# (uuid, raw), then json.loads + record_text for every row in Python on that
# one thread, then DELETE+INSERT, then create_fts_index. At ~873k rows the
# Python parse alone took ~45 MINUTES and blocked EVERY query (all _db_execute
# work is serialized on that thread) for the whole window. The periodic sync
# (is-565.5) re-triggered it whenever any record was embedded, so each cycle
# stalled the service service-wide.
#
# _rebuild_fts_async splits the rebuild so only the cheap phases touch the DB
# thread; the expensive parse runs OFF it via asyncio.to_thread (DuckDB query
# execution releases the GIL, so the DB thread keeps serving queries while the
# worker thread parses). The residual DB-thread block is the (uuid, text)
# INSERT plus the create_fts_index C++ index build — far smaller than the
# parse, but NOT zero (see the docstring; create_fts_index over a large corpus
# is itself a multi-second-to-minutes DB-thread cost that throttling bounds but
# cannot eliminate).
# ---------------------------------------------------------------------------

# Monotonic time of the last successful FTS rebuild ('None' = never this
# process). Read by the throttle decision (_should_rebuild_fts) so the periodic
# sweep rebuilds at most once per INGEST_SESSIONS_FTS_MIN_INTERVAL seconds.
_last_fts_rebuild_at: float | None = None


def _fts_min_interval() -> float:
    """Min seconds between THROTTLED (periodic-sweep) FTS rebuilds (default 3600).

    Decouples the expensive BM25 rebuild from the embedding cadence: records get
    embedded every INGEST_SESSIONS_SYNC_INTERVAL (vector recall stays current),
    but the FTS snapshot is rebuilt at most this often, bounding the residual
    DB-thread create_fts_index cost. Manual /api/sync and /api/backfill bypass
    the throttle.
    """
    return float(os.environ.get("INGEST_SESSIONS_FTS_MIN_INTERVAL", "3600"))


def _should_rebuild_fts(
    *,
    embedded: int,
    throttle: bool,
    force: bool,
    now: float,
    last: float | None,
    min_interval: float,
) -> bool:
    """Pure decision: should this run rebuild the FTS index? (is-e10 throttle.)

    - ``force`` → always (the /api/backfill repair path: rebuild even when no new
      records were embedded, e.g. to repair a partial ``record_fts``).
    - no new records embedded → no (the FTS corpus text is unchanged).
    - ``throttle`` (periodic sweep) → only if ``min_interval`` has elapsed since
      the last rebuild, so a fast sync cadence does not re-trigger the costly
      rebuild every cycle.
    - else (manual /api/sync) → yes: rebuild on any new records, no throttle.
    """
    if force:
        return True
    if embedded <= 0:
        return False
    if not throttle:
        return True
    if last is None:
        return True
    return (now - last) >= min_interval


async def _rebuild_fts_async(*, chunk_size: int = 5000) -> int:
    """Rebuild the BM25 FTS index with the expensive parse OFF the DB thread.

    Three phases (pebble is-e10):
      1. DB thread (fast): fetch every ``(uuid, raw)`` from ``records``.
      2. OFF thread (``asyncio.to_thread``, chunked): the ``json.loads`` +
         ``record_text`` projection — the ~45-min CPU loop that previously ran
         on the DB thread. Chunking bounds peak memory and lets the event loop
         interleave other requests between chunks.
      3. DB thread (atomic): DELETE + INSERT + ``create_fts_index`` in ONE
         transaction (:func:`core.write_fts_index`).

    Returns the indexed row count. The DB thread is touched only by the fetch
    and the transactional write; the multi-minute Python parse no longer blocks
    it. The create_fts_index C++ build in phase 3 is the residual DB-thread
    block — far smaller than the parse, throttled (not eliminated) at scale.
    """
    rows = await _db_execute(fetch_record_raws)
    projection: list[tuple[str, str]] = []
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        projection.extend(await asyncio.to_thread(project_record_texts, chunk))
        # Yield so queued requests interleave between parse chunks.
        await asyncio.sleep(0)
    return await _db_execute(partial(write_fts_index, projection=projection))


def _max_embedded_uuid(db: duckdb.DuckDBPyConnection) -> str:
    """Highest uuid already in record_embeddings ('' when empty) — keyset start."""
    row = db.execute("SELECT max(uuid) FROM record_embeddings").fetchone()
    return row[0] if row and row[0] is not None else ""


def _fetch_unembedded_batch(
    db: duckdb.DuckDBPyConnection, *, after: str, limit: int
) -> list[tuple[str, str]]:
    """Next batch of (uuid, raw) with uuid > cursor — a PK range scan.

    Keyset pagination by the uuid primary key, NOT a ``LEFT JOIN
    record_embeddings ... ORDER BY uuid`` anti-join: the anti-join re-sorts the
    whole corpus every batch (measured 3x slower). Because the cursor starts at
    max(record_embeddings.uuid) and the records scan is uuid-ordered, ``uuid >
    cursor`` is exactly the not-yet-embedded tail.
    """
    return db.execute(
        "SELECT uuid, raw FROM records WHERE uuid > ? ORDER BY uuid LIMIT ?",
        [after, limit],
    ).fetchall()


def _insert_embeddings_batch(
    db: duckdb.DuckDBPyConnection, *, rows: list[tuple[str, list[float], int]]
) -> None:
    """Insert a batch of (uuid, embedding, embedded_at). Idempotent."""
    db.executemany("INSERT OR IGNORE INTO record_embeddings VALUES (?, ?, ?)", rows)


async def _run_backfill_async(*, batch_size: int = 256) -> dict[str, int | str]:
    """Embed every not-yet-embedded record, then rebuild the FTS index once.

    Returns ``{"embedded": N, "fts_rows": M}`` on completion, or
    ``{"status": "already_running"}`` if a backfill is already in flight.

    The HNSW vector index is kept in place (incremental inserts) so vector
    search keeps working during the backfill; a modest batch_size keeps the
    per-batch HNSW maintenance small.
    """
    global _backfill_running, _last_fts_rebuild_at
    if _backfill_running:
        return {"status": "already_running"}
    _backfill_running = True
    try:
        cursor = await _db_execute(_max_embedded_uuid)
        embedded = 0
        batches = 0
        start = time.time()

        def _log(msg: str) -> None:
            print(f"[ingest-sessions] backfill: {msg}", file=sys.stderr)

        while True:
            rows = await _db_execute(
                partial(_fetch_unembedded_batch, after=cursor, limit=batch_size)
            )
            if not rows:
                break
            texts = [record_text(json.loads(raw)) for _uuid, raw in rows]
            # Embedding is the expensive, GIL-releasing work — keep it OFF the
            # DB thread and the event loop.
            vectors = await asyncio.to_thread(embed_texts, texts)
            now_ms = int(time.time() * 1000)
            params = [
                (uuid, vector, now_ms)
                for (uuid, _raw), vector in zip(rows, vectors, strict=True)
            ]
            await _db_execute(partial(_insert_embeddings_batch, rows=params))
            cursor = rows[-1][0]
            embedded += len(rows)
            batches += 1
            if batches % 20 == 0:
                rate = embedded / max(1e-6, time.time() - start)
                _log(f"embedded={embedded} ({rate:.0f}/s, batch {batches})")
            # Yield so other requests interleave between batches.
            await asyncio.sleep(0)

        # Backfill is the operator-driven repair path: ALWAYS rebuild FTS
        # (off-thread, atomic), even when 0 records were embedded — this is how
        # a partial/inconsistent record_fts gets repaired to records-count
        # (pebble is-e10). Reset the throttle clock so the periodic sweep does
        # not immediately rebuild again.
        fts_rows = await _rebuild_fts_async()
        _last_fts_rebuild_at = time.monotonic()
        _log(f"done: embedded={embedded} fts_rows={fts_rows}")
        return {"embedded": embedded, "fts_rows": fts_rows}
    finally:
        _backfill_running = False


# ---------------------------------------------------------------------------
# Automatic ongoing sync (is-565.5): keep record_embeddings + the FTS index in
# sync with `records` automatically, WITHOUT downtime. Ingestion
# (_ingest_file_full / _ingest_all / the watchdog) writes raw rows into
# `records` but does NOT embed them, so the embedding/FTS sidecars drift as new
# sessions arrive and retrieval silently loses recall on new content. A
# periodic background sweep closes that gap.
#
# The sweep uses an ANTI-JOIN (records LEFT JOIN record_embeddings WHERE NULL),
# NOT the keyset-from-max(uuid) pagination the bulk backfill uses. Record uuids
# are RANDOM, so a record ingested with a uuid BELOW the keyset cursor after the
# cursor already passed is never revisited by keyset pagination — re-running the
# backfill still misses it. The anti-join finds every unembedded record
# regardless of uuid ordering, so coverage is eventually-consistent by
# construction: any newly-ingested record gets embedded within one interval.
#
# The sweep shares the _backfill_running guard with the bulk backfill so a sync
# and a manual backfill never run concurrently (DuckDB single writer; avoids
# double-embed + HNSW thrash). Embedding runs OFF the DB thread via
# asyncio.to_thread (onnxruntime releases the GIL).
# ---------------------------------------------------------------------------


def _fetch_unembedded_antijoin(
    db: duckdb.DuckDBPyConnection, *, limit: int
) -> list[tuple[str, str]]:
    """Next batch of (uuid, raw) for records lacking an embedding — anti-join.

    Unlike the keyset PK range scan in ``_fetch_unembedded_batch``, this catches
    ANY unembedded record regardless of where its (random) uuid sorts: a record
    ingested with a uuid below an already-embedded uuid — invisible to
    ``uuid > cursor`` keyset pagination — is found here. Each sweep iteration
    inserts the embeddings for the batch it fetched, so the next anti-join
    excludes them and the loop terminates.
    """
    return db.execute(
        "SELECT r.uuid, r.raw FROM records r "
        "LEFT JOIN record_embeddings e ON r.uuid = e.uuid "
        "WHERE e.uuid IS NULL LIMIT ?",
        [limit],
    ).fetchall()


def _fetch_unembedded_summaries_antijoin(
    db: duckdb.DuckDBPyConnection, *, limit: int
) -> list[tuple[str, str]]:
    """Next batch of (summary_id, content) for summaries lacking an embedding.

    The summary-tier counterpart of ``_fetch_unembedded_antijoin`` (is-565.4).
    Summaries are produced by the summarize DAG over time, so the same sweep
    that keeps ``record_embeddings`` current also keeps ``summary_embeddings``
    current — the summary trust-tier becomes a retrieval candidate source
    eventually-consistently, within one sweep interval of a summary's creation.
    """
    return db.execute(
        "SELECT s.summary_id, s.content FROM summaries s "
        "LEFT JOIN summary_embeddings e ON s.summary_id = e.summary_id "
        "WHERE e.summary_id IS NULL LIMIT ?",
        [limit],
    ).fetchall()


def _insert_summary_embeddings_batch(
    db: duckdb.DuckDBPyConnection, *, rows: list[tuple[str, list[float], int]]
) -> None:
    """Insert a batch of (summary_id, embedding, embedded_at). Idempotent."""
    db.executemany("INSERT OR IGNORE INTO summary_embeddings VALUES (?, ?, ?)", rows)


async def _sync_summary_embeddings(batch_size: int) -> int:
    """Embed every summary missing an embedding (anti-join). Returns count.

    Mirrors the record sweep: fetch unembedded summaries via anti-join, embed
    OFF the DB thread (``asyncio.to_thread`` — onnxruntime releases the GIL),
    write the embeddings back through the single DB thread. Summaries have NO
    FTS arm (the BM25 index is records-only); they are a vector-search tier, so
    no index rebuild is needed here.
    """
    embedded = 0
    while True:
        rows = await _db_execute(
            partial(_fetch_unembedded_summaries_antijoin, limit=batch_size)
        )
        if not rows:
            break
        texts = [summary_text({"content": content}) for _sid, content in rows]
        vectors = await asyncio.to_thread(embed_texts, texts)
        now_ms = int(time.time() * 1000)
        params = [
            (sid, vector, now_ms)
            for (sid, _content), vector in zip(rows, vectors, strict=True)
        ]
        await _db_execute(partial(_insert_summary_embeddings_batch, rows=params))
        embedded += len(rows)
        await asyncio.sleep(0)
    return embedded


async def _run_sync_async(
    *,
    batch_size: int = 256,
    throttle_fts: bool = False,
    force_fts: bool = False,
    now: float | None = None,
) -> dict[str, int | bool | str]:
    """Embed every record + summary missing an embedding, maybe rebuild FTS.

    Returns ``{"embedded": N, "summaries_embedded": M, "fts_rebuilt": bool}`` on
    completion, or ``{"status": "already_running"}`` if a backfill/sync is
    already in flight (shares the bulk backfill's guard — only one embedding
    writer at a time).

    One sweep keeps BOTH the ``record_embeddings`` and ``summary_embeddings``
    sidecars in sync with their source tables (is-565.4): records via the
    anti-join below, summaries via ``_sync_summary_embeddings``. The HNSW vector
    indexes stay in place (incremental inserts) so search keeps working.

    The FTS rebuild (the expensive snapshot refresh) is gated by
    :func:`_should_rebuild_fts` (pebble is-e10):

    - ``throttle_fts`` (the periodic ``_sync_loop``): rebuild iff this run
      embedded RECORD rows AND ``INGEST_SESSIONS_FTS_MIN_INTERVAL`` has elapsed
      since the last rebuild — so a 5-min embedding cadence does not re-trigger
      the costly rebuild every cycle.
    - default / manual ``/api/sync`` (``throttle_fts=False``): rebuild iff this
      run embedded RECORD rows (the pre-is-e10 contract, no throttle).
    - ``force_fts``: always rebuild (operator repair path).

    The rebuild itself runs OFF the DB thread (``_rebuild_fts_async``). ``now``
    is injectable for deterministic throttle tests (defaults to ``monotonic``).
    """
    global _backfill_running, _last_fts_rebuild_at
    if _backfill_running:
        return {"status": "already_running"}
    _backfill_running = True
    try:
        embedded = 0
        while True:
            rows = await _db_execute(
                partial(_fetch_unembedded_antijoin, limit=batch_size)
            )
            if not rows:
                break
            texts = [record_text(json.loads(raw)) for _uuid, raw in rows]
            # Expensive, GIL-releasing — keep OFF the DB thread and event loop.
            vectors = await asyncio.to_thread(embed_texts, texts)
            now_ms = int(time.time() * 1000)
            params = [
                (uuid, vector, now_ms)
                for (uuid, _raw), vector in zip(rows, vectors, strict=True)
            ]
            await _db_execute(partial(_insert_embeddings_batch, rows=params))
            embedded += len(rows)
            # Yield so other requests interleave between batches.
            await asyncio.sleep(0)

        summaries_embedded = await _sync_summary_embeddings(batch_size)

        clock = time.monotonic() if now is None else now
        fts_rebuilt = False
        if _should_rebuild_fts(
            embedded=embedded,
            throttle=throttle_fts,
            force=force_fts,
            now=clock,
            last=_last_fts_rebuild_at,
            min_interval=_fts_min_interval(),
        ):
            await _rebuild_fts_async()
            _last_fts_rebuild_at = clock
            fts_rebuilt = True
        return {
            "embedded": embedded,
            "summaries_embedded": summaries_embedded,
            "fts_rebuilt": fts_rebuilt,
        }
    finally:
        _backfill_running = False


def _sync_interval() -> float:
    """Seconds between automatic sync sweeps (env override, default 300)."""
    return float(os.environ.get("INGEST_SESSIONS_SYNC_INTERVAL", "300"))


async def _sync_loop(interval: float) -> None:
    """Periodic driver: every `interval`s, run an anti-join sync sweep.

    This is a poll-for-new-work loop (each tick is a fresh sweep of whatever
    is currently unembedded), NOT a retry loop — it never re-attempts the same
    failed operation, it advances to the next interval's work. A sweep failure
    is logged loudly and the schedule continues, so one transient embedding
    error never silently disables ongoing sync. CancelledError propagates so
    shutdown stops the loop cleanly.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            # Periodic sweep: THROTTLE the FTS rebuild so a fast embedding
            # cadence does not re-trigger the costly rebuild every cycle
            # (pebble is-e10). Manual /api/sync + /api/backfill are not throttled.
            result = await _run_sync_async(throttle_fts=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — log + advance to next sweep
            print(f"[ingest-sessions] sync sweep failed: {exc}", file=sys.stderr)
            continue
        if result.get("embedded") or result.get("fts_rebuilt"):
            print(f"[ingest-sessions] sync: {result}", file=sys.stderr)


def _start_sync_task() -> asyncio.Task[None]:
    """Fire-and-forget the periodic sync driver (does NOT block startup)."""
    return asyncio.ensure_future(_sync_loop(_sync_interval()))


async def _stop_sync_task(task: asyncio.Task[None]) -> None:
    """Cancel the periodic sync driver and await its clean teardown."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# Database thread
# ---------------------------------------------------------------------------


def _db_loop() -> None:
    """Single database thread.  Owns the connection, drains the queue."""
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(path)
    try:
        create_tables(db)
        register_functions(db)
        _ingest_all(db)
        _startup_done.set()

        while True:
            req = _db_queue.get()
            if req is None:
                break
            try:
                req.result = req.fn(db)
            except Exception as exc:
                req.error = exc
            req.done.set()
            if req.log_errors and req.error:
                print(
                    f"[ingest-sessions] background db error: {req.error}",
                    file=sys.stderr,
                )
    finally:
        db.close()


def _start_db_thread() -> threading.Thread:
    """Start the database thread and wait for initial ingestion to finish."""
    t = threading.Thread(target=_db_loop, daemon=True, name="db-thread")
    t.start()
    _startup_done.wait()
    return t


def _stop_db_thread() -> None:
    """Send shutdown sentinel and wait for database thread to exit."""
    _db_queue.put(None)
    if _db_thread is not None:
        _db_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


class _JsonlHandler(FileSystemEventHandler):
    """Watchdog handler that enqueues new/modified JSONL files."""

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        self._handle(event)

    def _handle(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        path = Path(str(event.src_path))
        if path.suffix != ".jsonl":
            return
        captured = path
        req = _db_submit(lambda db: _ingest_file_full(db, captured))
        req.log_errors = True


def _start_watcher() -> BaseObserver:
    """Start watchdog observer on the projects directory."""
    projects_dir = _projects_dir()
    projects_dir.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(_JsonlHandler(), str(projects_dir), recursive=True)
    observer.daemon = True
    observer.start()
    return observer


def _warm_models() -> None:
    """Load the embed + rerank ONNX models so the first retrieve call is warm.

    The SessionStart retrieve hook (hooks/session_retrieve.py) calls
    /api/retrieve under a ~15s budget; a cold first call loads ~180MB of ONNX
    models (~60s) and would time out. Front-load them in a background daemon
    thread at startup so the hook is reliably ~2s. Best-effort: a failure here
    just means the first real call pays the cold load — never fatal to startup.
    """
    try:
        from ingest_sessions.embeddings import embed_query
        from ingest_sessions.rerank import rerank

        embed_query("warm")
        rerank("warm", ["warm"])
        print("[ingest-sessions] retrieval models warmed", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 — warming is best-effort, never fatal
        print(f"[ingest-sessions] model warm failed: {exc}", file=sys.stderr)


def _startup() -> BaseObserver | None:
    """Run on server start: start database thread, start watcher."""
    global _db_thread
    _startup_done.clear()
    _db_thread = _start_db_thread()
    return _start_watcher()


def _start_warm_thread() -> None:
    """Front-load the retrieval models in the background (HTTP server only).

    Only the long-lived HTTP server serves the SessionStart retrieve hook, so
    warming belongs there — not in the stdio/in-process path used by tests,
    where the extra startup CPU would contend with timing-sensitive sweeps.
    """
    threading.Thread(target=_warm_models, daemon=True, name="warm-models").start()


def _shutdown(observer: BaseObserver | None) -> None:
    """Clean shutdown: stop watcher, stop database thread."""
    if observer:
        observer.stop()
        observer.join(timeout=2)
    _stop_db_thread()


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="query",
            description=(
                "Execute a SQL query against the sessions database. "
                "Content blocks over 100KB are offloaded to a blob store "
                "and appear in records.raw as '[Large Content: file_...]' "
                "markers; rehydrate them with the SQL function "
                "read_blob(file_id). Blob metadata is in the blob_meta "
                "table; unparseable transcript lines are in malformed_lines."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to execute.",
                    },
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="refresh",
            description="Ingest a specific session JSONL file into the database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the .jsonl session file.",
                    },
                },
                "required": ["path"],
            },
        ),
        Tool(
            name="summarize",
            description=(
                "Trigger LCM summary DAG maintenance for a session. "
                "Summarizes unsummarized messages into sprigs, "
                "condenses sprigs into bindles. Uses claude --print "
                "(covered by Max). Returns counts of nodes created."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to summarize.",
                    },
                },
                "required": ["session_id"],
            },
        ),
        Tool(
            name="get_session",
            description=(
                "Fetch a FULL session by session_id: the complete transcript "
                "reassembled in chronological order with every "
                "'[Large Content: file_...]' blob marker rehydrated to its "
                "original content. This is the on-demand fetch primitive for "
                "durable session citations (a labbook/ADR session_id resolves "
                "to actual content). Unlike 'context' (a summary) this returns "
                "the raw records. Set include_blobs=false to keep compact "
                "markers; format='text' adds a rendered plaintext transcript."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to fetch.",
                    },
                    "include_blobs": {
                        "type": "boolean",
                        "description": "Rehydrate blob markers (default true).",
                    },
                    "format": {
                        "type": "string",
                        "enum": ["records", "text"],
                        "description": "Output shape (default 'records').",
                    },
                },
                "required": ["session_id"],
            },
        ),
        Tool(
            name="retrieve_relevant",
            description=(
                "Retrieve the top-k records most RELEVANT to a query. The "
                "hook-facing retrieval entry point: fuses on-device vector "
                "(semantic) + BM25 (lexical) candidate search into a bounded "
                "shortlist, then reranks it with an ONNX cross-encoder for "
                "precise relevance. Each result carries its uuid + session_id "
                "(deepen via get_session) plus rerank_score / fused_score. "
                "Returns [] when the embedding/FTS indexes have not been built."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The natural-language query to rank against.",
                    },
                    "k": {
                        "type": "integer",
                        "description": "Number of ranked results to return (default 10).",
                    },
                    "candidate_k": {
                        "type": "integer",
                        "description": (
                            "Stage-1 candidate pool size before rerank (default 40)."
                        ),
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="context",
            description=(
                "Assemble recovery context from the LCM summary DAG "
                "for a session. Returns the formatted context string "
                "suitable for injection after /clear."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to get context for.",
                    },
                },
                "required": ["session_id"],
            },
        ),
        Tool(
            name="add_supersession",
            description=(
                "Record that one record supersedes another (newer reasoning "
                "displacing older), for supersession-aware ranking. 'source' "
                "is an opaque provenance tag (e.g. 'manual', 'derived', "
                "'git-revert', 'tracker-close', 'adr-amend'). Idempotent: "
                'returns {"inserted": true} on a new link, false if it '
                "already existed. A self-link is rejected."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "superseding_id": {
                        "type": "string",
                        "description": "The record uuid that supersedes.",
                    },
                    "superseded_id": {
                        "type": "string",
                        "description": "The record uuid being superseded.",
                    },
                    "source": {
                        "type": "string",
                        "description": "Opaque provenance tag for the link.",
                    },
                },
                "required": ["superseding_id", "superseded_id", "source"],
            },
        ),
        Tool(
            name="backfill",
            description=(
                "Embed all not-yet-embedded records and rebuild the BM25 FTS "
                "index in the BACKGROUND, without blocking request serving or "
                "opening a second DB connection — so retrieval coverage fills "
                "in while the service stays up. Default returns immediately "
                "(status 'accepted'); wait=true awaits and returns "
                "{embedded, fts_rows}. Idempotent: a second run over an "
                "unchanged corpus embeds 0. Only one backfill runs at a time "
                "(a concurrent trigger returns 'already_running')."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "batch_size": {
                        "type": "integer",
                        "description": "Records embedded per model call (default 256).",
                    },
                    "wait": {
                        "type": "boolean",
                        "description": (
                            "Await completion and return counts (default false)."
                        ),
                    },
                },
            },
        ),
        Tool(
            name="sync",
            description=(
                "Run an ANTI-JOIN sync sweep: embed every record currently "
                "missing an embedding (regardless of uuid ordering — catches "
                "late low-uuid inserts the keyset backfill misses) and rebuild "
                "the BM25 FTS index if any rows were embedded. Keeps "
                "record_embeddings + FTS in sync with `records` with no "
                "downtime. The server also runs this automatically every "
                "INGEST_SESSIONS_SYNC_INTERVAL seconds; this triggers it on "
                "demand. wait=true awaits and returns {embedded, fts_rebuilt}; "
                "a concurrent trigger returns 'already_running'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "batch_size": {
                        "type": "integer",
                        "description": "Records embedded per model call (default 256).",
                    },
                    "wait": {
                        "type": "boolean",
                        "description": (
                            "Await completion and return counts (default false)."
                        ),
                    },
                    "force_fts": {
                        "type": "boolean",
                        "description": (
                            "Force a clean FTS rebuild even with no new records "
                            "(repair a partial/inconsistent record_fts; default "
                            "false)."
                        ),
                    },
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    if name == "query":
        sql = arguments["sql"]

        def do_query(db: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
            result = db.execute(sql)
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

        try:
            rows = await _db_execute(do_query)
            return [TextContent(type="text", text=json.dumps(rows, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "refresh":
        jsonl_path = Path(arguments["path"])
        if not jsonl_path.exists():
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": f"File not found: {jsonl_path}"}),
                )
            ]
        try:
            count = await _db_execute(lambda db: _ingest_file_full(db, jsonl_path))
            return [TextContent(type="text", text=json.dumps({"processed": count}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "summarize":
        session_id = arguments["session_id"]
        result_data = await _run_summarize_async(session_id)
        return [TextContent(type="text", text=json.dumps(result_data))]

    if name == "get_session":
        session_id = arguments["session_id"]
        include_blobs = arguments.get("include_blobs", True)
        fmt = arguments.get("format", "records")
        try:
            payload = await _db_execute(
                partial(
                    _get_session_payload,
                    session_id=session_id,
                    include_blobs=include_blobs,
                    fmt=fmt,
                )
            )
            return [TextContent(type="text", text=json.dumps(payload, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "retrieve_relevant":
        query = arguments["query"]
        k = arguments.get("k", 10)
        candidate_k = arguments.get("candidate_k", 40)
        try:
            results = await _db_execute(
                partial(retrieve_relevant, query=query, k=k, candidate_k=candidate_k)
            )
            return [TextContent(type="text", text=json.dumps(results, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "context":
        session_id = arguments["session_id"]
        try:
            ctx = await _db_execute(
                partial(assemble_context_for_session, session_id=session_id)
            )
            return [TextContent(type="text", text=json.dumps({"context": ctx}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "add_supersession":
        try:
            inserted = await _db_execute(
                partial(
                    add_supersession,
                    superseding_id=arguments["superseding_id"],
                    superseded_id=arguments["superseded_id"],
                    source=arguments["source"],
                )
            )
            return [TextContent(type="text", text=json.dumps({"inserted": inserted}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "backfill":
        batch_size = arguments.get("batch_size", 256)
        if arguments.get("wait"):
            result = await _run_backfill_async(batch_size=batch_size)
            return [TextContent(type="text", text=json.dumps(result))]

        async def _bg_backfill() -> None:
            try:
                result = await _run_backfill_async(batch_size=batch_size)
                print(f"[ingest-sessions] backfill: {result}", file=sys.stderr)
            except Exception as exc:
                print(f"[ingest-sessions] backfill failed: {exc}", file=sys.stderr)

        asyncio.ensure_future(_bg_backfill())
        return [TextContent(type="text", text=json.dumps({"status": "accepted"}))]

    if name == "sync":
        batch_size = arguments.get("batch_size", 256)
        # force_fts=true forces a clean FTS rebuild even with no new records —
        # the operator repair path for a partial/inconsistent record_fts (is-e10).
        force_fts = bool(arguments.get("force_fts", False))
        if arguments.get("wait"):
            result = await _run_sync_async(batch_size=batch_size, force_fts=force_fts)
            return [TextContent(type="text", text=json.dumps(result))]

        async def _bg_sync() -> None:
            try:
                result = await _run_sync_async(
                    batch_size=batch_size, force_fts=force_fts
                )
                print(f"[ingest-sessions] sync: {result}", file=sys.stderr)
            except Exception as exc:
                print(f"[ingest-sessions] sync failed: {exc}", file=sys.stderr)

        asyncio.ensure_future(_bg_sync())
        return [TextContent(type="text", text=json.dumps({"status": "accepted"}))]

    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# MCP resources
# ---------------------------------------------------------------------------

SCHEMA_URI = "ingest-sessions://schema"


@server.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri=AnyUrl(SCHEMA_URI),
            name="Database Schema",
            description="Table definitions for the sessions database.",
            mimeType="text/plain",
        ),
    ]


@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    if str(uri) == SCHEMA_URI:

        def get_schema(db: duckdb.DuckDBPyConnection) -> str:
            tables = db.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
            parts = []
            for (table_name,) in tables:
                parts.append(f"## {table_name}")
                cols = db.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = ? ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()
                for col_name, col_type in cols:
                    parts.append(f"  {col_name} {col_type}")
                parts.append("")
            return "\n".join(parts)

        return await _db_execute(get_schema)

    raise ValueError(f"Unknown resource: {uri}")


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


async def run_stdio() -> None:
    """Run in stdio mode (single client, good for testing)."""
    observer = _startup()
    sync_task = _start_sync_task()
    try:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())
    finally:
        await _stop_sync_task(sync_task)
        _shutdown(observer)


def run_http(host: str = "127.0.0.1", port: int | None = None) -> None:
    """Run as a streamable-HTTP server (multi-client)."""
    from collections.abc import AsyncIterator

    import uvicorn
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.types import Receive, Scope, Send

    if port is None:
        port = int(os.environ.get("INGEST_SESSIONS_PORT", str(DEFAULT_PORT)))

    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    observer: BaseObserver | None = None

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        nonlocal observer
        observer = _startup()
        _start_warm_thread()
        sync_task = _start_sync_task()
        async with session_manager.run():
            try:
                yield
            finally:
                await _stop_sync_task(sync_task)
                _shutdown(observer)

    async def handle_context_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the SessionStart hook.

        POST /api/context with {"project_dir": "..."} or {"session_id": "..."}.
        Returns {"context": "..."} or {"context": null}.
        Bypasses MCP protocol — the hook is a plain HTTP client.
        """
        body = await request.json()
        session_id = body.get("session_id")

        if not session_id:
            project_dir = body.get("project_dir", "")
            if project_dir:
                session_id = await _db_execute(
                    partial(get_latest_summarized_session, project_dir=project_dir)
                )

        if not session_id:
            return starlette.responses.JSONResponse({"context": None})

        ctx = await _db_execute(
            partial(assemble_context_for_session, session_id=session_id)
        )
        return starlette.responses.JSONResponse({"context": ctx})

    async def handle_refresh_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the PreCompact hook (step 1: ingest new messages).

        POST /api/refresh with {"path": "/path/to/session.jsonl"}.
        Returns {"processed": N} or {"error": "..."}.
        """
        body = await request.json()
        jsonl_path = Path(body.get("path", ""))
        if not jsonl_path.exists():
            return starlette.responses.JSONResponse(
                {"error": f"File not found: {jsonl_path}"}, status_code=404
            )
        count = await _db_execute(lambda db: _ingest_file_full(db, jsonl_path))
        return starlette.responses.JSONResponse({"processed": count})

    async def handle_summarize_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the PreCompact hook (step 2: run DAG maintenance).

        POST /api/summarize with {"session_id": "..."}.
        Returns 202 immediately; summarization runs in the background.

        The hook caller does not need to wait for LLM calls to finish.
        """
        body = await request.json()
        session_id = body.get("session_id", "")
        if not session_id:
            return starlette.responses.JSONResponse(
                {"error": "session_id required"}, status_code=400
            )

        if body.get("wait"):
            result = await _run_summarize_async(session_id)
            return starlette.responses.JSONResponse(result)

        async def _bg_summarize() -> None:
            try:
                result = await _run_summarize_async(session_id)
                print(
                    f"[ingest-sessions] summarize {session_id}: {result}",
                    file=sys.stderr,
                )
            except Exception as exc:
                print(
                    f"[ingest-sessions] summarize {session_id} failed: {exc}",
                    file=sys.stderr,
                )

        asyncio.ensure_future(_bg_summarize())
        return starlette.responses.JSONResponse(
            {"status": "accepted", "session_id": session_id}, status_code=202
        )

    async def handle_session_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for full-session fetch (pebble is-565.1).

        POST /api/session with {"session_id": "...", "include_blobs": bool,
        "format": "records"|"text"}.  Returns the reassembled transcript with
        blob markers rehydrated.
        """
        body = await request.json()
        session_id = body.get("session_id", "")
        if not session_id:
            return starlette.responses.JSONResponse(
                {"error": "session_id required"}, status_code=400
            )
        include_blobs = body.get("include_blobs", True)
        fmt = body.get("format", "records")
        payload = await _db_execute(
            partial(
                _get_session_payload,
                session_id=session_id,
                include_blobs=include_blobs,
                fmt=fmt,
            )
        )
        return starlette.responses.JSONResponse(payload)

    async def handle_retrieve_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for relevance retrieval (pebble is-565.2c).

        POST /api/retrieve with {"query": "...", "k"?, "candidate_k"?}.
        Returns the top-k cross-encoder-reranked records (each with uuid /
        session_id / rerank_score / fused_score) — the hook-facing retrieval
        entry point.  Returns [] when the embedding/FTS indexes are empty.
        """
        body = await request.json()
        query = body.get("query", "")
        if not query:
            return starlette.responses.JSONResponse(
                {"error": "query required"}, status_code=400
            )
        k = body.get("k", 10)
        candidate_k = body.get("candidate_k", 40)
        results = await _db_execute(
            partial(retrieve_relevant, query=query, k=k, candidate_k=candidate_k)
        )
        return starlette.responses.JSONResponse(results)

    async def handle_supersessions_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for supersession-link ingest (pebble is-565.3).

        POST /api/supersessions with either a single link object
        ``{"superseding_id", "superseded_id", "source"}`` or a batch
        ``{"links": [{...}, ...]}``.  Returns ``{"inserted": N}`` — the
        count of newly-inserted rows.  This is the seam consumer-side
        adapters (git-revert / tracker-close / ADR-amend detectors) feed.
        """
        body = await request.json()
        raw_links = body["links"] if "links" in body else [body]
        try:
            links = [
                (link["superseding_id"], link["superseded_id"], link["source"])
                for link in raw_links
            ]
        except (KeyError, TypeError) as exc:
            return starlette.responses.JSONResponse(
                {"error": f"malformed supersession link: {exc}"}, status_code=400
            )
        try:
            inserted = await _db_execute(partial(add_supersessions, links=links))
        except ValueError as exc:
            return starlette.responses.JSONResponse(
                {"error": str(exc)}, status_code=400
            )
        return starlette.responses.JSONResponse({"inserted": inserted})

    async def handle_backfill_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for in-server background backfill (pebble is-565.2).

        POST /api/backfill with optional {"batch_size": int, "wait": bool}.
        Default launches the driver in the background and returns 202
        {"status": "accepted"}; wait=true awaits and returns
        {"embedded", "fts_rows"} (used by tests / operational one-shots).
        A concurrent trigger while a run is in flight returns 409
        {"status": "already_running"} — never starts a parallel run.
        """
        body = await request.json()
        batch_size = int(body.get("batch_size", 256))

        if body.get("wait"):
            result = await _run_backfill_async(batch_size=batch_size)
            status = 409 if result.get("status") == "already_running" else 200
            return starlette.responses.JSONResponse(result, status_code=status)

        if _backfill_running:
            return starlette.responses.JSONResponse(
                {"status": "already_running"}, status_code=409
            )

        async def _bg_backfill() -> None:
            try:
                result = await _run_backfill_async(batch_size=batch_size)
                print(f"[ingest-sessions] backfill: {result}", file=sys.stderr)
            except Exception as exc:
                print(f"[ingest-sessions] backfill failed: {exc}", file=sys.stderr)

        asyncio.ensure_future(_bg_backfill())
        return starlette.responses.JSONResponse(
            {"status": "accepted", "batch_size": batch_size}, status_code=202
        )

    async def handle_sync_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for an on-demand anti-join sync sweep (is-565.5).

        POST /api/sync with optional {"batch_size": int, "wait": bool}.
        Default launches the sweep in the background and returns 202
        {"status": "accepted"}; wait=true awaits and returns
        {"embedded", "fts_rebuilt"} (used by tests / operational one-shots).
        A concurrent trigger while a backfill/sync is in flight returns 409
        {"status": "already_running"}. The server also runs this sweep
        automatically every INGEST_SESSIONS_SYNC_INTERVAL seconds.
        """
        body = await request.json()
        batch_size = int(body.get("batch_size", 256))
        # force_fts=true forces a clean FTS rebuild even with no new records —
        # the operator repair path for a partial/inconsistent record_fts (is-e10).
        force_fts = bool(body.get("force_fts", False))

        if body.get("wait"):
            result = await _run_sync_async(batch_size=batch_size, force_fts=force_fts)
            status = 409 if result.get("status") == "already_running" else 200
            return starlette.responses.JSONResponse(result, status_code=status)

        if _backfill_running:
            return starlette.responses.JSONResponse(
                {"status": "already_running"}, status_code=409
            )

        async def _bg_sync() -> None:
            try:
                result = await _run_sync_async(
                    batch_size=batch_size, force_fts=force_fts
                )
                print(f"[ingest-sessions] sync: {result}", file=sys.stderr)
            except Exception as exc:
                print(f"[ingest-sessions] sync failed: {exc}", file=sys.stderr)

        asyncio.ensure_future(_bg_sync())
        return starlette.responses.JSONResponse(
            {"status": "accepted", "batch_size": batch_size}, status_code=202
        )

    import starlette.requests
    import starlette.responses
    from starlette.routing import Route

    app = Starlette(
        routes=[
            Mount("/mcp", app=handle_mcp),
            Route("/api/context", handle_context_api, methods=["POST"]),
            Route("/api/refresh", handle_refresh_api, methods=["POST"]),
            Route("/api/summarize", handle_summarize_api, methods=["POST"]),
            Route("/api/session", handle_session_api, methods=["POST"]),
            Route("/api/retrieve", handle_retrieve_api, methods=["POST"]),
            Route("/api/supersessions", handle_supersessions_api, methods=["POST"]),
            Route("/api/backfill", handle_backfill_api, methods=["POST"]),
            Route("/api/sync", handle_sync_api, methods=["POST"]),
        ],
        lifespan=lifespan,
    )

    uvicorn.run(app, host=host, port=port)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def cli() -> None:
    """Entry point for ``ingest-sessions-server``.

    Usage::

        ingest-sessions-server              # streamable-HTTP on 127.0.0.1:8741
        ingest-sessions-server --stdio      # stdio (single client / testing)
        ingest-sessions-server --port 9000  # custom port
        ingest-sessions-server --host 0.0.0.0  # listen on all interfaces
    """
    import argparse

    parser = argparse.ArgumentParser(description="ingest-sessions MCP server")
    parser.add_argument(
        "--stdio", action="store_true", help="Run in stdio mode (single client)"
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="HTTP listen address (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"HTTP listen port (default: {DEFAULT_PORT})",
    )
    args = parser.parse_args()

    if args.stdio:
        asyncio.run(run_stdio())
    else:
        run_http(host=args.host, port=args.port)


if __name__ == "__main__":
    cli()
