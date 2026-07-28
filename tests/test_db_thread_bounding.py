"""Outer-loop tests for the single DB-thread bounding fix (pebble is-189).

The server serializes ALL database work onto ONE thread / ONE connection /
ONE FIFO queue (server.py `_db_loop`). Before this fix:

  1. an expensive op (e.g. a full scan) monopolized the DB thread, so a
     trivial op queued behind it was starved — head-of-line blocking, the
     cause of the MCP query timeouts; and
  2. the connection was opened with no config, letting DuckDB default
     memory_limit to ~80% of RAM (observed 42GB RSS).

These tests drive the real `_db_loop` / `_db_submit` machinery (module
globals reset per test for isolation) and assert the bounding behaviour:

  * a long-running op is interrupted at a per-op budget so the next op runs
    within a bounded wall-clock; and
  * the live connection reports BOUNDED memory_limit + threads, not the
    ~80%-RAM / all-cores defaults.

DuckDB has no Postgres-style `SET statement_timeout`; the supported
mechanism is `DuckDBPyConnection.interrupt()` from another thread (verified
against duckdb 1.4.x), which the fix arms via a per-op timer in `_db_loop`.

A nonexistent projects dir / history file is pointed at deliberately so the
DB thread's startup ingestion (`_ingest_all`) returns immediately without any
filesystem setup — keeping these tests free of I/O boundary work.
"""

from __future__ import annotations

import queue
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pytest
from ingest_sessions import server


@contextmanager
def running_db_thread() -> Iterator[None]:
    """Start the real DB thread with fresh module globals, stop it on exit.

    `_db_submit` / `_db_loop` / `_start_db_thread` reference the module globals
    `_db_queue` / `_startup_done` by name at call time, so reassigning them
    gives per-test isolation without reloading the heavy server module. The
    isolated DB path and (nonexistent) projects/history are taken from env,
    which the caller sets before entering.
    """
    server._db_queue = queue.Queue()
    server._startup_done = threading.Event()
    server._db_thread = server._start_db_thread()
    try:
        yield
    finally:
        server._stop_db_thread()


def _set_isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> bool:
    """Point the server at an isolated DB and nonexistent projects/history.

    Returns True so callers can `assert` the setup ran — the nonexistent
    projects dir is what lets the DB thread skip startup ingestion without any
    mkdir, keeping the test's I/O surface at the DuckDB boundary only.
    """
    monkeypatch.setenv("INGEST_SESSIONS_DB", str(tmp_path / "test.duckdb"))
    monkeypatch.setenv("INGEST_SESSIONS_PROJECTS_DIR", str(tmp_path / "noproj"))
    monkeypatch.setenv("INGEST_SESSIONS_HISTORY_FILE", str(tmp_path / "none.jsonl"))
    return not (tmp_path / "noproj").exists()


def _heavy(db: duckdb.DuckDBPyConnection) -> Any:
    """A DuckDB op that runs well past any test budget (large cross join)."""
    return db.execute(
        "SELECT count(*) FROM range(100000000) a, range(100000000) b"
    ).fetchall()


def _trivial(db: duckdb.DuckDBPyConnection) -> Any:
    return db.execute("SELECT 1").fetchall()


def _read_settings(db: duckdb.DuckDBPyConnection) -> Any:
    return db.execute(
        "SELECT current_setting('memory_limit'), current_setting('threads')"
    ).fetchall()


def _parse_mem_to_bytes(value: str) -> float:
    """Parse a DuckDB current_setting('memory_limit') string to bytes."""
    num, unit = value.split()
    mult = {
        "bytes": 1,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }[unit]
    return float(num) * mult


def test_head_of_line_blocking_is_bounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A long op must not starve a trivial op queued behind it.

    With a 1s per-op budget the heavy op is interrupted, the queue drains,
    and the trivial op completes within a bounded wall-clock. On the
    unbounded (pre-fix) code the heavy op runs for minutes and the trivial
    op never completes inside the window.
    """
    assert _set_isolated_env(monkeypatch, tmp_path)
    monkeypatch.setenv("INGEST_SESSIONS_DB_OP_BUDGET_SEC", "1")

    with running_db_thread():
        heavy_req = server._db_submit(_heavy)
        # Let the DB thread pick up the heavy op before queuing the trivial one.
        time.sleep(0.2)
        trivial_req = server._db_submit(_trivial)

        start = time.monotonic()
        completed = trivial_req.done.wait(timeout=10)
        elapsed = time.monotonic() - start

        assert completed, (
            "trivial op was starved: the heavy op monopolized the DB thread "
            "(no per-op interrupt bounding the single DB thread)"
        )
        assert elapsed < 8, (
            f"trivial op took {elapsed:.1f}s — heavy op was not interrupted "
            "at the per-op budget"
        )
        assert trivial_req.result == [(1,)]
        # The heavy op surfaced an explicit error rather than hanging forever.
        assert heavy_req.error is not None


def test_connection_memory_and_threads_are_bounded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The live connection must report bounded memory_limit + threads.

    On the pre-fix no-config connect, memory_limit defaults to ~80% of RAM
    and threads to all cores. The fix passes an env-overridable, bounded
    config at connect.
    """
    assert _set_isolated_env(monkeypatch, tmp_path)
    monkeypatch.setenv("INGEST_SESSIONS_DB_MEMORY_LIMIT", "512MB")
    monkeypatch.setenv("INGEST_SESSIONS_DB_THREADS", "2")

    with running_db_thread():
        req = server._db_submit(_read_settings)
        assert req.done.wait(timeout=10)
        assert req.error is None
        mem_limit, threads = req.result[0]

    assert threads == 2, f"threads not bounded to env value: {threads}"
    assert _parse_mem_to_bytes(mem_limit) < 1024**3, (
        f"memory_limit not bounded ({mem_limit}); connect ignored config"
    )
