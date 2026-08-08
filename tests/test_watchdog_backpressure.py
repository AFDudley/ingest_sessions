"""Outer-loop tests for watchdog ingest backpressure.

The server serializes ALL database work onto ONE thread / ONE FIFO queue
(server.py `_db_loop`), so anything the watchdog enqueues sits directly in
front of every inbound MCP query. Three unbounded behaviours in that path let
a busy corpus starve queries indefinitely -- observed live as 33h of
`sync sweep failed: database operation exceeded the bounded wait (60.0s)`
while every db-thread sample sat in `omp.ingest_omp_jsonl`'s insert:

  1. `_submit_ingest` (was: `_JsonlHandler._handle`) enqueued one ingest per
     watchdog EVENT, so a burst grew the queue without bound;
  2. `_ingest_file_full` discarded `file_changed`'s `changed` flag, so an
     event that changed nothing still re-ingested the whole file; and
  3. `omp.ingest_omp_jsonl` re-offered EVERY row of a re-parsed transcript to
     `INSERT OR IGNORE`, paying a per-row primary-key + secondary-index probe
     against the full-corpus `records` table for rows already stored.

Each test below pins one of those three contracts.
"""

from __future__ import annotations

import json
import queue
from pathlib import Path
from typing import Any

import duckdb
import pytest
from ingest_sessions import server
from ingest_sessions.core import create_tables
from ingest_sessions.omp import ingest_omp_jsonl

HEADER = {
    "type": "session",
    "version": 3,
    "id": "sess-backpressure",
    "cwd": "/repo",
    "title": "a session",
}


def _entry(entry_id: str, parent: str | None) -> dict[str, Any]:
    return {
        "type": "message",
        "id": entry_id,
        "parentId": parent,
        "timestamp": f"2026-08-08T00:00:{int(entry_id[1:]):02d}.000Z",
        "message": {"role": "user", "content": [{"type": "text", "text": entry_id}]},
    }


def _write_omp(path: Path, count: int) -> None:
    lines = [json.dumps(HEADER)]
    parent: str | None = None
    for i in range(count):
        entry_id = f"e{i}"
        lines.append(json.dumps(_entry(entry_id, parent)))
        parent = entry_id
    path.write_text("\n".join(lines) + "\n")


def _append_omp(path: Path, entry_id: str, parent: str) -> None:
    with path.open("a") as handle:
        handle.write(json.dumps(_entry(entry_id, parent)) + "\n")


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


class _InsertCounter:
    """Connection proxy counting the rows offered to each `INSERT ... INTO`.

    The contract under test is about the WORK a re-ingest performs, and
    `INSERT OR IGNORE` makes the resulting table state identical whether or
    not already-stored rows are re-offered. The rows handed to the insert are
    therefore the observable that distinguishes the two.
    """

    def __init__(self, inner: duckdb.DuckDBPyConnection) -> None:
        self._inner = inner
        self.record_rows_offered = 0
        self.malformed_rows_offered = 0

    def executemany(self, sql: str, rows: list[Any]) -> Any:
        if "INTO records" in sql:
            self.record_rows_offered += len(rows)
        elif "INTO malformed_lines" in sql:
            self.malformed_rows_offered += len(rows)
        return self._inner.executemany(sql, rows)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class TestOmpReingestCost:
    def test_reingest_of_unchanged_file_offers_no_rows(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 20)
        counter = _InsertCounter(db)

        ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]
        assert counter.record_rows_offered == 20

        counter.record_rows_offered = 0
        ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]
        assert counter.record_rows_offered == 0

    def test_reingest_after_append_offers_only_the_appended_row(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 20)
        counter = _InsertCounter(db)
        ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]

        _append_omp(path, "e20", "e19")
        counter.record_rows_offered = 0
        count, _malformed, header = ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]

        assert counter.record_rows_offered == 1
        # The returned count still describes the FILE, not the write.
        assert count == 21
        assert header is not None
        stored = db.execute(
            "SELECT count(*) FROM records WHERE session_id = ?", [HEADER["id"]]
        ).fetchone()
        assert stored is not None and stored[0] == 21

    def test_malformed_lines_are_not_reoffered(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 3)
        with path.open("a") as handle:
            handle.write("{not json\n")
        counter = _InsertCounter(db)

        _first, malformed_first, _h = ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]
        assert malformed_first == 1
        assert counter.malformed_rows_offered == 1

        counter.malformed_rows_offered = 0
        ingest_omp_jsonl(counter, path)  # type: ignore[arg-type]
        assert counter.malformed_rows_offered == 0
        rows = db.execute("SELECT count(*) FROM malformed_lines").fetchone()
        assert rows is not None and rows[0] == 1


class TestIngestFileFullChangeGuard:
    def test_unchanged_file_is_skipped(
        self,
        db: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("INGEST_SESSIONS_BLOB_DIR", str(tmp_path / "blobs"))
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 5)

        assert server._ingest_file_full(db, path) == 5
        assert server._ingest_file_full(db, path) == 0

    def test_modified_file_is_reingested(
        self,
        db: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("INGEST_SESSIONS_BLOB_DIR", str(tmp_path / "blobs"))
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 5)
        assert server._ingest_file_full(db, path) == 5

        _append_omp(path, "e5", "e4")
        assert server._ingest_file_full(db, path) == 6


class TestSubmitIngestCoalescing:
    """A burst of events for one path must not grow the DB queue."""

    @pytest.fixture(autouse=True)
    def isolated_queue(self) -> Any:
        saved_queue = server._db_queue
        saved_pending = server._pending_ingests
        server._db_queue = queue.Queue()
        server._pending_ingests = set()
        yield
        server._db_queue = saved_queue
        server._pending_ingests = saved_pending

    def test_burst_on_one_path_enqueues_once(self, tmp_path: Path) -> None:
        path = tmp_path / "sess.jsonl"
        for _ in range(50):
            server._submit_ingest(path)
        assert server._db_queue.qsize() == 1

    def test_distinct_paths_each_enqueue(self, tmp_path: Path) -> None:
        for i in range(5):
            server._submit_ingest(tmp_path / f"s{i}.jsonl")
        assert server._db_queue.qsize() == 5

    def test_event_during_the_ingest_enqueues_a_follow_up(
        self,
        db: duckdb.DuckDBPyConnection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The pending marker clears when the ingest STARTS, not when it ends.

        An append landing while its own file is being ingested must still be
        able to queue the pass that picks it up, or its tail is lost until
        some unrelated event happens to arrive.
        """
        monkeypatch.setenv("INGEST_SESSIONS_BLOB_DIR", str(tmp_path / "blobs"))
        path = tmp_path / "sess.jsonl"
        _write_omp(path, 3)

        server._submit_ingest(path)
        req = server._db_queue.get_nowait()
        assert req is not None  # None is the shutdown sentinel, not an ingest

        observed: list[int] = []

        def _mid_ingest_event(conn: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
            server._submit_ingest(path)
            observed.append(server._db_queue.qsize())
            return 0

        monkeypatch.setattr(server, "_ingest_file_full", _mid_ingest_event)
        req.fn(db)

        assert observed == [1]
