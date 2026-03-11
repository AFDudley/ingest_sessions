"""Unit tests for ingest_sessions.core.

Tests the shared ingestion logic: schema creation, JSONL parsing,
session metadata, history ingestion, and file-change tracking.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingest_sessions.core import (
    build_session_metadata,
    create_tables,
    derive_session_metadata,
    file_changed,
    ingest_history,
    ingest_jsonl,
    ingest_session_metadata,
    record_file,
)


def _scalar(db: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """Execute SQL and return the first column of the first row."""
    row = db.execute(sql).fetchone()
    assert row is not None
    return row[0]


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with schema initialized."""
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


SAMPLE_RECORD = {
    "uuid": "abc-123",
    "sessionId": "sess-001",
    "type": "user",
    "timestamp": "2026-03-01T12:00:00.000Z",
    "parentUuid": None,
    "message": {"role": "user", "content": "hello world"},
}


class TestCreateTables:
    def test_creates_all_tables(self, db: duckdb.DuckDBPyConnection) -> None:
        tables = [
            row[0]
            for row in db.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        ]
        assert "file_mtimes" in tables
        assert "history" in tables
        assert "records" in tables
        assert "sessions" in tables

    def test_idempotent(self, db: duckdb.DuckDBPyConnection) -> None:
        create_tables(db)  # second call should not raise


class TestIngestJsonl:
    def test_ingests_records(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        jsonl = tmp_path / "sess-001.jsonl"
        jsonl.write_text(json.dumps(SAMPLE_RECORD) + "\n")
        count, _ = ingest_jsonl(db, jsonl)
        assert count == 1
        rows = db.execute("SELECT uuid, session_id, type FROM records").fetchall()
        assert rows == [("abc-123", "sess-001", "user")]

    def test_skips_malformed_lines(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        jsonl = tmp_path / "sess-002.jsonl"
        jsonl.write_text(json.dumps(SAMPLE_RECORD) + "\nnot valid json\n\n")
        count, _ = ingest_jsonl(db, jsonl)
        assert count == 1

    def test_byte_offset_skips_earlier_content(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        r1 = {**SAMPLE_RECORD, "uuid": "r1"}
        r2 = {**SAMPLE_RECORD, "uuid": "r2"}
        r3 = {**SAMPLE_RECORD, "uuid": "r3"}
        line1 = json.dumps(r1) + "\n"
        line2 = json.dumps(r2) + "\n"
        line3 = json.dumps(r3) + "\n"
        jsonl = tmp_path / "sess-003.jsonl"
        jsonl.write_text(line1 + line2 + line3)

        # Offset into middle of line1 — readline() skips remainder of line1,
        # then reads line2 and line3
        count, _ = ingest_jsonl(db, jsonl, byte_offset=5)
        assert count == 2
        uuids = {r[0] for r in db.execute("SELECT uuid FROM records").fetchall()}
        assert uuids == {"r2", "r3"}

    def test_deduplicates_on_uuid(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        jsonl = tmp_path / "sess-001.jsonl"
        jsonl.write_text(json.dumps(SAMPLE_RECORD) + "\n")
        ingest_jsonl(db, jsonl)
        ingest_jsonl(db, jsonl)  # second ingestion
        assert _scalar(db, "SELECT count(*) FROM records") == 1

    def test_session_id_from_filename(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        record = {**SAMPLE_RECORD, "uuid": "no-session"}
        del record["sessionId"]
        jsonl = tmp_path / "my-session.jsonl"
        jsonl.write_text(json.dumps(record) + "\n")
        ingest_jsonl(db, jsonl)
        sid = _scalar(db, "SELECT session_id FROM records WHERE uuid = 'no-session'")
        assert sid == "my-session"


class TestIngestHistory:
    def test_ingests_entries(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        history = tmp_path / "history.jsonl"
        entries = [
            {"timestamp": 100, "display": "cmd1", "sessionId": "s1", "project": "p1"},
            {"timestamp": 200, "display": "cmd2", "sessionId": "s2", "project": "p1"},
        ]
        history.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        count = ingest_history(db, history)
        assert count == 2

    def test_content_filter(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        history = tmp_path / "history.jsonl"
        entries = [
            {"timestamp": 100, "display": "hello", "sessionId": "s1", "project": "p1"},
            {"timestamp": 200, "display": "world", "sessionId": "s2", "project": "p1"},
        ]
        history.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
        count = ingest_history(db, history, content_filter="hello")
        assert count == 1

    def test_skips_malformed_lines(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        history = tmp_path / "history.jsonl"
        entry = {"timestamp": 100, "display": "ok", "sessionId": "s1", "project": "p1"}
        history.write_text(json.dumps(entry) + "\nnot json\n")
        count = ingest_history(db, history)
        assert count == 1

    def test_missing_file_returns_zero(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        count = ingest_history(db, tmp_path / "nonexistent.jsonl")
        assert count == 0

    def test_deduplicates(self, db: duckdb.DuckDBPyConnection, tmp_path: Path) -> None:
        history = tmp_path / "history.jsonl"
        entry = {"timestamp": 100, "display": "cmd", "sessionId": "s1", "project": "p1"}
        history.write_text(json.dumps(entry) + "\n")
        ingest_history(db, history)
        ingest_history(db, history)
        assert _scalar(db, "SELECT count(*) FROM history") == 1


class TestBuildSessionMetadata:
    def test_reads_index_files(self, tmp_path: Path) -> None:
        proj = tmp_path / "proj1"
        proj.mkdir()
        index = {
            "entries": [
                {"sessionId": "s1", "summary": "test"},
                {"sessionId": "s2", "summary": "other"},
            ]
        }
        (proj / "sessions-index.json").write_text(json.dumps(index))
        meta = build_session_metadata([proj])
        assert "s1" in meta
        assert meta["s1"]["summary"] == "test"
        assert "s2" in meta

    def test_skips_missing_index(self, tmp_path: Path) -> None:
        proj = tmp_path / "proj1"
        proj.mkdir()
        meta = build_session_metadata([proj])
        assert meta == {}


class TestSessionMetadata:
    def test_from_index(self, db: duckdb.DuckDBPyConnection) -> None:
        meta = {
            "s1": {
                "summary": "A session",
                "firstPrompt": "hi",
                "messageCount": 3,
                "created": "2026-01-01",
                "modified": "2026-01-02",
                "gitBranch": "main",
                "projectPath": "/tmp/proj",
            }
        }
        ingest_session_metadata(db, "s1", meta)
        row = db.execute(
            "SELECT summary, first_prompt, message_count "
            "FROM sessions WHERE session_id = 's1'"
        ).fetchone()
        assert row == ("A session", "hi", 3)

    def test_derived_from_records(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        records = [
            {
                "uuid": "r1",
                "sessionId": "s1",
                "type": "user",
                "timestamp": "2026-03-01T10:00:00.000Z",
                "parentUuid": None,
                "message": {"role": "user", "content": "my prompt"},
            },
            {
                "uuid": "r2",
                "sessionId": "s1",
                "type": "assistant",
                "timestamp": "2026-03-01T10:05:00.000Z",
                "parentUuid": "r1",
            },
        ]
        jsonl = tmp_path / "s1.jsonl"
        jsonl.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        ingest_jsonl(db, jsonl)
        derive_session_metadata(db, "s1")

        row = db.execute(
            "SELECT message_count, first_prompt FROM sessions WHERE session_id = 's1'"
        ).fetchone()
        assert row is not None
        assert row[0] == 2
        assert row[1] == "my prompt"


class TestRefreshAppendedRecords:
    """Regression test: refresh must pick up records appended after initial ingest.

    The bug: record_file() captures path.stat().st_size at call time, but the
    file may have grown *during* ingest_jsonl(). On next refresh, file_changed()
    sees the mtime matches and returns the recorded size as prev_size, so
    ingest_jsonl seeks to EOF and reads nothing — even though not all records
    were ingested.
    """

    def test_refresh_picks_up_appended_records(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """Ingest 5 records, append 3 more, refresh should find the 3 new ones."""
        jsonl = tmp_path / "sess-append.jsonl"

        # Write 5 initial records
        records_initial = []
        for i in range(5):
            r = {**SAMPLE_RECORD, "uuid": f"initial-{i}"}
            records_initial.append(json.dumps(r))
        jsonl.write_text("\n".join(records_initial) + "\n")

        # Ingest the initial file
        count, bytes_read = ingest_jsonl(db, jsonl)
        assert count == 5

        # Record the file with the actual bytes read offset (not stat size)
        record_file(db, jsonl, size_bytes=bytes_read)

        # Append 3 more records (simulating writes during/after ingest)
        with open(jsonl, "a") as f:
            for i in range(3):
                r = {**SAMPLE_RECORD, "uuid": f"appended-{i}"}
                f.write(json.dumps(r) + "\n")

        time.sleep(0.01)  # ensure mtime changes

        # Refresh: file_changed should detect the change
        changed, prev_size = file_changed(db, jsonl)
        assert changed is True, "file_changed must detect appended content"

        # Ingest from prev_size offset
        new_count, _ = ingest_jsonl(db, jsonl, byte_offset=prev_size)
        assert new_count == 3, f"Expected 3 new records, got {new_count}"

        # All 8 records in DB
        total = db.execute("SELECT count(*) FROM records").fetchone()
        assert total is not None
        assert total[0] == 8

    def test_record_file_with_explicit_size(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """record_file should use explicit size_bytes when provided."""
        f = tmp_path / "test.jsonl"
        f.write_text("data\nmore data\n")
        actual_size = f.stat().st_size

        # Record with a smaller size (simulating partial read)
        record_file(db, f, size_bytes=5)

        row = db.execute(
            "SELECT size_bytes FROM file_mtimes WHERE file_path = ?",
            [str(f)],
        ).fetchone()
        assert row is not None
        assert row[0] == 5, f"Expected recorded size 5, got {row[0]}"
        assert row[0] != actual_size

    def test_ingest_jsonl_returns_bytes_read(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """ingest_jsonl should return (count, bytes_read) tuple."""
        r1 = {**SAMPLE_RECORD, "uuid": "br-1"}
        r2 = {**SAMPLE_RECORD, "uuid": "br-2"}
        line1 = json.dumps(r1) + "\n"
        line2 = json.dumps(r2) + "\n"
        jsonl = tmp_path / "sess-bytes.jsonl"
        jsonl.write_text(line1 + line2)

        result = ingest_jsonl(db, jsonl)
        # Should now return a tuple (count, bytes_read)
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        count, bytes_read = result
        assert count == 2
        assert bytes_read == len(line1.encode()) + len(line2.encode())


class TestFileChanged:
    def test_new_file_is_changed(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        f = tmp_path / "test.jsonl"
        f.write_text("data\n")
        changed, prev_size = file_changed(db, f)
        assert changed is True
        assert prev_size == 0

    def test_recorded_file_unchanged(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        f = tmp_path / "test.jsonl"
        f.write_text("data\n")
        record_file(db, f)

        changed, prev_size = file_changed(db, f)
        assert changed is False
        assert prev_size == f.stat().st_size

    def test_modified_file_is_changed(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        f = tmp_path / "test.jsonl"
        f.write_text("short\n")
        record_file(db, f)
        original_size = f.stat().st_size

        time.sleep(0.01)
        f.write_text("short\nmore data\n")

        changed, prev_size = file_changed(db, f)
        assert changed is True
        assert prev_size == original_size
