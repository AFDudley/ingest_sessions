"""Regression tests for March 2026 bugfixes.

These tests verify three fixes merged together, each targeting a different
failure mode in the ingestion pipeline.  Tests use realistic JSONL samples
but run fully in isolation (in-memory DB, tmp_path files).

Commits under test:
  9b0f308  is-a2a:  refresh returns processed:0 for appended files
  b8dc955  e37:     null byte crash in _call_claude subprocess output
  b4cd183  is-b3b:  records lost during concurrent write/read
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pytest

from ingest_sessions.core import (
    create_tables,
    file_changed,
    ingest_jsonl,
    record_file,
)
from ingest_sessions.summarize import _call_claude


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


def _record(uuid: str, **overrides: Any) -> dict[str, Any]:
    """Build a realistic JSONL record with the given uuid."""
    base = {
        "uuid": uuid,
        "sessionId": "sess-regress",
        "type": "user",
        "timestamp": "2026-03-15T12:00:00.000Z",
        "parentUuid": None,
        "message": {"role": "user", "content": f"message {uuid}"},
    }
    base.update(overrides)
    return base


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def _record_count(db: duckdb.DuckDBPyConnection) -> int:
    row = db.execute("SELECT count(*) FROM records").fetchone()
    assert row is not None
    return row[0]


# ---------------------------------------------------------------------------
# is-a2a: refresh returns processed:0 for files with appended lines
#
# Root cause: record_file() captured path.stat().st_size instead of the
# byte offset that ingest_jsonl() actually read up to.  On next refresh,
# file_changed() returned the full file size as prev_size, so ingest_jsonl
# seeked to EOF and read nothing.
# ---------------------------------------------------------------------------


class TestIsA2A_RefreshAfterAppend:
    """Verify that appending records to an already-ingested file and
    re-ingesting picks up exactly the new records."""

    def test_append_then_refresh_finds_new_records(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        jsonl = tmp_path / "sess-regress.jsonl"

        # Initial ingest: 5 records
        _write_jsonl(jsonl, [_record(f"init-{i}") for i in range(5)])
        count, bytes_read = ingest_jsonl(db, jsonl)
        assert count == 5
        record_file(db, jsonl, size_bytes=bytes_read)

        # Append 3 more records
        with open(jsonl, "a") as f:
            for i in range(3):
                f.write(json.dumps(_record(f"appended-{i}")) + "\n")
        time.sleep(0.01)  # ensure mtime changes

        # file_changed must detect the growth
        changed, prev_size = file_changed(db, jsonl)
        assert changed is True
        assert prev_size == bytes_read  # not the full file size

        # Re-ingest from recorded offset
        new_count, new_bytes = ingest_jsonl(db, jsonl, byte_offset=prev_size)
        assert new_count == 3
        assert _record_count(db) == 8

    def test_record_file_uses_explicit_size_not_stat(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """record_file(size_bytes=N) must store N, not path.stat().st_size."""
        jsonl = tmp_path / "sess-regress.jsonl"
        _write_jsonl(jsonl, [_record("r1"), _record("r2")])
        stat_size = jsonl.stat().st_size

        record_file(db, jsonl, size_bytes=42)

        row = db.execute(
            "SELECT size_bytes FROM file_mtimes WHERE file_path = ?",
            [str(jsonl)],
        ).fetchone()
        assert row is not None
        assert row[0] == 42
        assert row[0] != stat_size

    def test_ingest_jsonl_returns_byte_offset(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """ingest_jsonl must return (count, bytes_read) tuple."""
        lines = [json.dumps(_record(f"r{i}")) + "\n" for i in range(3)]
        jsonl = tmp_path / "sess-regress.jsonl"
        jsonl.write_text("".join(lines))

        count, bytes_read = ingest_jsonl(db, jsonl)
        assert count == 3
        expected_bytes = sum(len(line.encode()) for line in lines)
        assert bytes_read == expected_bytes

    def test_clean_boundary_offset_does_not_skip_first_line(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """When byte_offset is at a line boundary, the first line after
        that offset must NOT be skipped."""
        line1 = json.dumps(_record("first")) + "\n"
        line2 = json.dumps(_record("second")) + "\n"
        line3 = json.dumps(_record("third")) + "\n"
        jsonl = tmp_path / "sess-regress.jsonl"
        jsonl.write_text(line1 + line2 + line3)

        # Offset at exact end of line1 (clean boundary)
        offset = len(line1.encode())
        count, _ = ingest_jsonl(db, jsonl, byte_offset=offset)
        assert count == 2
        uuids = {r[0] for r in db.execute("SELECT uuid FROM records").fetchall()}
        assert uuids == {"second", "third"}


# ---------------------------------------------------------------------------
# is-b3b: records lost during concurrent write/read of JSONL files
#
# Root cause: f.read() could capture an incomplete trailing JSON line when
# the file is being actively written.  The partial line failed json.loads
# silently, but end_offset included those bytes.  On the next pass,
# byte_offset started past the partial line, permanently losing the record.
# ---------------------------------------------------------------------------


class TestIsB3B_ConcurrentWriteRead:
    """Verify that incomplete trailing lines are not counted in end_offset,
    so they get picked up on the next incremental pass."""

    def test_partial_trailing_line_excluded_from_offset(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """Simulate a concurrent write: file has a complete record followed
        by a truncated JSON line (no trailing newline)."""
        complete = json.dumps(_record("complete-1")) + "\n"
        partial = '{"uuid": "partial-1", "sessionId": "sess-regress"'  # no closing brace, no newline

        jsonl = tmp_path / "sess-regress.jsonl"
        jsonl.write_bytes(complete.encode() + partial.encode())

        count, bytes_read = ingest_jsonl(db, jsonl)

        # Only the complete record should be ingested
        assert count == 1
        # bytes_read should NOT include the partial line's bytes
        assert bytes_read == len(complete.encode())

    def test_partial_line_recovered_on_second_pass(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """After the partial line is completed, the next ingest pass
        must pick it up."""
        complete1 = json.dumps(_record("c1")) + "\n"
        partial = '{"uuid": "recovered", "sessionId": "sess-regress", "type": "user"'

        jsonl = tmp_path / "sess-regress.jsonl"
        jsonl.write_bytes(complete1.encode() + partial.encode())

        # First pass: only c1 ingested, partial excluded from offset
        count1, offset1 = ingest_jsonl(db, jsonl)
        assert count1 == 1
        record_file(db, jsonl, size_bytes=offset1)

        # Now "complete" the partial line (simulating writer finishing)
        completed_line = partial + ', "timestamp": null, "parentUuid": null}\n'
        complete2 = json.dumps(_record("c2")) + "\n"
        jsonl.write_bytes(
            complete1.encode() + completed_line.encode() + complete2.encode()
        )
        time.sleep(0.01)

        # Second pass from recorded offset
        changed, prev_size = file_changed(db, jsonl)
        assert changed is True
        count2, offset2 = ingest_jsonl(db, jsonl, byte_offset=prev_size)

        # Should pick up both the now-complete "recovered" and "c2"
        assert count2 == 2
        assert _record_count(db) == 3
        uuids = {r[0] for r in db.execute("SELECT uuid FROM records").fetchall()}
        assert {"c1", "recovered", "c2"} == uuids

    def test_entirely_partial_file_returns_zero(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        """A file containing only a partial line (no newline) should
        return 0 records and bytes_read == 0."""
        jsonl = tmp_path / "sess-regress.jsonl"
        jsonl.write_bytes(b'{"uuid": "incomplete"')

        count, bytes_read = ingest_jsonl(db, jsonl)
        assert count == 0
        assert bytes_read == 0


# ---------------------------------------------------------------------------
# ingest_sessions-e37: null byte crash in _call_claude subprocess output
#
# Root cause: subprocess.run(text=True) raises ValueError on null bytes.
# Fix: use text=False, decode with errors="replace", strip \x00.
# ---------------------------------------------------------------------------


class TestE37_NullBytesInSubprocess:
    """Verify _call_claude handles null bytes in stdout and stderr."""

    def test_null_bytes_in_stdout_stripped(self) -> None:
        """Null bytes in subprocess stdout should be replaced, not crash."""
        mock_result = type(
            "CompletedProcess",
            (),
            {
                "returncode": 0,
                "stdout": b"summary\x00of\x00session",
                "stderr": b"",
            },
        )()
        with patch(
            "ingest_sessions.summarize._find_claude", return_value="/usr/bin/claude"
        ):
            with patch("subprocess.run", return_value=mock_result):
                output = _call_claude("system prompt", "user message")

        assert "\x00" not in output
        assert "summary" in output
        assert "session" in output

    def test_null_bytes_in_stderr_on_failure(self) -> None:
        """Null bytes in stderr should not crash the error path."""
        mock_result = type(
            "CompletedProcess",
            (),
            {
                "returncode": 1,
                "stdout": b"",
                "stderr": b"fatal\x00error\x00occurred",
            },
        )()
        with patch(
            "ingest_sessions.summarize._find_claude", return_value="/usr/bin/claude"
        ):
            with patch("subprocess.run", return_value=mock_result):
                with pytest.raises(RuntimeError, match="claude --print failed"):
                    _call_claude("system prompt", "user message")

    def test_mixed_valid_and_null_output(self) -> None:
        """Output with interspersed null bytes produces clean text."""
        mock_result = type(
            "CompletedProcess",
            (),
            {
                "returncode": 0,
                "stdout": b"\x00leading\x00and\x00trailing\x00",
                "stderr": b"",
            },
        )()
        with patch(
            "ingest_sessions.summarize._find_claude", return_value="/usr/bin/claude"
        ):
            with patch("subprocess.run", return_value=mock_result):
                output = _call_claude("sys", "usr")

        assert "\x00" not in output
        assert "leading" in output
        assert "trailing" in output
