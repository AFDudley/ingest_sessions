"""Tests for blob extraction during JSONL ingestion."""

import json
from pathlib import Path

import duckdb

from ingest_sessions.blobs import generate_blob_id, read_blob
from ingest_sessions.core import ingest_jsonl


def _make_record(uuid: str, session_id: str, content: str) -> str:
    """Build a JSONL line with the given content."""
    return json.dumps(
        {
            "uuid": uuid,
            "sessionId": session_id,
            "type": "assistant",
            "timestamp": "2026-03-01T12:00:00.000Z",
            "parentUuid": None,
            "message": {"role": "assistant", "content": content},
        }
    )


def _make_block_record(uuid: str, session_id: str, blocks: list[dict]) -> str:
    """Build a JSONL line with content blocks."""
    return json.dumps(
        {
            "uuid": uuid,
            "sessionId": session_id,
            "type": "assistant",
            "timestamp": "2026-03-01T12:00:00.000Z",
            "parentUuid": None,
            "message": {"role": "assistant", "content": blocks},
        }
    )


def test_small_content_not_extracted(db: duckdb.DuckDBPyConnection, blob_root: Path):
    """Content under threshold stays inline in the record."""
    jsonl = blob_root.parent / "sess-001.jsonl"
    jsonl.write_text(_make_record("msg-1", "sess-001", "small content") + "\n")

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row is not None
    raw = json.loads(row[0])
    assert raw["message"]["content"] == "small content"


def test_large_string_content_extracted(db: duckdb.DuckDBPyConnection, blob_root: Path):
    """Large string content is replaced with a marker in the record."""
    large = "x" * 150_000
    jsonl = blob_root.parent / "sess-001.jsonl"
    jsonl.write_text(_make_record("msg-1", "sess-001", large) + "\n")

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    # Record should have a marker, not the original content
    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row is not None
    raw = json.loads(row[0])
    content = raw["message"]["content"]
    assert "[Large Content:" in content
    assert "file_" in content
    assert len(content) < 1000  # marker is compact

    # Blob should be on disk
    file_id = generate_blob_id(large)
    assert read_blob(file_id, blob_root=blob_root) == large

    # Metadata should be in DuckDB
    meta = db.execute(
        "SELECT file_id, token_count FROM blob_meta WHERE file_id = ?",
        [file_id],
    ).fetchone()
    assert meta is not None
    assert meta[0] == file_id


def test_large_text_block_extracted(db: duckdb.DuckDBPyConnection, blob_root: Path):
    """Large text block in content array is replaced with marker."""
    large_text = "y" * 150_000
    blocks = [
        {"type": "text", "text": "small preamble"},
        {"type": "text", "text": large_text},
    ]
    jsonl = blob_root.parent / "sess-001.jsonl"
    jsonl.write_text(_make_block_record("msg-1", "sess-001", blocks) + "\n")

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row is not None
    raw = json.loads(row[0])
    content_blocks = raw["message"]["content"]
    assert content_blocks[0]["text"] == "small preamble"
    assert "[Large Content:" in content_blocks[1]["text"]

    file_id = generate_blob_id(large_text)
    assert read_blob(file_id, blob_root=blob_root) == large_text


def test_large_tool_result_extracted(db: duckdb.DuckDBPyConnection, blob_root: Path):
    """Large tool_result content is replaced with marker."""
    large_result = "z" * 150_000
    blocks = [
        {"type": "tool_result", "tool_use_id": "tool-1", "content": large_result},
    ]
    jsonl = blob_root.parent / "sess-001.jsonl"
    jsonl.write_text(_make_block_record("msg-1", "sess-001", blocks) + "\n")

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row is not None
    raw = json.loads(row[0])
    content_blocks = raw["message"]["content"]
    assert "[Large Content:" in content_blocks[0]["content"]

    file_id = generate_blob_id(large_result)
    assert read_blob(file_id, blob_root=blob_root) == large_result


def test_re_ingestion_does_not_re_extract_markers(
    db: duckdb.DuckDBPyConnection, blob_root: Path
):
    """Re-ingesting a JSONL after blob extraction doesn't corrupt markers.

    On second ingest, records already exist (INSERT OR IGNORE), so the
    marker content stays as-is. The marker string is under the blob
    threshold, so _extract_blobs won't try to extract it.
    """
    large = "x" * 150_000
    jsonl = blob_root.parent / "sess-001.jsonl"
    jsonl.write_text(_make_record("msg-1", "sess-001", large) + "\n")

    # First ingestion — extracts blob, inserts marker
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row1 = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row1 is not None
    raw1 = json.loads(row1[0])
    marker = raw1["message"]["content"]
    assert "[Large Content:" in marker

    # Second ingestion of the SAME file — original large content is still
    # in the JSONL on disk, but INSERT OR IGNORE skips the duplicate uuid.
    count = ingest_jsonl(db, jsonl, blob_root=blob_root)
    assert count == 1  # line was parsed but INSERT OR IGNORE skipped it

    # Record is unchanged
    row2 = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    assert row2 is not None
    raw2 = json.loads(row2[0])
    assert raw2["message"]["content"] == marker

    # Blob is still intact
    file_id = generate_blob_id(large)
    assert read_blob(file_id, blob_root=blob_root) == large

    # Only one blob_meta entry
    meta_count = db.execute(
        "SELECT count(*) FROM blob_meta WHERE file_id = ?", [file_id]
    ).fetchone()
    assert meta_count is not None
    assert meta_count[0] == 1
