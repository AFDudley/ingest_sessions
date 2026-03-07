"""Tests for the content-addressable blob store."""

from pathlib import Path

import duckdb

from ingest_sessions.blobs import (
    BLOB_THRESHOLD_BYTES,
    blob_dir,
    generate_blob_id,
    get_blob_meta,
    insert_blob_meta,
    is_large_content,
    read_blob,
    write_blob,
)
from ingest_sessions.core import create_tables


def test_generate_blob_id_deterministic():
    """Same content produces same ID."""
    content = "hello world" * 1000
    id1 = generate_blob_id(content)
    id2 = generate_blob_id(content)
    assert id1 == id2
    assert id1.startswith("file_")
    assert len(id1) == 5 + 16  # "file_" + 16 hex chars


def test_generate_blob_id_different_content():
    """Different content produces different IDs."""
    id1 = generate_blob_id("content A")
    id2 = generate_blob_id("content B")
    assert id1 != id2


def test_is_large_content_under_threshold():
    """Small content is not large."""
    assert is_large_content("small") is False


def test_is_large_content_over_threshold():
    """Content over 100KB is large."""
    large = "x" * (BLOB_THRESHOLD_BYTES + 1)
    assert is_large_content(large) is True


def test_write_and_read_blob(tmp_path: Path):
    """Write a blob, then read it back."""
    content = "hello world" * 5000
    file_id = generate_blob_id(content)

    write_blob(content, blob_root=tmp_path)
    result = read_blob(file_id, blob_root=tmp_path)

    assert result == content


def test_write_blob_creates_sharded_path(tmp_path: Path):
    """Blob is stored at <root>/<first 2 hex>/<file_id>."""
    content = "test content for sharding"
    file_id = generate_blob_id(content)
    hex_prefix = file_id[5:7]  # first 2 hex chars after "file_"

    write_blob(content, blob_root=tmp_path)

    expected_path = tmp_path / hex_prefix / file_id
    assert expected_path.exists()
    assert expected_path.read_text() == content


def test_write_blob_idempotent(tmp_path: Path):
    """Writing the same content twice doesn't error or change the file."""
    content = "idempotent content"
    write_blob(content, blob_root=tmp_path)
    write_blob(content, blob_root=tmp_path)

    file_id = generate_blob_id(content)
    assert read_blob(file_id, blob_root=tmp_path) == content


def test_read_blob_missing(tmp_path: Path):
    """Reading a nonexistent blob returns None."""
    result = read_blob("file_0000000000000000", blob_root=tmp_path)
    assert result is None


def test_blob_dir_default():
    """Default blob dir is under ~/.local/share/ingest_sessions/blobs."""
    d = blob_dir()
    assert d == Path.home() / ".local" / "share" / "ingest_sessions" / "blobs"


# ---------------------------------------------------------------------------
# blob_meta DuckDB tests
# ---------------------------------------------------------------------------


def test_insert_and_get_blob_meta():
    """Insert blob metadata and retrieve it."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    insert_blob_meta(
        db,
        file_id="file_abcdef0123456789",
        session_id="sess-001",
        token_count=5000,
        original_size=80000,
    )

    meta = get_blob_meta(db, "file_abcdef0123456789")
    assert meta is not None
    assert meta["file_id"] == "file_abcdef0123456789"
    assert meta["session_id"] == "sess-001"
    assert meta["token_count"] == 5000
    assert meta["original_size"] == 80000


def test_insert_blob_meta_idempotent():
    """Inserting the same file_id twice is a no-op."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    insert_blob_meta(db, "file_abcdef0123456789", "sess-001", 5000, 80000)
    insert_blob_meta(db, "file_abcdef0123456789", "sess-001", 5000, 80000)

    meta = get_blob_meta(db, "file_abcdef0123456789")
    assert meta is not None


def test_get_blob_meta_missing():
    """Getting nonexistent blob meta returns None."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    assert get_blob_meta(db, "file_0000000000000000") is None
