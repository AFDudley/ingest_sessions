"""Tests for the content-addressable blob store."""

from pathlib import Path

from ingest_sessions.blobs import (
    BLOB_THRESHOLD_BYTES,
    blob_dir,
    generate_blob_id,
    is_large_content,
    read_blob,
    write_blob,
)


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
