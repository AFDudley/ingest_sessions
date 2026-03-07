"""Content-addressable blob store for large message content.

Large content blocks extracted during ingestion are stored on disk
at ``~/.local/share/ingest_sessions/blobs/<aa>/<file_id>`` where
``<aa>`` is the first two hex characters of the file ID (git-style
sharding). The file ID is ``file_`` + first 16 chars of SHA-256.

Content-addressable: same content = same ID = idempotent writes.

# See docs/plans/2026-03-08-blob-store.md
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

# Volt uses 100KB / 25K tokens. We match the byte threshold.
BLOB_THRESHOLD_BYTES = 100_000


def blob_dir() -> Path:
    """Default blob storage root."""
    env = os.environ.get("INGEST_SESSIONS_BLOBS_DIR")
    if env:
        return Path(env)
    return Path.home() / ".local" / "share" / "ingest_sessions" / "blobs"


def generate_blob_id(content: str) -> str:
    """Deterministic blob ID: file_ + SHA-256(content)[:16]."""
    h = hashlib.sha256(content.encode()).hexdigest()[:16]
    return f"file_{h}"


def is_large_content(content: str) -> bool:
    """Check if content exceeds the blob threshold."""
    return len(content) > BLOB_THRESHOLD_BYTES


def _blob_path(file_id: str, blob_root: Path | None = None) -> Path:
    """Compute the sharded path for a blob."""
    root = blob_root or blob_dir()
    hex_prefix = file_id[5:7]  # first 2 hex chars after "file_"
    return root / hex_prefix / file_id


def write_blob(content: str, blob_root: Path | None = None) -> str:
    """Write content to the blob store. Returns the file_id.

    Idempotent: if the blob already exists, this is a no-op.
    """
    file_id = generate_blob_id(content)
    path = _blob_path(file_id, blob_root)
    if path.exists():
        return file_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return file_id


def read_blob(file_id: str, blob_root: Path | None = None) -> str | None:
    """Read a blob by ID. Returns None if not found."""
    path = _blob_path(file_id, blob_root)
    if not path.exists():
        return None
    return path.read_text()
