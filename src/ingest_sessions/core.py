"""Shared ingestion logic for ingest_sessions.

Single source of truth for schema, JSONL parsing, session metadata,
and history ingestion.  Both the MCP server and the batch CLI import
from here.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def migrate_history_pk(db: duckdb.DuckDBPyConnection) -> None:
    """Add primary key to history table if missing (migration for existing DBs)."""
    constraints = db.execute(
        "SELECT constraint_type FROM information_schema.table_constraints "
        "WHERE table_name = 'history' AND constraint_type = 'PRIMARY KEY'"
    ).fetchall()
    if constraints:
        return
    db.execute("CREATE TABLE history_new AS SELECT DISTINCT * FROM history")
    db.execute("DROP TABLE history")
    db.execute("""
        CREATE TABLE history (
            timestamp BIGINT,
            display VARCHAR,
            session_id VARCHAR,
            project VARCHAR,
            PRIMARY KEY (timestamp, session_id)
        )
    """)
    db.execute("INSERT OR IGNORE INTO history SELECT * FROM history_new")
    db.execute("DROP TABLE history_new")


def create_tables(db: duckdb.DuckDBPyConnection) -> None:
    """Create the schema and indexes if they don't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id VARCHAR PRIMARY KEY,
            summary VARCHAR,
            first_prompt VARCHAR,
            message_count INTEGER,
            created TIMESTAMP,
            modified TIMESTAMP,
            git_branch VARCHAR,
            project_path VARCHAR
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS records (
            uuid VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            type VARCHAR,
            timestamp TIMESTAMP,
            parent_uuid VARCHAR,
            raw JSON
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS history (
            timestamp BIGINT,
            display VARCHAR,
            session_id VARCHAR,
            project VARCHAR,
            PRIMARY KEY (timestamp, session_id)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS file_mtimes (
            file_path VARCHAR PRIMARY KEY,
            mtime_ns BIGINT,
            size_bytes BIGINT DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS summaries (
            summary_id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            kind VARCHAR NOT NULL,
            condensation_order INTEGER NOT NULL DEFAULT 1,
            content TEXT NOT NULL,
            token_count INTEGER NOT NULL,
            parent_ids VARCHAR[] DEFAULT [],
            message_uuids VARCHAR[] DEFAULT [],
            file_ids VARCHAR[] DEFAULT [],
            created_at BIGINT NOT NULL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS blob_meta (
            file_id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            token_count INTEGER NOT NULL,
            original_size INTEGER NOT NULL,
            created_at BIGINT NOT NULL
        )
    """)
    migrate_history_pk(db)
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_session_id ON records(session_id)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_records_type ON records(type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_records_timestamp ON records(timestamp)")
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_session_id ON history(session_id)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_summaries_session_id ON summaries(session_id)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_summaries_kind ON summaries(kind)")
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_blob_meta_session_id ON blob_meta(session_id)"
    )


# ---------------------------------------------------------------------------
# Session metadata
# ---------------------------------------------------------------------------


def build_session_metadata(project_dirs: list[Path]) -> dict[str, dict[str, Any]]:
    """Build a lookup of session metadata from sessions-index.json files."""
    meta: dict[str, dict[str, Any]] = {}
    for proj_dir in project_dirs:
        idx_file = proj_dir / "sessions-index.json"
        if not idx_file.exists():
            continue
        idx = json.loads(idx_file.read_text())
        for entry in idx.get("entries", []):
            meta[entry["sessionId"]] = entry
    return meta


def derive_session_metadata(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
) -> None:
    """Derive session metadata from ingested records when no index exists."""
    row = db.execute(
        """
        SELECT
            min(timestamp) AS created,
            max(timestamp) AS modified,
            count(*) AS message_count
        FROM records
        WHERE session_id = ?
        """,
        [session_id],
    ).fetchone()
    if row is None:
        return
    created, modified, message_count = row

    first_user = db.execute(
        """
        SELECT raw FROM records
        WHERE session_id = ? AND type = 'user'
        ORDER BY timestamp ASC
        LIMIT 1
        """,
        [session_id],
    ).fetchone()
    first_prompt = _extract_first_prompt(first_user[0]) if first_user else None

    db.execute(
        "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [session_id, None, first_prompt, message_count, created, modified, None, None],
    )


def _extract_first_prompt(raw_json: str) -> str | None:
    """Extract the user's first prompt text from a raw record JSON string."""
    try:
        record = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(record, dict):
        return None
    msg = record.get("message")
    if not isinstance(msg, dict):
        return None
    content = msg.get("content")
    if isinstance(content, str):
        return content[:500]
    return None


def ingest_session_metadata(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    session_meta: dict[str, dict[str, Any]],
) -> None:
    """Insert session metadata from index, or derive from records."""
    meta = session_meta.get(session_id)
    if meta:
        db.execute(
            "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                session_id,
                meta.get("summary"),
                meta.get("firstPrompt"),
                meta.get("messageCount"),
                meta.get("created"),
                meta.get("modified"),
                meta.get("gitBranch"),
                meta.get("projectPath"),
            ],
        )
    else:
        derive_session_metadata(db, session_id)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def _make_blob_marker(file_id: str, token_count: int) -> str:
    """Build the compact marker that replaces large content."""
    return f"[Large Content: {file_id}] [Tokens: ~{token_count}]"


def _extract_blobs(
    db: duckdb.DuckDBPyConnection,
    record: dict,
    session_id: str,
    blob_root: Path,
) -> None:
    """Extract large content blocks from a record, mutating it in place.

    Writes blobs to disk, records metadata in DuckDB, and replaces
    content with compact markers.
    """
    from ingest_sessions.blobs import (
        insert_blob_meta,
        is_large_content,
        write_blob,
    )

    msg = record.get("message")
    if not isinstance(msg, dict):
        return
    content = msg.get("content")
    if content is None:
        return

    # Case 1: content is a plain string
    if isinstance(content, str) and is_large_content(content):
        file_id = write_blob(content, blob_root=blob_root)
        token_count = len(content) // 4
        insert_blob_meta(db, file_id, session_id, token_count, len(content))
        msg["content"] = _make_blob_marker(file_id, token_count)
        return

    # Case 2: content is a list of blocks
    if not isinstance(content, list):
        return
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = block.get("text", "")
            if is_large_content(text):
                file_id = write_blob(text, blob_root=blob_root)
                token_count = len(text) // 4
                insert_blob_meta(db, file_id, session_id, token_count, len(text))
                block["text"] = _make_blob_marker(file_id, token_count)
        elif block.get("type") == "tool_result":
            result_content = block.get("content", "")
            if isinstance(result_content, str) and is_large_content(result_content):
                file_id = write_blob(result_content, blob_root=blob_root)
                token_count = len(result_content) // 4
                insert_blob_meta(
                    db, file_id, session_id, token_count, len(result_content)
                )
                block["content"] = _make_blob_marker(file_id, token_count)
            elif isinstance(result_content, list):
                for sub in result_content:
                    if isinstance(sub, dict) and sub.get("type") == "text":
                        text = sub.get("text", "")
                        if is_large_content(text):
                            file_id = write_blob(text, blob_root=blob_root)
                            token_count = len(text) // 4
                            insert_blob_meta(
                                db, file_id, session_id, token_count, len(text)
                            )
                            sub["text"] = _make_blob_marker(file_id, token_count)


def ingest_jsonl(
    db: duckdb.DuckDBPyConnection,
    jsonl_path: Path,
    byte_offset: int = 0,
    blob_root: Path | None = None,
) -> tuple[int, int]:
    """Ingest a single JSONL session file.

    Returns (record_count, bytes_read) where bytes_read is the byte offset
    up to which the file was actually consumed.  Callers should pass
    bytes_read to record_file() so that future incremental ingestion starts
    at the correct position — even if the file grew while we were reading.

    When byte_offset > 0, only lines after that offset are read (JSONL is
    append-only).  The first partial line after the offset is silently
    skipped to avoid parsing a truncated JSON object.

    When blob_root is provided, large content blocks are extracted to the
    blob store and replaced with compact markers before insertion into
    DuckDB.  See blobs.py for threshold and storage details.
    """
    with open(jsonl_path, "r") as f:
        if byte_offset > 0:
            f.seek(byte_offset)
            # If we landed mid-line, skip the partial remainder.
            # Check the byte just before the offset: if it's not a newline
            # (or offset is 0), we're mid-line and must skip forward.
            f.seek(byte_offset - 1)
            ch = f.read(1)
            if ch != "\n":
                f.readline()  # skip remainder of partial line
        text = f.read()
        end_offset = f.tell()

    batch = []
    session_id = jsonl_path.stem
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Extract large content blocks to blob store
        if blob_root is not None:
            _extract_blobs(db, record, session_id, blob_root)
            line = json.dumps(record)

        batch.append(
            (
                record.get("uuid", ""),
                record.get("sessionId", session_id),
                record.get("type", ""),
                record.get("timestamp"),
                record.get("parentUuid"),
                line,
            )
        )
    if batch:
        db.executemany(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )
    return len(batch), end_offset


def ingest_history(
    db: duckdb.DuckDBPyConnection,
    history_file: Path,
    content_filter: str | None = None,
) -> int:
    """Ingest history.jsonl entries.  Returns count loaded."""
    if not history_file.exists():
        return 0
    filter_lower = content_filter.lower() if content_filter else None
    batch = []
    for line in history_file.read_text().splitlines():
        if not line.strip():
            continue
        if filter_lower and filter_lower not in line.lower():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        batch.append(
            (
                entry.get("timestamp"),
                entry.get("display"),
                entry.get("sessionId"),
                entry.get("project"),
            )
        )
    if batch:
        db.executemany("INSERT OR IGNORE INTO history VALUES (?, ?, ?, ?)", batch)
    return len(batch)


# ---------------------------------------------------------------------------
# File-change tracking
# ---------------------------------------------------------------------------


def file_changed(db: duckdb.DuckDBPyConnection, path: Path) -> tuple[bool, int]:
    """Check if a file changed since last ingestion.

    Returns (changed, prev_size_bytes).  When the file has not changed,
    returns (False, <recorded size>).  When it has changed (or is new),
    returns (True, <previous size or 0>).
    """
    stat = path.stat()
    mtime_ns = stat.st_mtime_ns
    row = db.execute(
        "SELECT mtime_ns, size_bytes FROM file_mtimes WHERE file_path = ?",
        [str(path)],
    ).fetchone()
    if row and row[0] == mtime_ns:
        return False, row[1]
    prev_size = row[1] if row else 0
    return True, prev_size


def record_file(
    db: duckdb.DuckDBPyConnection, path: Path, *, size_bytes: int | None = None
) -> None:
    """Record a file's mtime and the byte offset actually consumed.

    When *size_bytes* is provided it is used instead of ``path.stat().st_size``.
    This prevents the race where the file grows during ingestion: we record
    only as far as we actually read, so the next incremental run picks up
    from the right place.
    """
    stat = path.stat()
    db.execute(
        "INSERT OR REPLACE INTO file_mtimes VALUES (?, ?, ?)",
        [
            str(path),
            stat.st_mtime_ns,
            size_bytes if size_bytes is not None else stat.st_size,
        ],
    )
