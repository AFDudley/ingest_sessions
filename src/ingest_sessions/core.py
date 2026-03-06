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
    migrate_history_pk(db)
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_records_session_id ON records(session_id)"
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_records_type ON records(type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_records_timestamp ON records(timestamp)")
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_history_session_id ON history(session_id)"
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


def ingest_jsonl(
    db: duckdb.DuckDBPyConnection,
    jsonl_path: Path,
    byte_offset: int = 0,
) -> int:
    """Ingest a single JSONL session file.  Returns record count.

    When byte_offset > 0, only lines after that offset are read (JSONL is
    append-only).  The first partial line after the offset is silently
    skipped to avoid parsing a truncated JSON object.
    """
    with open(jsonl_path, "r") as f:
        if byte_offset > 0:
            f.seek(byte_offset)
            f.readline()  # skip potential partial line at boundary
        text = f.read()

    batch = []
    session_id = jsonl_path.stem
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
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
    return len(batch)


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


def record_file(db: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Record a file's current mtime and size after successful ingestion."""
    stat = path.stat()
    db.execute(
        "INSERT OR REPLACE INTO file_mtimes VALUES (?, ?, ?)",
        [str(path), stat.st_mtime_ns, stat.st_size],
    )
