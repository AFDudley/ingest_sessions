"""ingest_sessions MCP server.

Runs as a long-lived HTTP process. Multiple Claude Code instances connect to
the same server over streamable-HTTP at /mcp.  Stdio transport is retained
for single-client use and testing.

All database access goes through a single thread that owns the only DuckDB
connection.  Callers submit work to a queue; the thread processes it
sequentially.  DuckDB does not allow mixing read-only and read-write
connections in the same process, so one thread, one connection, one queue.
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import duckdb
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import AnyUrl, Resource, TextContent, Tool
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

DEFAULT_DB_PATH = (
    Path.home() / ".local" / "share" / "ingest_sessions" / "sessions.duckdb"
)
DEFAULT_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DEFAULT_HISTORY_FILE = Path.home() / ".claude" / "history.jsonl"
DEFAULT_PORT = 8741

server = Server("ingest-sessions")

# ---------------------------------------------------------------------------
# Database queue.  A single thread owns the DuckDB connection and processes
# all requests — reads and writes — sequentially.
# ---------------------------------------------------------------------------


@dataclass
class _DbRequest:
    """A unit of work for the database thread."""

    fn: Callable[[duckdb.DuckDBPyConnection], Any]
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: Exception | None = None
    wait: bool = True


# None is the shutdown sentinel.
_db_queue: queue.Queue[_DbRequest | None] = queue.Queue()
_startup_done = threading.Event()
_db_thread: threading.Thread | None = None


def _db_submit(fn: Callable[[duckdb.DuckDBPyConnection], Any]) -> _DbRequest:
    """Submit work to the database thread.  Does not wait."""
    req = _DbRequest(fn=fn)
    _db_queue.put(req)
    return req


async def _db_execute(fn: Callable[[duckdb.DuckDBPyConnection], Any]) -> Any:
    """Submit work to the database thread and await its completion."""
    req = _db_submit(fn)
    await asyncio.to_thread(req.done.wait)
    if req.error is not None:
        raise req.error
    return req.result


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _db_path() -> str:
    return os.environ.get("INGEST_SESSIONS_DB", str(DEFAULT_DB_PATH))


def _projects_dir() -> Path:
    env = os.environ.get("INGEST_SESSIONS_PROJECTS_DIR")
    if env:
        return Path(env)
    return DEFAULT_PROJECTS_DIR


def _history_file() -> Path:
    env = os.environ.get("INGEST_SESSIONS_HISTORY_FILE")
    if env:
        return Path(env)
    return DEFAULT_HISTORY_FILE


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _migrate_history_pk(db: duckdb.DuckDBPyConnection) -> None:
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


def _create_tables(db: duckdb.DuckDBPyConnection) -> None:
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
    _migrate_history_pk(db)
    # Indexes per DESIGN.md
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


def _build_session_metadata(project_dirs: list[Path]) -> dict[str, dict[str, Any]]:
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


def _derive_session_metadata(
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
    first_prompt = None
    if first_user:
        try:
            record = json.loads(first_user[0])
        except json.JSONDecodeError:
            record = None
        if isinstance(record, dict):
            msg = record.get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content", "")
                if isinstance(content, str):
                    first_prompt = content[:500]

    db.execute(
        "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [session_id, None, first_prompt, message_count, created, modified, None, None],
    )


def _ingest_session_metadata(
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
        _derive_session_metadata(db, session_id)


# ---------------------------------------------------------------------------
# Ingestion — called only from the database thread.
# ---------------------------------------------------------------------------


def _ingest_jsonl(db: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
    """Ingest a single JSONL session file. Returns record count."""
    text = jsonl_path.read_text()
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
        for row in batch:
            db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
    return len(batch)


def _ingest_file_full(db: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
    """Ingest records AND session metadata for a single JSONL file."""
    session_id = jsonl_path.stem
    count = _ingest_jsonl(db, jsonl_path)
    session_meta = _build_session_metadata([jsonl_path.parent])
    _ingest_session_metadata(db, session_id, session_meta)
    return count


def _ingest_history(db: duckdb.DuckDBPyConnection) -> int:
    """Ingest history.jsonl entries. Returns count loaded."""
    history_file = _history_file()
    if not history_file.exists():
        return 0
    batch = []
    for line in history_file.read_text().splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
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


def _ingest_all(db: duckdb.DuckDBPyConnection) -> None:
    """Full ingestion of all projects on startup."""
    projects_dir = _projects_dir()
    if not projects_dir.exists():
        return

    project_dirs = sorted(d for d in projects_dir.iterdir() if d.is_dir())
    session_meta = _build_session_metadata(project_dirs)

    for proj_dir in project_dirs:
        for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
            session_id = jsonl_path.stem
            _ingest_jsonl(db, jsonl_path)
            _ingest_session_metadata(db, session_id, session_meta)

    _ingest_history(db)


# ---------------------------------------------------------------------------
# Database thread
# ---------------------------------------------------------------------------


def _db_loop() -> None:
    """Single database thread.  Owns the connection, drains the queue."""
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(path)
    try:
        _create_tables(db)
        _ingest_all(db)
        _startup_done.set()

        while True:
            req = _db_queue.get()
            if req is None:
                break
            try:
                req.result = req.fn(db)
            except Exception as exc:
                req.error = exc
            req.done.set()
            if not req.wait and req.error:
                print(
                    f"[ingest-sessions] background db error: {req.error}",
                    file=sys.stderr,
                )
    finally:
        db.close()


def _start_db_thread() -> threading.Thread:
    """Start the database thread and wait for initial ingestion to finish."""
    t = threading.Thread(target=_db_loop, daemon=True, name="db-thread")
    t.start()
    _startup_done.wait()
    return t


def _stop_db_thread() -> None:
    """Send shutdown sentinel and wait for database thread to exit."""
    _db_queue.put(None)
    if _db_thread is not None:
        _db_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


class _JsonlHandler(FileSystemEventHandler):
    """Watchdog handler that enqueues new/modified JSONL files."""

    def on_created(self, event):  # type: ignore[override]
        self._handle(event)

    def on_modified(self, event):  # type: ignore[override]
        self._handle(event)

    def _handle(self, event) -> None:  # type: ignore[no-untyped-def]
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix != ".jsonl":
            return
        # Fire-and-forget: enqueue, don't wait.
        captured = path
        req = _db_submit(lambda db: _ingest_file_full(db, captured))
        req.wait = False


def _start_watcher() -> BaseObserver:
    """Start watchdog observer on the projects directory."""
    projects_dir = _projects_dir()
    projects_dir.mkdir(parents=True, exist_ok=True)
    observer = Observer()
    observer.schedule(_JsonlHandler(), str(projects_dir), recursive=True)
    observer.daemon = True
    observer.start()
    return observer


def _startup() -> BaseObserver | None:
    """Run on server start: start database thread, start watcher."""
    global _db_thread
    _startup_done.clear()
    _db_thread = _start_db_thread()
    return _start_watcher()


def _shutdown(observer: BaseObserver | None) -> None:
    """Clean shutdown: stop watcher, stop database thread."""
    if observer:
        observer.stop()
        observer.join(timeout=2)
    _stop_db_thread()


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="query",
            description="Execute a SQL query against the sessions database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to execute.",
                    },
                },
                "required": ["sql"],
            },
        ),
        Tool(
            name="refresh",
            description="Ingest a specific session JSONL file into the database.",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Path to the .jsonl session file.",
                    },
                },
                "required": ["path"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    if name == "query":
        sql = arguments["sql"]

        def do_query(db: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
            result = db.execute(sql)
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

        try:
            rows = await _db_execute(do_query)
            return [TextContent(type="text", text=json.dumps(rows, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "refresh":
        jsonl_path = Path(arguments["path"])
        if not jsonl_path.exists():
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": f"File not found: {jsonl_path}"}),
                )
            ]
        count = await _db_execute(lambda db: _ingest_file_full(db, jsonl_path))
        return [TextContent(type="text", text=json.dumps({"processed": count}))]

    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# MCP resources
# ---------------------------------------------------------------------------

SCHEMA_URI = "ingest-sessions://schema"


@server.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri=AnyUrl(SCHEMA_URI),
            name="Database Schema",
            description="Table definitions for the sessions database.",
            mimeType="text/plain",
        ),
    ]


@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    if str(uri) == SCHEMA_URI:

        def get_schema(db: duckdb.DuckDBPyConnection) -> str:
            tables = db.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
            parts = []
            for (table_name,) in tables:
                parts.append(f"## {table_name}")
                cols = db.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = ? ORDER BY ordinal_position",
                    [table_name],
                ).fetchall()
                for col_name, col_type in cols:
                    parts.append(f"  {col_name} {col_type}")
                parts.append("")
            return "\n".join(parts)

        return await _db_execute(get_schema)

    raise ValueError(f"Unknown resource: {uri}")


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


async def run_stdio() -> None:
    """Run in stdio mode (single client, good for testing)."""
    observer = _startup()
    try:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())
    finally:
        _shutdown(observer)


def run_http(host: str = "127.0.0.1", port: int | None = None) -> None:
    """Run as a streamable-HTTP server (multi-client)."""
    import contextlib
    from collections.abc import AsyncIterator

    import uvicorn
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.types import Receive, Scope, Send

    if port is None:
        port = int(os.environ.get("INGEST_SESSIONS_PORT", str(DEFAULT_PORT)))

    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
        stateless=True,
    )

    async def handle_mcp(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    observer: BaseObserver | None = None

    @contextlib.asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        nonlocal observer
        observer = _startup()
        async with session_manager.run():
            try:
                yield
            finally:
                _shutdown(observer)

    app = Starlette(
        routes=[Mount("/mcp", app=handle_mcp)],
        lifespan=lifespan,
    )

    uvicorn.run(app, host=host, port=port)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    """Default entrypoint — stdio for backward compat."""
    await run_stdio()


def cli() -> None:
    """Entry point for ``ingest-sessions-server``.

    Usage::

        ingest-sessions-server              # streamable-HTTP on 127.0.0.1:8741
        ingest-sessions-server --stdio      # stdio (single client / testing)
        ingest-sessions-server --port 9000  # custom port
        ingest-sessions-server --host 0.0.0.0  # listen on all interfaces
    """
    import argparse

    parser = argparse.ArgumentParser(description="ingest-sessions MCP server")
    parser.add_argument(
        "--stdio", action="store_true", help="Run in stdio mode (single client)"
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="HTTP listen address (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"HTTP listen port (default: {DEFAULT_PORT})",
    )
    args = parser.parse_args()

    if args.stdio:
        asyncio.run(run_stdio())
    else:
        run_http(host=args.host, port=args.port)


if __name__ == "__main__":
    cli()
