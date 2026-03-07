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
from functools import partial
from pathlib import Path
from typing import Any, Callable, TypeVar

import duckdb
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import AnyUrl, Resource, TextContent, Tool
from watchdog.events import (
    DirModifiedEvent,
    FileModifiedEvent,
    FileSystemEvent,
    FileSystemEventHandler,
)
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

from ingest_sessions.dag import (
    BINDLE_THRESHOLD,
    SPRIG_CHUNK_SIZE,
    assemble_context_for_session,
    get_latest_bindle_content,
    get_latest_summarized_session,
    get_sprigs_for_session,
    get_unsummarized_messages,
    insert_bindle,
    insert_sprig,
)
from ingest_sessions.summarize import (
    condense_summaries,
    summarize_messages,
)
from ingest_sessions.core import (
    build_session_metadata,
    create_tables,
    file_changed,
    ingest_history,
    ingest_jsonl,
    ingest_session_metadata,
    record_file,
)

T = TypeVar("T")

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
    log_errors: bool = False


# None is the shutdown sentinel.
_db_queue: queue.Queue[_DbRequest | None] = queue.Queue()
_startup_done = threading.Event()
_db_thread: threading.Thread | None = None


def _db_submit(fn: Callable[[duckdb.DuckDBPyConnection], Any]) -> _DbRequest:
    """Submit work to the database thread.  Does not wait."""
    req = _DbRequest(fn=fn)
    _db_queue.put(req)
    return req


async def _db_execute(fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
    """Submit work to the database thread and await its completion."""
    req = _db_submit(fn)
    await asyncio.to_thread(req.done.wait)
    if req.error is not None:
        raise req.error
    return req.result  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _db_path() -> str:
    default = str(
        Path.home() / ".local" / "share" / "ingest_sessions" / "sessions.duckdb"
    )
    return os.environ.get("INGEST_SESSIONS_DB", default)


def _projects_dir() -> Path:
    env = os.environ.get("INGEST_SESSIONS_PROJECTS_DIR")
    if env:
        return Path(env)
    return Path.home() / ".claude" / "projects"


def _history_file() -> Path:
    env = os.environ.get("INGEST_SESSIONS_HISTORY_FILE")
    if env:
        return Path(env)
    return Path.home() / ".claude" / "history.jsonl"


# ---------------------------------------------------------------------------
# Ingestion helpers (server-specific orchestration around core functions)
# ---------------------------------------------------------------------------


def _ingest_file_full(db: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
    """Ingest records AND session metadata for a single JSONL file."""
    _, prev_size = file_changed(db, jsonl_path)
    session_id = jsonl_path.stem
    count = ingest_jsonl(db, jsonl_path, byte_offset=prev_size)
    session_meta = build_session_metadata([jsonl_path.parent])
    ingest_session_metadata(db, session_id, session_meta)
    record_file(db, jsonl_path)
    return count


def _ingest_all(db: duckdb.DuckDBPyConnection) -> None:
    """Incremental ingestion: only re-process files modified since last run."""
    projects_dir = _projects_dir()
    if not projects_dir.exists():
        return

    project_dirs = sorted(d for d in projects_dir.iterdir() if d.is_dir())
    session_meta = build_session_metadata(project_dirs)

    for proj_dir in project_dirs:
        for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
            changed, prev_size = file_changed(db, jsonl_path)
            if not changed:
                continue
            session_id = jsonl_path.stem
            ingest_jsonl(db, jsonl_path, byte_offset=prev_size)
            ingest_session_metadata(db, session_id, session_meta)
            record_file(db, jsonl_path)

    history = _history_file()
    if history.exists():
        changed, _ = file_changed(db, history)
        if changed:
            ingest_history(db, history)
            record_file(db, history)


# ---------------------------------------------------------------------------
# Async summarization (shared by MCP tool handler and REST endpoint)
# ---------------------------------------------------------------------------


async def _run_summarize_async(session_id: str) -> dict[str, int]:
    """Run one pass of DAG maintenance for a session.

    DB reads/writes go through _db_execute (DB thread).
    LLM calls go through asyncio.to_thread (NOT the DB thread).

    Returns counts of what was created.
    """
    unsummarized = await _db_execute(
        partial(get_unsummarized_messages, session_id=session_id)
    )
    existing_sprigs = await _db_execute(
        partial(get_sprigs_for_session, session_id=session_id)
    )
    previous_context = existing_sprigs[-1]["content"] if existing_sprigs else None

    sprigs_created = 0
    full_chunks = len(unsummarized) // SPRIG_CHUNK_SIZE
    for i in range(full_chunks):
        chunk = unsummarized[i * SPRIG_CHUNK_SIZE : (i + 1) * SPRIG_CHUNK_SIZE]
        content = await asyncio.to_thread(summarize_messages, chunk, previous_context)
        uuids = [r["uuid"] for r in chunk]
        await _db_execute(
            partial(
                insert_sprig,
                session_id=session_id,
                content=content,
                message_uuids=uuids,
            )
        )
        previous_context = content
        sprigs_created += 1

    uncondensed = await _db_execute(
        partial(get_sprigs_for_session, session_id=session_id, uncondensed_only=True)
    )
    bindles_created = 0
    if len(uncondensed) >= BINDLE_THRESHOLD:
        bindle_context = await _db_execute(
            partial(get_latest_bindle_content, session_id=session_id)
        )
        content = await asyncio.to_thread(
            condense_summaries, uncondensed, bindle_context
        )
        parent_ids = [s["summary_id"] for s in uncondensed]
        await _db_execute(
            partial(
                insert_bindle,
                session_id=session_id,
                content=content,
                parent_ids=parent_ids,
            )
        )
        bindles_created += 1

    return {
        "sprigs_created": sprigs_created,
        "bindles_created": bindles_created,
        "unsummarized_remaining": len(unsummarized) - (full_chunks * SPRIG_CHUNK_SIZE),
    }


# ---------------------------------------------------------------------------
# Database thread
# ---------------------------------------------------------------------------


def _db_loop() -> None:
    """Single database thread.  Owns the connection, drains the queue."""
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(path)
    try:
        create_tables(db)
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
            if req.log_errors and req.error:
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

    def on_created(self, event: FileSystemEvent) -> None:
        self._handle(event)

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        self._handle(event)

    def _handle(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        path = Path(str(event.src_path))
        if path.suffix != ".jsonl":
            return
        captured = path
        req = _db_submit(lambda db: _ingest_file_full(db, captured))
        req.log_errors = True


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
        Tool(
            name="summarize",
            description=(
                "Trigger LCM summary DAG maintenance for a session. "
                "Summarizes unsummarized messages into sprigs, "
                "condenses sprigs into bindles. Uses claude --print "
                "(covered by Max). Returns counts of nodes created."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to summarize.",
                    },
                },
                "required": ["session_id"],
            },
        ),
        Tool(
            name="context",
            description=(
                "Assemble recovery context from the LCM summary DAG "
                "for a session. Returns the formatted context string "
                "suitable for injection after /clear."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "The session ID to get context for.",
                    },
                },
                "required": ["session_id"],
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
        try:
            count = await _db_execute(lambda db: _ingest_file_full(db, jsonl_path))
            return [TextContent(type="text", text=json.dumps({"processed": count}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    if name == "summarize":
        session_id = arguments["session_id"]
        result_data = await _run_summarize_async(session_id)
        return [TextContent(type="text", text=json.dumps(result_data))]

    if name == "context":
        session_id = arguments["session_id"]
        try:
            ctx = await _db_execute(
                partial(assemble_context_for_session, session_id=session_id)
            )
            return [TextContent(type="text", text=json.dumps({"context": ctx}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

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

    async def handle_context_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the SessionStart hook.

        POST /api/context with {"project_dir": "..."} or {"session_id": "..."}.
        Returns {"context": "..."} or {"context": null}.
        Bypasses MCP protocol — the hook is a plain HTTP client.
        """
        body = await request.json()
        session_id = body.get("session_id")

        if not session_id:
            project_dir = body.get("project_dir", "")
            if project_dir:
                session_id = await _db_execute(
                    partial(get_latest_summarized_session, project_dir=project_dir)
                )

        if not session_id:
            return starlette.responses.JSONResponse({"context": None})

        ctx = await _db_execute(
            partial(assemble_context_for_session, session_id=session_id)
        )
        return starlette.responses.JSONResponse({"context": ctx})

    async def handle_refresh_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the PreCompact hook (step 1: ingest new messages).

        POST /api/refresh with {"path": "/path/to/session.jsonl"}.
        Returns {"processed": N} or {"error": "..."}.
        """
        body = await request.json()
        jsonl_path = Path(body.get("path", ""))
        if not jsonl_path.exists():
            return starlette.responses.JSONResponse(
                {"error": f"File not found: {jsonl_path}"}, status_code=404
            )
        count = await _db_execute(lambda db: _ingest_file_full(db, jsonl_path))
        return starlette.responses.JSONResponse({"processed": count})

    async def handle_summarize_api(
        request: starlette.requests.Request,
    ) -> starlette.responses.JSONResponse:
        """REST endpoint for the PreCompact hook (step 2: run DAG maintenance).

        POST /api/summarize with {"session_id": "..."}.
        Returns 202 immediately; summarization runs in the background.

        The hook caller does not need to wait for LLM calls to finish.
        """
        body = await request.json()
        session_id = body.get("session_id", "")
        if not session_id:
            return starlette.responses.JSONResponse(
                {"error": "session_id required"}, status_code=400
            )

        if body.get("wait"):
            result = await _run_summarize_async(session_id)
            return starlette.responses.JSONResponse(result)

        async def _bg_summarize() -> None:
            try:
                result = await _run_summarize_async(session_id)
                print(
                    f"[ingest-sessions] summarize {session_id}: {result}",
                    file=sys.stderr,
                )
            except Exception as exc:
                print(
                    f"[ingest-sessions] summarize {session_id} failed: {exc}",
                    file=sys.stderr,
                )

        asyncio.ensure_future(_bg_summarize())
        return starlette.responses.JSONResponse(
            {"status": "accepted", "session_id": session_id}, status_code=202
        )

    import starlette.requests
    import starlette.responses
    from starlette.routing import Route

    app = Starlette(
        routes=[
            Mount("/mcp", app=handle_mcp),
            Route("/api/context", handle_context_api, methods=["POST"]),
            Route("/api/refresh", handle_refresh_api, methods=["POST"]),
            Route("/api/summarize", handle_summarize_api, methods=["POST"]),
        ],
        lifespan=lifespan,
    )

    uvicorn.run(app, host=host, port=port)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


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
