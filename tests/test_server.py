"""E2E tests for the ingest_sessions MCP server.

Tests the server as a running process over stdio, the way a real client would.
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult, TextContent

SERVER_SCRIPT = str(
    Path(__file__).resolve().parent.parent / "src" / "ingest_sessions" / "server.py"
)


def _text(result: CallToolResult, index: int = 0) -> str:
    """Extract text from a CallToolResult, narrowing the union type."""
    content = result.content[index]
    assert isinstance(content, TextContent)
    return content.text


@asynccontextmanager
async def run_server(
    tmpdir: Path,
    extra_env: dict[str, str] | None = None,
) -> AsyncIterator[tuple[ClientSession, Path]]:
    """Start the server with isolated DB, projects dir, and history file."""
    db_path = str(tmpdir / "test.duckdb")
    projects_dir = tmpdir / "projects"
    projects_dir.mkdir(exist_ok=True)
    # Point history at a nonexistent file by default to avoid reading real data
    history_path = str(tmpdir / "history.jsonl")
    env = {
        **os.environ,
        "INGEST_SESSIONS_DB": db_path,
        "INGEST_SESSIONS_PROJECTS_DIR": str(projects_dir),
        "INGEST_SESSIONS_HISTORY_FILE": history_path,
        **(extra_env or {}),
    }
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[SERVER_SCRIPT, "--stdio"],
        env=env,
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as client:
            await client.initialize()
            yield client, projects_dir


@pytest.mark.asyncio
async def test_server_lists_query_tool(tmp_path: Path):
    """The server should list 'query' as an available tool."""
    async with run_server(tmp_path) as (client, _):
        result = await client.list_tools()
        tool_names = [t.name for t in result.tools]
        assert "query" in tool_names


@pytest.mark.asyncio
async def test_query_select_one(tmp_path: Path):
    """The query tool should execute SQL and return results."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool("query", {"sql": "SELECT 1 AS n"})
        text = _text(result)
        rows = json.loads(text)
        assert rows == [{"n": 1}]


SAMPLE_RECORD = {
    "uuid": "abc-123",
    "sessionId": "sess-001",
    "type": "user",
    "timestamp": "2026-03-01T12:00:00.000Z",
    "parentUuid": None,
    "message": {"role": "user", "content": "hello"},
}


@pytest.mark.asyncio
async def test_query_ingested_session(tmp_path: Path):
    """Server should ingest JSONL on startup and make records queryable."""
    # Seed a project with one session file
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)
    (proj_dir / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")

    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT uuid, session_id, type FROM records"},
        )
        rows = json.loads(_text(result))
        assert len(rows) == 1
        assert rows[0]["uuid"] == "abc-123"
        assert rows[0]["session_id"] == "sess-001"
        assert rows[0]["type"] == "user"


@pytest.mark.asyncio
async def test_refresh_ingests_new_file(tmp_path: Path):
    """Refresh tool should ingest a session file added after startup."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)

    async with run_server(tmp_path) as (client, _):
        # No records yet
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 0}]

        # Write a session file after server started (sessionId matches filename)
        record = {**SAMPLE_RECORD, "uuid": "new-001", "sessionId": "sess-002"}
        jsonl_path = proj_dir / "sess-002.jsonl"
        jsonl_path.write_text(json.dumps(record) + "\n")

        # Refresh that file
        result = await client.call_tool("refresh", {"path": str(jsonl_path)})
        text = _text(result)
        assert "error" not in text.lower()

        # Now queryable
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 1}]

        # Session metadata should also be created (derived from records)
        result = await client.call_tool(
            "query",
            {
                "sql": "SELECT session_id, message_count FROM sessions WHERE session_id = 'sess-002'"
            },
        )
        rows = json.loads(_text(result))
        assert len(rows) == 1
        assert rows[0]["session_id"] == "sess-002"
        assert rows[0]["message_count"] == 1


@pytest.mark.asyncio
async def test_schema_resource(tmp_path: Path):
    """Server should expose a schema resource with table definitions."""
    async with run_server(tmp_path) as (client, _):
        # List resources — should include schema
        resources = await client.list_resources()
        uris = [r.uri for r in resources.resources]
        assert any("schema" in str(u) for u in uris), f"No schema resource in {uris}"

        # Read the schema resource
        schema_uri = next(u for u in uris if "schema" in str(u))
        result = await client.read_resource(schema_uri)
        content = result.contents[0]
        assert hasattr(content, "text")
        text = content.text

        # Should describe all three tables
        assert "sessions" in text
        assert "records" in text
        assert "history" in text


SESSIONS_INDEX = {
    "entries": [
        {
            "sessionId": "sess-001",
            "summary": "Test session",
            "firstPrompt": "hello world",
            "messageCount": 5,
            "created": "2026-03-01T12:00:00.000Z",
            "modified": "2026-03-01T13:00:00.000Z",
            "gitBranch": "main",
            "projectPath": "/home/user/project",
        }
    ]
}


@pytest.mark.asyncio
async def test_session_metadata_from_index(tmp_path: Path):
    """Sessions table should be populated from sessions-index.json."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)
    (proj_dir / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")
    (proj_dir / "sessions-index.json").write_text(json.dumps(SESSIONS_INDEX))

    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query",
            {
                "sql": "SELECT session_id, summary, first_prompt, message_count, git_branch FROM sessions"
            },
        )
        rows = json.loads(_text(result))
        assert len(rows) == 1
        assert rows[0]["session_id"] == "sess-001"
        assert rows[0]["summary"] == "Test session"
        assert rows[0]["first_prompt"] == "hello world"
        assert rows[0]["message_count"] == 5
        assert rows[0]["git_branch"] == "main"


@pytest.mark.asyncio
async def test_session_metadata_derived_without_index(tmp_path: Path):
    """Without sessions-index.json, session metadata should be derived from records."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)
    records = [
        {
            "uuid": "r1",
            "sessionId": "sess-003",
            "type": "user",
            "timestamp": "2026-03-01T10:00:00.000Z",
            "parentUuid": None,
            "message": {"role": "user", "content": "first message"},
        },
        {
            "uuid": "r2",
            "sessionId": "sess-003",
            "type": "assistant",
            "timestamp": "2026-03-01T10:05:00.000Z",
            "parentUuid": "r1",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "reply"}],
            },
        },
    ]
    (proj_dir / "sess-003.jsonl").write_text(
        "\n".join(json.dumps(r) for r in records) + "\n"
    )
    # No sessions-index.json

    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT session_id, message_count FROM sessions"},
        )
        rows = json.loads(_text(result))
        assert len(rows) == 1
        assert rows[0]["session_id"] == "sess-003"
        assert rows[0]["message_count"] == 2


@pytest.mark.asyncio
async def test_history_ingestion(tmp_path: Path):
    """Server should ingest history.jsonl entries."""
    # Create history file in tmp_path
    history_entries = [
        {
            "timestamp": 1709290800000,
            "display": "test command",
            "sessionId": "sess-001",
            "project": "myproject",
        },
        {
            "timestamp": 1709290860000,
            "display": "another cmd",
            "sessionId": "sess-002",
            "project": "myproject",
        },
    ]
    history_path = tmp_path / "history.jsonl"
    history_path.write_text("\n".join(json.dumps(e) for e in history_entries) + "\n")

    async with run_server(
        tmp_path, {"INGEST_SESSIONS_HISTORY_FILE": str(history_path)}
    ) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT count(*) AS n FROM history"},
        )
        rows = json.loads(_text(result))
        assert rows == [{"n": 2}]


@pytest.mark.asyncio
async def test_history_no_duplicates_on_restart(tmp_path: Path):
    """Restarting against the same DB should not duplicate history entries."""
    history_entries = [
        {
            "timestamp": 1709290800000,
            "display": "test command",
            "sessionId": "sess-001",
            "project": "myproject",
        },
        {
            "timestamp": 1709290860000,
            "display": "another cmd",
            "sessionId": "sess-002",
            "project": "myproject",
        },
    ]
    history_path = tmp_path / "history.jsonl"
    history_path.write_text("\n".join(json.dumps(e) for e in history_entries) + "\n")

    # First server run
    async with run_server(
        tmp_path, {"INGEST_SESSIONS_HISTORY_FILE": str(history_path)}
    ) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT count(*) AS n FROM history"},
        )
        assert json.loads(_text(result)) == [{"n": 2}]

    # Second server run against same DB — count must stay 2
    async with run_server(
        tmp_path, {"INGEST_SESSIONS_HISTORY_FILE": str(history_path)}
    ) as (client, _):
        result = await client.call_tool(
            "query",
            {"sql": "SELECT count(*) AS n FROM history"},
        )
        assert json.loads(_text(result)) == [{"n": 2}]


@pytest.mark.asyncio
async def test_refresh_idempotent_no_duplicates(tmp_path: Path):
    """Ingesting the same file twice should not create duplicate records."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)
    jsonl_path = proj_dir / "sess-001.jsonl"
    jsonl_path.write_text(json.dumps(SAMPLE_RECORD) + "\n")

    # File is ingested on startup, then refreshed — should still be 1 record
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 1}]

        # Refresh the same file
        result = await client.call_tool("refresh", {"path": str(jsonl_path)})
        assert "error" not in _text(result).lower()

        # Count must remain 1
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 1}]

        # Sessions table should also have exactly one entry
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM sessions"}
        )
        assert json.loads(_text(result)) == [{"n": 1}]


@pytest.mark.asyncio
async def test_watchdog_detects_new_file(tmp_path: Path):
    """Watchdog should auto-ingest new JSONL files without calling refresh."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)

    async with run_server(tmp_path) as (client, _):
        # No records yet
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 0}]

        # Write a new session file — watchdog should pick it up
        (proj_dir / "sess-new.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")

        # Poll until the record appears (watchdog is async, give it time)
        for _ in range(20):
            await asyncio.sleep(0.5)
            result = await client.call_tool(
                "query", {"sql": "SELECT count(*) AS n FROM records"}
            )
            rows = json.loads(_text(result))
            if rows == [{"n": 1}]:
                break
        else:
            pytest.fail("Watchdog did not ingest new file within 10 seconds")


@pytest.mark.asyncio
async def test_query_sql_error(tmp_path: Path):
    """Query tool should return an error object for invalid SQL."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool("query", {"sql": "SELECT * FROM nonexistent"})
        body = json.loads(_text(result))
        assert "error" in body
        assert isinstance(body["error"], str)
        assert len(body["error"]) > 0


@pytest.mark.asyncio
async def test_refresh_nonexistent_file(tmp_path: Path):
    """Refresh tool should return an error for a missing file."""
    async with run_server(tmp_path) as (client, _):
        fake_path = str(tmp_path / "does_not_exist.jsonl")
        result = await client.call_tool("refresh", {"path": fake_path})
        body = json.loads(_text(result))
        assert "error" in body
        assert "not found" in body["error"].lower()


def _free_port() -> int:
    """Return an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@asynccontextmanager
async def run_http_server(
    tmpdir: Path,
    extra_env: dict[str, str] | None = None,
) -> AsyncIterator[tuple[ClientSession, Path]]:
    """Start the server over HTTP on a random port, yield a connected client."""
    port = _free_port()
    db_path = str(tmpdir / "test.duckdb")
    projects_dir = tmpdir / "projects"
    projects_dir.mkdir(exist_ok=True)
    history_path = str(tmpdir / "history.jsonl")
    env = {
        **os.environ,
        "INGEST_SESSIONS_DB": db_path,
        "INGEST_SESSIONS_PROJECTS_DIR": str(projects_dir),
        "INGEST_SESSIONS_HISTORY_FILE": history_path,
        **(extra_env or {}),
    }
    proc = subprocess.Popen(
        [sys.executable, SERVER_SCRIPT, "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}/mcp"
    # Wait for the server to accept connections
    for _ in range(40):
        await asyncio.sleep(0.25)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            continue
    else:
        proc.kill()
        raise RuntimeError(f"HTTP server did not start within 10s on port {port}")
    try:
        async with streamable_http_client(url) as (read, write, _):
            async with ClientSession(read, write) as client:
                await client.initialize()
                yield client, projects_dir
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_summaries_table_exists(tmp_path: Path):
    """The summaries table should exist after server startup."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool(
            "query",
            {
                "sql": "SELECT column_name FROM information_schema.columns WHERE table_name = 'summaries' ORDER BY ordinal_position"
            },
        )
        rows = json.loads(_text(result))
        col_names = [r["column_name"] for r in rows]
        assert "summary_id" in col_names
        assert "session_id" in col_names
        assert "kind" in col_names
        assert "content" in col_names
        assert "token_count" in col_names
        assert "parent_ids" in col_names
        assert "message_uuids" in col_names
        assert "file_ids" in col_names
        assert "created_at" in col_names


@pytest.mark.asyncio
async def test_server_lists_summarize_tool(tmp_path: Path):
    """The server should list 'summarize' and 'context' as tools."""
    async with run_server(tmp_path) as (client, _):
        result = await client.list_tools()
        tool_names = [t.name for t in result.tools]
        assert "summarize" in tool_names
        assert "context" in tool_names


@pytest.mark.asyncio
async def test_context_tool_returns_none_without_summaries(tmp_path: Path):
    """Context tool should indicate no summaries when DAG is empty."""
    async with run_server(tmp_path) as (client, _):
        result = await client.call_tool("context", {"session_id": "nonexistent"})
        body = json.loads(_text(result))
        assert body.get("context") is None


@pytest.mark.asyncio
async def test_http_query_and_refresh(tmp_path: Path):
    """Exercise query and refresh tools over the HTTP transport."""
    proj_dir = tmp_path / "projects" / "myproject"
    proj_dir.mkdir(parents=True)
    (proj_dir / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")

    async with run_http_server(tmp_path) as (client, _):
        # Server should list tools
        tools = await client.list_tools()
        tool_names = [t.name for t in tools.tools]
        assert "query" in tool_names
        assert "refresh" in tool_names

        # Pre-seeded record should be queryable
        result = await client.call_tool(
            "query", {"sql": "SELECT uuid, session_id FROM records"}
        )
        rows = json.loads(_text(result))
        assert len(rows) == 1
        assert rows[0]["uuid"] == "abc-123"

        # Add a new file and refresh it
        new_record = {**SAMPLE_RECORD, "uuid": "def-456", "sessionId": "sess-002"}
        jsonl_path = proj_dir / "sess-002.jsonl"
        jsonl_path.write_text(json.dumps(new_record) + "\n")

        result = await client.call_tool("refresh", {"path": str(jsonl_path)})
        assert "error" not in _text(result).lower()

        # Now both records should be present
        result = await client.call_tool(
            "query", {"sql": "SELECT count(*) AS n FROM records"}
        )
        assert json.loads(_text(result)) == [{"n": 2}]
