"""E2E tests for the ingest_sessions MCP server.

Tests the server as a running process over stdio, the way a real client would.
"""

import json
import os
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

SERVER_SCRIPT = str(Path(__file__).resolve().parent.parent / "server.py")


@asynccontextmanager
async def run_server(tmpdir, extra_env=None):
    """Start the server with isolated DB, projects dir, and history file."""
    db_path = str(Path(tmpdir) / "test.duckdb")
    projects_dir = Path(tmpdir) / "projects"
    projects_dir.mkdir(exist_ok=True)
    # Point history at a nonexistent file by default to avoid reading real data
    history_path = str(Path(tmpdir) / "history.jsonl")
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
async def test_server_lists_query_tool():
    """The server should list 'query' as an available tool."""
    with tempfile.TemporaryDirectory() as tmpdir:
        async with run_server(tmpdir) as (client, _):
            result = await client.list_tools()
            tool_names = [t.name for t in result.tools]
            assert "query" in tool_names


@pytest.mark.asyncio
async def test_query_select_one():
    """The query tool should execute SQL and return results."""
    with tempfile.TemporaryDirectory() as tmpdir:
        async with run_server(tmpdir) as (client, _):
            result = await client.call_tool("query", {"sql": "SELECT 1 AS n"})
            text = result.content[0].text
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
async def test_query_ingested_session():
    """Server should ingest JSONL on startup and make records queryable."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Seed a project with one session file
        proj_dir = Path(tmpdir) / "projects" / "myproject"
        proj_dir.mkdir(parents=True)
        (proj_dir / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")

        async with run_server(tmpdir) as (client, _):
            result = await client.call_tool(
                "query",
                {"sql": "SELECT uuid, session_id, type FROM records"},
            )
            rows = json.loads(result.content[0].text)
            assert len(rows) == 1
            assert rows[0]["uuid"] == "abc-123"
            assert rows[0]["session_id"] == "sess-001"
            assert rows[0]["type"] == "user"


@pytest.mark.asyncio
async def test_refresh_ingests_new_file():
    """Refresh tool should ingest a session file added after startup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        proj_dir = Path(tmpdir) / "projects" / "myproject"
        proj_dir.mkdir(parents=True)

        async with run_server(tmpdir) as (client, _):
            # No records yet
            result = await client.call_tool(
                "query", {"sql": "SELECT count(*) AS n FROM records"}
            )
            assert json.loads(result.content[0].text) == [{"n": 0}]

            # Write a session file after server started
            jsonl_path = proj_dir / "sess-002.jsonl"
            jsonl_path.write_text(json.dumps(SAMPLE_RECORD) + "\n")

            # Refresh that file
            result = await client.call_tool(
                "refresh", {"path": str(jsonl_path)}
            )
            text = result.content[0].text
            assert "error" not in text.lower()

            # Now queryable
            result = await client.call_tool(
                "query", {"sql": "SELECT count(*) AS n FROM records"}
            )
            assert json.loads(result.content[0].text) == [{"n": 1}]


@pytest.mark.asyncio
async def test_schema_resource():
    """Server should expose a schema resource with table definitions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        async with run_server(tmpdir) as (client, _):
            # List resources — should include schema
            resources = await client.list_resources()
            uris = [r.uri for r in resources.resources]
            assert any("schema" in str(u) for u in uris), f"No schema resource in {uris}"

            # Read the schema resource
            schema_uri = next(u for u in uris if "schema" in str(u))
            result = await client.read_resource(schema_uri)
            text = result.contents[0].text

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
async def test_session_metadata_from_index():
    """Sessions table should be populated from sessions-index.json."""
    with tempfile.TemporaryDirectory() as tmpdir:
        proj_dir = Path(tmpdir) / "projects" / "myproject"
        proj_dir.mkdir(parents=True)
        (proj_dir / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")
        (proj_dir / "sessions-index.json").write_text(json.dumps(SESSIONS_INDEX))

        async with run_server(tmpdir) as (client, _):
            result = await client.call_tool(
                "query",
                {"sql": "SELECT session_id, summary, first_prompt, message_count, git_branch FROM sessions"},
            )
            rows = json.loads(result.content[0].text)
            assert len(rows) == 1
            assert rows[0]["session_id"] == "sess-001"
            assert rows[0]["summary"] == "Test session"
            assert rows[0]["first_prompt"] == "hello world"
            assert rows[0]["message_count"] == 5
            assert rows[0]["git_branch"] == "main"


@pytest.mark.asyncio
async def test_session_metadata_derived_without_index():
    """Without sessions-index.json, session metadata should be derived from records."""
    with tempfile.TemporaryDirectory() as tmpdir:
        proj_dir = Path(tmpdir) / "projects" / "myproject"
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
                "message": {"role": "assistant", "content": [{"type": "text", "text": "reply"}]},
            },
        ]
        (proj_dir / "sess-003.jsonl").write_text(
            "\n".join(json.dumps(r) for r in records) + "\n"
        )
        # No sessions-index.json

        async with run_server(tmpdir) as (client, _):
            result = await client.call_tool(
                "query",
                {"sql": "SELECT session_id, message_count FROM sessions"},
            )
            rows = json.loads(result.content[0].text)
            assert len(rows) == 1
            assert rows[0]["session_id"] == "sess-003"
            assert rows[0]["message_count"] == 2


@pytest.mark.asyncio
async def test_history_ingestion():
    """Server should ingest history.jsonl entries."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create history file in the tmpdir
        history_entries = [
            {"timestamp": 1709290800000, "display": "test command", "sessionId": "sess-001", "project": "myproject"},
            {"timestamp": 1709290860000, "display": "another cmd", "sessionId": "sess-002", "project": "myproject"},
        ]
        history_path = Path(tmpdir) / "history.jsonl"
        history_path.write_text("\n".join(json.dumps(e) for e in history_entries) + "\n")

        async with run_server(tmpdir, {"INGEST_SESSIONS_HISTORY_FILE": str(history_path)}) as (client, _):
            result = await client.call_tool(
                "query",
                {"sql": "SELECT count(*) AS n FROM history"},
            )
            rows = json.loads(result.content[0].text)
            assert rows == [{"n": 2}]


@pytest.mark.asyncio
async def test_watchdog_detects_new_file():
    """Watchdog should auto-ingest new JSONL files without calling refresh."""
    with tempfile.TemporaryDirectory() as tmpdir:
        proj_dir = Path(tmpdir) / "projects" / "myproject"
        proj_dir.mkdir(parents=True)

        async with run_server(tmpdir) as (client, _):
            # No records yet
            result = await client.call_tool(
                "query", {"sql": "SELECT count(*) AS n FROM records"}
            )
            assert json.loads(result.content[0].text) == [{"n": 0}]

            # Write a new session file — watchdog should pick it up
            (proj_dir / "sess-new.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")

            # Poll until the record appears (watchdog is async, give it time)
            import asyncio
            for _ in range(20):
                await asyncio.sleep(0.5)
                result = await client.call_tool(
                    "query", {"sql": "SELECT count(*) AS n FROM records"}
                )
                rows = json.loads(result.content[0].text)
                if rows == [{"n": 1}]:
                    break
            else:
                pytest.fail("Watchdog did not ingest new file within 10 seconds")
