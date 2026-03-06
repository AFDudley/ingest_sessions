# ingest_sessions MCP Server

ingest_sessions is an MCP server that gives Claude Code instances queryable access to session transcript data stored in DuckDB.

On startup, the server performs a full ingestion of Claude Code session transcripts found under `~/.claude/projects/`. It parses each JSONL transcript file and loads everything into a local DuckDB database. It also ingests command history from `~/.claude/history.jsonl` when present.

Once the initial load completes, the server begins watching the projects directory for changes using `watchdog`. When a session transcript is created or modified on disk, the server detects the event and ingests the affected file incrementally — without disturbing the rest of the database.

## Transport

The server supports two transports:

**Streamable HTTP** (default) — runs as a long-lived process via uvicorn. Multiple Claude Code instances connect to the same server over HTTP at `/mcp`. Default listen address is `127.0.0.1:8741`. Configurable via `--host`, `--port`, or the `INGEST_SESSIONS_PORT` environment variable.

**Stdio** — single-client mode activated with `--stdio`. Useful for testing and for MCP clients that spawn the server as a subprocess.

The HTTP transport is intended for production use with a systemd user service. See `README.md` for setup.

## MCP interface

The server exposes two tools and one resource over MCP.

**`query`** accepts a SQL string and executes it against the database. Results are returned as JSON. Errors are returned as `{"error": "..."}` rather than raising exceptions.

**`refresh`** accepts a file path to a `.jsonl` session file and ingests it into the database. This allows a client to ensure a particular session is current without waiting for the file watcher to act. Returns `{"processed": N}` where N is the number of lines processed (including already-present records).

**`schema` resource** (`ingest-sessions://schema`) exposes the current database schema. Clients read this resource to discover available tables, columns, and types. The schema is generated dynamically from the database at runtime — there is no hardcoded copy.

## CLI tool

**Status: future work.** The current `cli.py` is the original batch ingestion script predating the MCP server. It does not connect to the server's database or provide operational visibility.

The intended CLI would:

- Report configuration details (database location, watched directories, ingestion status)
- Execute queries from `.sql` files and write results to an output file

This is secondary to the MCP server.

## Database schema

The database holds three tables.

**`sessions`** stores per-session metadata sourced from `sessions-index.json` files that Claude Code maintains alongside the JSONL transcripts.

- `session_id` (VARCHAR, PRIMARY KEY)
- `summary` (VARCHAR)
- `first_prompt` (VARCHAR)
- `message_count` (INTEGER)
- `created` (TIMESTAMP)
- `modified` (TIMESTAMP)
- `git_branch` (VARCHAR)
- `project_path` (VARCHAR)

**`records`** stores individual transcript entries. Each row is one JSONL line from a session file.

- `uuid` (VARCHAR, PRIMARY KEY) — unique record identifier
- `session_id` (VARCHAR) — which session this record belongs to
- `type` (VARCHAR) — record type (user, assistant, file-history-snapshot, etc.)
- `timestamp` (TIMESTAMP) — when the record was created
- `parent_uuid` (VARCHAR) — parent record for threading
- `raw` (JSON) — the full original JSONL line

**`history`** stores entries from `~/.claude/history.jsonl`.

- `timestamp` (BIGINT)
- `display` (VARCHAR)
- `session_id` (VARCHAR)
- `project` (VARCHAR)
- PRIMARY KEY (`timestamp`, `session_id`)

Indexes: `session_id`, `type`, and `timestamp` on records; `session_id` on history.

### Migrations

On startup, if the history table exists without a primary key (from an earlier schema), the server migrates it: creates a new table with the PK, copies distinct rows, drops the old table, and renames. This is handled by `_migrate_history_pk`.

## Ingestion behavior

Records are keyed by `uuid`. New records are inserted; existing records are skipped (`INSERT OR IGNORE`). JSONL files are append-only, so existing records do not change — only new lines are appended.

Malformed JSONL lines (truncated writes, corrupted files) are silently skipped. This prevents a single bad line from blocking ingestion of an entire session.

Session metadata is sourced from `sessions-index.json` files when they exist. These files are updated by Claude Code as sessions progress, so session rows use `INSERT OR REPLACE` to pick up changes to summary, message count, and timestamps.

On some platforms (observed on Linux), Claude Code does not write `sessions-index.json` files. When the index is absent, the server derives metadata from the JSONL records: first user message content as `first_prompt`, record count as `message_count`, and min/max timestamps as `created`/`modified`. Summary, git branch, and project path remain NULL in this case.

When watchdog detects a file change or `refresh` is called, the server reads the affected JSONL file and its parent directory's `sessions-index.json` (if present), then inserts any records and session metadata not already present. This is idempotent.

Content filtering (the old `--filter` flag) is not part of the MCP server. Clients filter at query time using SQL `WHERE` clauses.

Profiles are not part of the MCP server. The server always ingests all projects.

## File locations

The DuckDB database is derived data, not configuration, so it lives in `~/.local/share/ingest_sessions/sessions.duckdb`. The server creates this directory on first run if it does not exist.

All paths are configurable via environment variables:

| Variable | Default |
|----------|---------|
| `INGEST_SESSIONS_DB` | `~/.local/share/ingest_sessions/sessions.duckdb` |
| `INGEST_SESSIONS_PROJECTS_DIR` | `~/.claude/projects` |
| `INGEST_SESSIONS_HISTORY_FILE` | `~/.claude/history.jsonl` |
| `INGEST_SESSIONS_PORT` | `8741` |

## Concurrency

A single database thread owns the only DuckDB connection and processes all requests — reads and writes — sequentially via a queue. DuckDB does not allow mixing read-only and read-write connections in the same process, so one thread, one connection, one queue.

Callers submit work to the queue as `_DbRequest` objects. The async `_db_execute` helper submits a request and awaits its completion via `asyncio.to_thread`. Watchdog submits fire-and-forget requests with `wait=False`; errors on these are logged to stderr.

The server runs as a single process per user. Multiple Claude Code instances connect to the same HTTP server and share the same database.

## Dependencies

`duckdb`, `mcp`, `watchdog`, `uvicorn`, `starlette`.

## Source files

- `src/ingest_sessions/server.py` — MCP server, database thread, ingestion, watchdog, transports
- `src/ingest_sessions/cli.py` — Legacy batch ingestion script (predates MCP server)
- `tests/test_server.py` — E2E tests over stdio and HTTP transports
