# ingest_sessions MCP Server

ingest_sessions is an MCP server that gives Claude Code instances queryable access to session transcript data stored in DuckDB.

On startup, the server performs a full ingestion of Claude Code session transcripts found under `~/.claude/projects/`. It parses each JSONL transcript file and loads everything into a local DuckDB database. It also ingests command history from `~/.claude/history.jsonl` when present.

Once the initial load completes, the server begins watching the projects directory for changes using `watchdog`. When a session transcript is created or modified on disk, the server detects the event and ingests the affected file incrementally — without disturbing the rest of the database.

## MCP interface

The server exposes two tools and one resource over MCP.

**`query`** accepts a SQL string and executes it against the database. Results are returned directly to the calling client. This is the primary interface — clients formulate their own queries against the schema and receive structured results.

**`refresh`** accepts a session file path or session identifier and ingests that specific file into the database. This allows a client to ensure a particular session is current without waiting for the file watcher to act.

**`schema` resource** exposes the current database schema. Clients read this resource to discover available tables, columns, and types. The schema is generated dynamically from the database at runtime — there is no hardcoded copy.

## CLI tool

A separate CLI tool connects to the same DuckDB database maintained by the MCP server. It is intended for human use. The CLI can:

- Report configuration details (database location, watched directories, ingestion status)
- Execute queries from `.sql` files and write results to an output file

The CLI is secondary to the MCP server and is not the initial development priority.

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

Indexes: `session_id`, `type`, and `timestamp` on records; `session_id` on history.

## Ingestion behavior

Records are keyed by `uuid`. New records are inserted; existing records are skipped (`INSERT OR IGNORE`). JSONL files are append-only, so existing records do not change — only new lines are appended.

Session metadata is sourced from `sessions-index.json` files when they exist. These files are updated by Claude Code as sessions progress, so session rows use `INSERT OR REPLACE` to pick up changes to summary, message count, and timestamps. On some platforms (observed on Linux), Claude Code does not write these index files. When the index is absent, the server derives what it can from the JSONL records themselves: session ID, first user message as first prompt, message count, and created/modified timestamps from the earliest and latest record timestamps.

When watchdog detects a file change or `refresh` is called, the server reads the affected JSONL file and its parent directory's `sessions-index.json` (if present), then inserts any records and session metadata not already present. This is idempotent.

Content filtering (the old `--filter` flag) is not part of the MCP server. Clients filter at query time using SQL `WHERE` clauses.

Profiles are not part of the MCP server. The server always ingests all projects.

## File locations

The server follows XDG conventions. Configuration lives in `~/.config/ingest_sessions/`. The DuckDB database is derived data, not configuration, so it lives in `~/.local/share/ingest_sessions/sessions.duckdb`. The server creates this directory on first run if it does not exist.

## Concurrency

The server runs as a single process per user. Multiple Claude Code instances connect to the same server and share the same database. DuckDB handles concurrent reads natively; writes are serialized within the single server process using an asyncio lock.

## Dependencies

`duckdb`, `mcp`, and `watchdog`.
