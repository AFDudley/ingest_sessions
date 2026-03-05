# ingest-sessions

MCP server that ingests Claude Code session transcripts into DuckDB and exposes them for querying.

On startup, ingests all JSONL session files from `~/.claude/projects/` and command history from `~/.claude/history.jsonl`. Watches for new/modified files and ingests them incrementally.

## Install

```bash
uv tool install .
```

Binaries are installed to `~/.local/bin/`:
- `ingest-sessions-server` — MCP server (HTTP or stdio)
- `ingest-sessions` — CLI ingestion tool

## Run

```bash
# HTTP server (default, multi-client, port 8741)
ingest-sessions-server

# Stdio mode (single client, testing)
ingest-sessions-server --stdio

# Custom host/port
ingest-sessions-server --host 0.0.0.0 --port 9000
```

## Systemd service

```bash
cp ingest-sessions.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now ingest-sessions
```

## MCP tools

**`query`** — Execute SQL against the sessions database.

```json
{"sql": "SELECT session_id, summary FROM sessions ORDER BY modified DESC LIMIT 10"}
```

**`refresh`** — Ingest a specific JSONL file into the database.

```json
{"path": "/home/user/.claude/projects/myproject/sess-001.jsonl"}
```

## MCP resources

**`ingest-sessions://schema`** — Current database schema (tables, columns, types).

## Database schema

| Table | Description |
|-------|-------------|
| `sessions` | Per-session metadata (summary, first prompt, message count, timestamps, git branch, project path) |
| `records` | Individual transcript entries — one row per JSONL line (uuid, session_id, type, timestamp, raw JSON) |
| `history` | Command history entries from `~/.claude/history.jsonl` |

## Configuration

All via environment variables:

| Variable | Default |
|----------|---------|
| `INGEST_SESSIONS_DB` | `~/.local/share/ingest_sessions/sessions.duckdb` |
| `INGEST_SESSIONS_PROJECTS_DIR` | `~/.claude/projects` |
| `INGEST_SESSIONS_HISTORY_FILE` | `~/.claude/history.jsonl` |
| `INGEST_SESSIONS_PORT` | `8741` |

## Tests

```bash
uv run pytest tests/ -v
```
