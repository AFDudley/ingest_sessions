# ingest-sessions

MCP server that ingests Claude Code session transcripts into DuckDB, builds an LCM-style summary DAG, and provides context recovery after `/clear`.

On startup, ingests all JSONL session files from `~/.claude/projects/` and command history from `~/.claude/history.jsonl`. Watches for new/modified files and ingests them incrementally.

## Install

```bash
uv tool install .
ingest-sessions-install
```

Binaries are installed to `~/.local/bin/`:
- `ingest-sessions-server` — MCP server (HTTP or stdio)
- `ingest-sessions` — CLI ingestion tool
- `ingest-sessions-install` — Hook and MCP server installer

The installer registers:
1. **SessionStart hook** — injects summary context after `/clear`
2. **PreCompact hook** — triggers summarization before context compaction
3. **MCP server** — registers `ingest-sessions` as an HTTP MCP provider

```bash
# Check installation status
ingest-sessions-install --check

# Repair (re-run is idempotent)
ingest-sessions-install

# Remove everything
ingest-sessions-install --uninstall
```

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

## Summary DAG

The server builds an LCM (Lossless Context Management) summary hierarchy:

- **Sprigs (d1)** — Summaries of 20-message chunks, created by the PreCompact hook
- **Bindles (d2)** — Condensed summaries of 4+ sprigs

After `/clear`, the SessionStart hook assembles recovery context from:
1. The most recent bindle (condensed history)
2. Uncondensed sprigs (recent activity)
3. Unsummarized tail messages (most recent, not yet chunked)

Summarization uses Claude with a three-level escalation algorithm
(normal → aggressive → deterministic truncation) to guarantee convergence.

## MCP tools

**`query`** — Execute SQL against the sessions database.

```json
{"sql": "SELECT session_id, summary FROM sessions ORDER BY modified DESC LIMIT 10"}
```

**`refresh`** — Ingest a specific JSONL file into the database.

```json
{"path": "/home/user/.claude/projects/myproject/sess-001.jsonl"}
```

**`summarize`** — Run DAG maintenance for a session (create sprigs/bindles).

```json
{"session_id": "abc-123-def"}
```

**`context`** — Assemble recovery context for a session.

```json
{"session_id": "abc-123-def"}
```

## REST endpoints

Plain HTTP endpoints for hooks (bypass MCP protocol overhead):

| Endpoint | Method | Body | Description |
|----------|--------|------|-------------|
| `/api/context` | POST | `{"session_id": "..."}` or `{"project_dir": "..."}` | Assemble recovery context |
| `/api/refresh` | POST | `{"path": "/path/to/session.jsonl"}` | Ingest new messages |
| `/api/summarize` | POST | `{"session_id": "..."}` | Run DAG maintenance |

## MCP resources

**`ingest-sessions://schema`** — Current database schema (tables, columns, types).

## Database schema

| Table | Description |
|-------|-------------|
| `sessions` | Per-session metadata (summary, first prompt, message count, timestamps, git branch, project path) |
| `records` | Individual transcript entries — one row per JSONL line (uuid, session_id, type, timestamp, raw JSON) |
| `history` | Command history entries from `~/.claude/history.jsonl` |
| `summaries` | Summary DAG nodes (sprigs, bindles) with content, token counts, parent links |

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
