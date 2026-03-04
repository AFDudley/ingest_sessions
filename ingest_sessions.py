#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""Ingest Claude Code session transcripts into DuckDB for analysis.

Supports named profiles in ~/.config/ingest_sessions/profiles.toml
and ad-hoc CLI overrides. See README or --help for usage.
"""

import argparse
import json
import sys
import tomllib
from pathlib import Path
from typing import Any

import duckdb

CLAUDE_DIR = Path.home() / ".claude"
PROJECTS_DIR = CLAUDE_DIR / "projects"
HISTORY_FILE = CLAUDE_DIR / "history.jsonl"
CONFIG_FILE = Path.home() / ".config" / "ingest_sessions" / "profiles.toml"


def load_profile(name: str) -> dict[str, Any]:
    """Load a named profile from the TOML config file."""
    if not CONFIG_FILE.exists():
        print(f"Config file not found: {CONFIG_FILE}", file=sys.stderr)
        sys.exit(1)
    profiles = tomllib.loads(CONFIG_FILE.read_text())
    if name not in profiles:
        available = ", ".join(profiles.keys())
        print(f"Profile '{name}' not found. Available: {available}", file=sys.stderr)
        sys.exit(1)
    return profiles[name]


def resolve_project_dirs(projects_spec: str | list[str]) -> list[Path]:
    """Resolve project directories from spec.

    Args:
        projects_spec: Either "*" for all projects, or a list of project
            directory names under ~/.claude/projects/.
    """
    if projects_spec == "*":
        return sorted(d for d in PROJECTS_DIR.iterdir() if d.is_dir())
    if isinstance(projects_spec, list):
        dirs: list[Path] = []
        for name in projects_spec:
            d = PROJECTS_DIR / name
            if not d.is_dir():
                print(f"Project dir not found: {d}", file=sys.stderr)
                sys.exit(1)
            dirs.append(d)
        return dirs
    print(f"Invalid projects spec: {projects_spec!r}", file=sys.stderr)
    sys.exit(1)


def create_tables(db: duckdb.DuckDBPyConnection) -> None:
    """Create the schema if it doesn't exist."""
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
            project VARCHAR
        )
    """)


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


def load_jsonl_session(
    db: duckdb.DuckDBPyConnection,
    text: str,
    session_id: str,
    session_meta: dict[str, dict[str, Any]],
) -> int:
    """Load a single JSONL session's text into the database.

    Returns the number of records loaded.
    """
    batch: list[tuple[str, str, str, str | None, str | None, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        batch.append((
            record.get("uuid", ""),
            record.get("sessionId", session_id),
            record.get("type", ""),
            record.get("timestamp"),
            record.get("parentUuid"),
            line,
        ))

    if batch:
        db.executemany(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )

    # Insert session metadata (from index if available, bare ID otherwise)
    meta = session_meta.get(session_id, {})
    db.execute(
        "INSERT OR IGNORE INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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

    return len(batch)


def load_history(
    db: duckdb.DuckDBPyConnection,
    content_filter: str | None,
) -> int:
    """Load matching entries from history.jsonl. Returns count loaded."""
    if not HISTORY_FILE.exists():
        return 0

    filter_lower = content_filter.lower() if content_filter else None
    batch: list[tuple[int | None, str | None, str | None, str | None]] = []
    for line in HISTORY_FILE.read_text().splitlines():
        if not line.strip():
            continue
        if filter_lower and filter_lower not in line.lower():
            continue
        entry = json.loads(line)
        batch.append((
            entry.get("timestamp"),
            entry.get("display"),
            entry.get("sessionId"),
            entry.get("project"),
        ))

    if batch:
        db.executemany("INSERT INTO history VALUES (?, ?, ?, ?)", batch)
    return len(batch)


def ingest(config: dict[str, Any]) -> None:
    """Run the ingestion pipeline from a resolved config dict."""
    projects_spec = config.get("projects", "*")
    content_filter: str | None = config.get("filter")
    output_path = Path(config["output"]).expanduser()
    include_history: bool = config.get("include_history", True)

    print(f"Output: {output_path}")
    print(f"Projects: {projects_spec}")
    if content_filter:
        print(f"Filter: {content_filter!r} (case-insensitive)")

    filter_lower = content_filter.lower() if content_filter else None

    output_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(output_path))
    create_tables(db)

    project_dirs = resolve_project_dirs(projects_spec)
    session_meta = build_session_metadata(project_dirs)

    total_sessions = 0
    total_records = 0

    for proj_dir in project_dirs:
        jsonl_files = sorted(proj_dir.glob("*.jsonl"))
        if not jsonl_files:
            continue

        proj_sessions = 0
        for jsonl_path in jsonl_files:
            session_id = jsonl_path.stem
            text = jsonl_path.read_text()

            # Apply content filter
            if filter_lower and filter_lower not in text.lower():
                continue

            count = load_jsonl_session(db, text, session_id, session_meta)
            total_records += count
            proj_sessions += 1

        if proj_sessions:
            print(f"  {proj_dir.name}: {proj_sessions} sessions")
            total_sessions += proj_sessions

    # History
    history_count = 0
    if include_history:
        history_count = load_history(db, content_filter)

    # Indexes
    db.execute("CREATE INDEX idx_records_session ON records(session_id)")
    db.execute("CREATE INDEX idx_records_type ON records(type)")
    db.execute("CREATE INDEX idx_records_timestamp ON records(timestamp)")
    db.execute("CREATE INDEX idx_history_session ON history(session_id)")

    # Summary
    print(f"\nLoaded {total_sessions} sessions, {total_records} records, "
          f"{history_count} history entries")

    print("\nRecords by type:")
    for row in db.execute(
        "SELECT type, count(*) as cnt FROM records GROUP BY type ORDER BY cnt DESC"
    ).fetchall():
        print(f"  {row[0]}: {row[1]}")

    db_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nDB size: {db_size_mb:.1f} MB at {output_path}")

    db.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Claude Code sessions into DuckDB",
    )
    parser.add_argument(
        "profile", nargs="?",
        help="Named profile from ~/.config/ingest_sessions/profiles.toml",
    )
    parser.add_argument(
        "--projects",
        help='Project dirs: "*" for all, or comma-separated names',
    )
    parser.add_argument("--filter", help="Case-insensitive content filter")
    parser.add_argument("--output", help="Output DuckDB path")
    parser.add_argument(
        "--no-history", action="store_true",
        help="Skip history.jsonl ingestion",
    )
    args = parser.parse_args()

    # Build config: profile defaults + CLI overrides
    config: dict[str, Any] = {}
    if args.profile:
        config = load_profile(args.profile)

    if args.projects is not None:
        config["projects"] = (
            args.projects if args.projects == "*"
            else args.projects.split(",")
        )
    if args.filter is not None:
        config["filter"] = args.filter
    if args.output is not None:
        config["output"] = args.output
    if args.no_history:
        config["include_history"] = False

    if "output" not in config:
        parser.error("--output is required (or use a profile that defines it)")

    ingest(config)


if __name__ == "__main__":
    main()
