#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""Batch ingest Claude Code session transcripts into DuckDB.

Supports named profiles in ~/.config/ingest_sessions/profiles.toml
and ad-hoc CLI overrides.  See README or --help for usage.

This is the one-shot batch tool.  For the long-running MCP server,
see ingest_sessions.server.
"""

from __future__ import annotations

import argparse
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import (
    build_session_metadata,
    create_tables,
    ingest_history,
    ingest_jsonl,
    ingest_session_metadata,
)


def _claude_dir() -> Path:
    return Path.home() / ".claude"


def _config_file() -> Path:
    return Path.home() / ".config" / "ingest_sessions" / "profiles.toml"


@dataclass
class IngestConfig:
    """Configuration for a batch ingestion run."""

    output: Path
    projects: str | list[str] = "*"
    content_filter: str | None = None
    include_history: bool = True


def load_profile(name: str) -> dict[str, Any]:
    """Load a named profile from the TOML config file.

    Raises SystemExit if the config file or profile is missing.
    """
    config_file = _config_file()
    if not config_file.exists():
        print(f"Config file not found: {config_file}", file=sys.stderr)
        sys.exit(1)
    profiles = tomllib.loads(config_file.read_text())
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

    Raises:
        ValueError: If a named project directory doesn't exist or the spec
            is invalid.
    """
    projects_dir = _claude_dir() / "projects"
    if projects_spec == "*":
        return sorted(d for d in projects_dir.iterdir() if d.is_dir())
    if isinstance(projects_spec, list):
        dirs: list[Path] = []
        for name in projects_spec:
            d = projects_dir / name
            if not d.is_dir():
                raise ValueError(f"Project dir not found: {d}")
            dirs.append(d)
        return dirs
    raise ValueError(f"Invalid projects spec: {projects_spec!r}")


def ingest(config: IngestConfig) -> None:
    """Run the batch ingestion pipeline."""
    output_path = config.output.expanduser()
    filter_lower = config.content_filter.lower() if config.content_filter else None

    print(f"Output: {output_path}")
    print(f"Projects: {config.projects}")
    if config.content_filter:
        print(f"Filter: {config.content_filter!r} (case-insensitive)")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(output_path))
    create_tables(db)

    project_dirs = resolve_project_dirs(config.projects)
    session_meta = build_session_metadata(project_dirs)

    total_sessions = 0
    total_records = 0

    for proj_dir in project_dirs:
        jsonl_files = sorted(proj_dir.glob("*.jsonl"))
        if not jsonl_files:
            continue

        proj_sessions = 0
        for jsonl_path in jsonl_files:
            text = jsonl_path.read_text()
            if filter_lower and filter_lower not in text.lower():
                continue

            session_id = jsonl_path.stem
            count, _ = ingest_jsonl(db, jsonl_path)
            ingest_session_metadata(db, session_id, session_meta)
            total_records += count
            proj_sessions += 1

        if proj_sessions:
            print(f"  {proj_dir.name}: {proj_sessions} sessions")
            total_sessions += proj_sessions

    history_count = 0
    if config.include_history:
        history_count = ingest_history(
            db, _claude_dir() / "history.jsonl", content_filter=config.content_filter
        )

    # Summary
    print(
        f"\nLoaded {total_sessions} sessions, {total_records} records, "
        f"{history_count} history entries"
    )

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
        "profile",
        nargs="?",
        help="Named profile from ~/.config/ingest_sessions/profiles.toml",
    )
    parser.add_argument(
        "--projects",
        help='Project dirs: "*" for all, or comma-separated names',
    )
    parser.add_argument("--filter", help="Case-insensitive content filter")
    parser.add_argument("--output", help="Output DuckDB path")
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Skip history.jsonl ingestion",
    )
    args = parser.parse_args()

    # Build config: profile defaults + CLI overrides
    raw: dict[str, Any] = {}
    if args.profile:
        raw = load_profile(args.profile)

    if args.projects is not None:
        raw["projects"] = (
            args.projects if args.projects == "*" else args.projects.split(",")
        )
    if args.filter is not None:
        raw["filter"] = args.filter
    if args.output is not None:
        raw["output"] = args.output
    if args.no_history:
        raw["include_history"] = False

    if "output" not in raw:
        parser.error("--output is required (or use a profile that defines it)")

    config = IngestConfig(
        output=Path(raw["output"]),
        projects=raw.get("projects", "*"),
        content_filter=raw.get("filter"),
        include_history=raw.get("include_history", True),
    )
    ingest(config)


if __name__ == "__main__":
    main()
