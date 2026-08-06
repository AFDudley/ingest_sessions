#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.3 acceptance test c2.

Feeds the same folder -- one omp-shaped session file, one Claude-Code-shaped
session file -- through all three ingest call sites (cli.ingest,
server._ingest_all, server._ingest_file_full), each into its own disposable
DuckDB, and observes whether the omp file ended up as a real omp session
(no surrogate uuid, real parent linkage, a real first_prompt) in every one
of them. Judges nothing -- prints one stdout_json object.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import duckdb

OMP_SESSION_ID = "shared-routing-omp-sess"


def _build_sample_folder(folder: Path) -> tuple[Path, Path]:
    """One omp-shaped session file + one Claude-Code-shaped session file.

    The omp file is a linear 8-entry chain (only its root entry has a null
    parent) so its parent_uuid non-null rate clears the 0.85 floor -- a
    single 2-entry file would sit at 0.5 and fail for a reason unrelated to
    routing correctness.
    """
    header = {
        "type": "session",
        "version": 3,
        "id": OMP_SESSION_ID,
        "cwd": "/repo",
        "title": "shared routing sample",
    }
    entries = []
    prev_id = None
    for i in range(8):
        entry_id = f"omp-e{i}"
        role = "user" if i % 2 == 0 else "assistant"
        entries.append(
            {
                "id": entry_id,
                "parentId": prev_id,
                "type": "message",
                "timestamp": f"2026-01-01T00:{i:02d}:00.000Z",
                "message": {"role": role, "content": f"turn {i}"},
            }
        )
        prev_id = entry_id
    omp_path = folder / "omp-shared.jsonl"
    omp_path.write_text("\n".join(json.dumps(x) for x in [header, *entries]) + "\n")

    claude_entry = {
        "uuid": "claude-shared-uuid",
        "sessionId": "claude-shared-sess",
        "type": "user",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "parentUuid": None,
        "message": {"role": "user", "content": "a claude code record"},
    }
    claude_path = folder / "claude-shared.jsonl"
    claude_path.write_text(json.dumps(claude_entry) + "\n")

    return omp_path, claude_path


def _omp_metrics(db: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    surrogate_row = db.execute(
        "SELECT count(*) FROM records WHERE uuid LIKE 'nouuid_%'"
    ).fetchone()
    assert surrogate_row is not None
    surrogate_count = surrogate_row[0]

    counts = db.execute(
        "SELECT count(*), count(parent_uuid) FROM records WHERE session_id = ?",
        [OMP_SESSION_ID],
    ).fetchone()
    assert counts is not None
    total, nonnull = counts
    rate = (nonnull / total) if total else 0.0

    first_prompt = db.execute(
        "SELECT first_prompt FROM sessions WHERE session_id = ?", [OMP_SESSION_ID]
    ).fetchone()
    first_prompt_nonnull = bool(first_prompt and first_prompt[0])

    return {
        "surrogate_uuid_count": surrogate_count,
        "parent_uuid_nonnull_rate": rate,
        "first_prompt_nonnull": first_prompt_nonnull,
    }


def _run_cli_ingest(sample_dir: Path, tmp_path: Path) -> dict[str, Any]:
    import ingest_sessions.cli as cli

    claude_home = tmp_path / "cli_home"
    project_dir = claude_home / "projects" / "shared"
    project_dir.mkdir(parents=True)
    for src in sample_dir.iterdir():
        (project_dir / src.name).write_text(src.read_text())

    cli._claude_dir = lambda: claude_home  # type: ignore[method-assign]

    db_path = tmp_path / "cli.duckdb"
    config = cli.IngestConfig(output=db_path, projects="*", include_history=False)
    with contextlib.redirect_stdout(io.StringIO()):
        cli.ingest(config)

    db = duckdb.connect(str(db_path))
    try:
        return _omp_metrics(db)
    finally:
        db.close()


def _run_ingest_all(sample_dir: Path, tmp_path: Path) -> dict[str, Any]:
    import ingest_sessions.server as server
    from ingest_sessions.core import create_tables

    os.environ["INGEST_SESSIONS_PROJECTS_DIR"] = str(sample_dir)
    os.environ["INGEST_SESSIONS_HISTORY_FILE"] = str(tmp_path / "no_history.jsonl")

    db_path = tmp_path / "ingest_all.duckdb"
    db = duckdb.connect(str(db_path))
    try:
        create_tables(db)
        server._ingest_all(db)
        return _omp_metrics(db)
    finally:
        db.close()


def _run_ingest_file_full(sample_dir: Path, tmp_path: Path) -> dict[str, Any]:
    import ingest_sessions.server as server
    from ingest_sessions.core import create_tables

    db_path = tmp_path / "ingest_file_full.duckdb"
    db = duckdb.connect(str(db_path))
    try:
        create_tables(db)
        for jsonl_path in sorted(sample_dir.glob("*.jsonl")):
            server._ingest_file_full(db, jsonl_path)
        return _omp_metrics(db)
    finally:
        db.close()


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        os.environ["INGEST_SESSIONS_BLOBS_DIR"] = str(tmp_path / "blobs")
        sample_dir = tmp_path / "samples"
        sample_dir.mkdir()
        _build_sample_folder(sample_dir)

        cli_run = tmp_path / "cli_run"
        cli_run.mkdir()
        cli_metrics = _run_cli_ingest(sample_dir, cli_run)

        all_run = tmp_path / "all_run"
        all_run.mkdir()
        ingest_all_metrics = _run_ingest_all(sample_dir, all_run)

        file_full_run = tmp_path / "file_full_run"
        file_full_run.mkdir()
        ingest_file_full_metrics = _run_ingest_file_full(sample_dir, file_full_run)

    parent_uuid_nonnull_rate = min(
        cli_metrics["parent_uuid_nonnull_rate"],
        ingest_all_metrics["parent_uuid_nonnull_rate"],
        ingest_file_full_metrics["parent_uuid_nonnull_rate"],
    )
    session_row_first_prompt_nonnull = (
        cli_metrics["first_prompt_nonnull"]
        and ingest_all_metrics["first_prompt_nonnull"]
        and ingest_file_full_metrics["first_prompt_nonnull"]
    )

    print(
        json.dumps(
            {
                "cli_ingest_surrogate_uuid_count": cli_metrics["surrogate_uuid_count"],
                "ingest_all_surrogate_uuid_count": ingest_all_metrics[
                    "surrogate_uuid_count"
                ],
                "ingest_file_full_surrogate_uuid_count": ingest_file_full_metrics[
                    "surrogate_uuid_count"
                ],
                "parent_uuid_nonnull_rate": parent_uuid_nonnull_rate,
                "session_row_first_prompt_nonnull": session_row_first_prompt_nonnull,
            }
        )
    )


if __name__ == "__main__":
    main()
