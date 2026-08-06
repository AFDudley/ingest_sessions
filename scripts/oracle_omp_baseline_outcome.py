#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c8.

Ingests a representative sample of omp session files covering the
entry-type mix the naive line-by-file-order approach failed on (message,
custom, custom_message, model_change, session_init, thinking_level_change,
plus a branch), and observes the measured baseline this pebble must
reverse: not-100%-surrogate-uuid, some parent_uuid populated, some
role-typed (user/assistant) records. Judges nothing -- prints one
stdout_json object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import ingest_omp_session


def _session_a(tmp_path: Path) -> Path:
    """message + custom + custom_message + model_change, linear chain."""
    header = {"type": "session", "version": 3, "id": "omp-sample-a", "cwd": "/repo"}
    e1 = {
        "id": "a-1",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "what backend should we use?"},
    }
    e2 = {
        "id": "a-2",
        "parentId": "a-1",
        "type": "model_change",
        "timestamp": "2026-01-01T00:00:30.000Z",
        "model": "claude-sonnet-5",
    }
    e3 = {
        "id": "a-3",
        "parentId": "a-2",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "use duckdb, it is embedded"},
    }
    e4 = {
        "id": "a-4",
        "parentId": "a-3",
        "type": "custom",
        "timestamp": "2026-01-01T00:01:30.000Z",
        "data": {"note": "operator annotation"},
    }
    e5 = {
        "id": "a-5",
        "parentId": "a-4",
        "type": "custom_message",
        "timestamp": "2026-01-01T00:02:00.000Z",
        "content": "a synthesized custom message",
    }
    path = tmp_path / "omp-sample-a.jsonl"
    path.write_text(
        "\n".join(json.dumps(x) for x in [header, e1, e2, e3, e4, e5]) + "\n"
    )
    return path


def _session_b(tmp_path: Path) -> Path:
    """session_init + thinking_level_change + a branch."""
    header = {"type": "session", "version": 3, "id": "omp-sample-b", "cwd": "/repo"}
    init = {
        "id": "b-1",
        "parentId": None,
        "type": "session_init",
        "timestamp": "2026-01-01T00:00:00.000Z",
    }
    thinking = {
        "id": "b-2",
        "parentId": "b-1",
        "type": "thinking_level_change",
        "timestamp": "2026-01-01T00:00:30.000Z",
        "level": "high",
    }
    fork_point = {
        "id": "b-3",
        "parentId": "b-2",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "user", "content": "question before the fork"},
    }
    branch_1 = {
        "id": "b-4a",
        "parentId": "b-3",
        "type": "message",
        "timestamp": "2026-01-01T00:01:30.000Z",
        "message": {"role": "assistant", "content": "branch 1 reply"},
    }
    branch_2 = {
        "id": "b-4b",
        "parentId": "b-3",
        "type": "message",
        "timestamp": "2026-01-01T00:01:45.000Z",
        "message": {"role": "assistant", "content": "branch 2 reply"},
    }
    path = tmp_path / "omp-sample-b.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(x)
            for x in [header, init, thinking, fork_point, branch_1, branch_2]
        )
        + "\n"
    )
    return path


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session_a = _session_a(tmp_path)
        session_b = _session_b(tmp_path)

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)
        ingest_omp_session(db, session_a)
        ingest_omp_session(db, session_b)

        total = db.execute("SELECT count(*) FROM records").fetchone()
        assert total is not None
        total_count = total[0]

        surrogate = db.execute(
            "SELECT count(*) FROM records WHERE uuid LIKE 'nouuid_%'"
        ).fetchone()
        assert surrogate is not None
        surrogate_count = surrogate[0]

        parent_populated = db.execute(
            "SELECT count(*) FROM records WHERE parent_uuid IS NOT NULL"
        ).fetchone()
        assert parent_populated is not None
        parent_populated_count = parent_populated[0]

        role_typed = db.execute(
            "SELECT count(*) FROM records WHERE type IN ('user', 'assistant')"
        ).fetchone()
        assert role_typed is not None
        role_typed_count = role_typed[0]

        db.close()

    percent_records_with_surrogate_uuid = (
        100.0 * surrogate_count / total_count if total_count else 0.0
    )
    percent_records_with_parent_populated = (
        100.0 * parent_populated_count / total_count if total_count else 0.0
    )

    print(
        json.dumps(
            {
                "percent_records_with_surrogate_uuid": percent_records_with_surrogate_uuid,
                "percent_records_with_parent_populated": percent_records_with_parent_populated,
                "role_typed_record_count": role_typed_count,
            }
        )
    )


if __name__ == "__main__":
    main()
