"""Tests for the ingest_sessions CLI batch tool."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingest_sessions.cli import IngestConfig, ingest, load_profile, resolve_project_dirs


def _scalar(db: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """Execute SQL and return the first column of the first row."""
    row = db.execute(sql).fetchone()
    assert row is not None
    return row[0]


SAMPLE_RECORD = {
    "uuid": "abc-123",
    "sessionId": "sess-001",
    "type": "user",
    "timestamp": "2026-03-01T12:00:00.000Z",
    "parentUuid": None,
    "message": {"role": "user", "content": "hello"},
}

SESSIONS_INDEX = {
    "entries": [
        {
            "sessionId": "sess-001",
            "summary": "Test session",
            "firstPrompt": "hello",
            "messageCount": 1,
            "created": "2026-03-01T12:00:00.000Z",
            "modified": "2026-03-01T12:00:00.000Z",
            "gitBranch": "main",
            "projectPath": "/tmp/test",
        }
    ]
}


class TestResolveProjectDirs:
    def test_wildcard_returns_all_dirs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        projects.mkdir()
        (projects / "proj1").mkdir()
        (projects / "proj2").mkdir()
        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)
        dirs = resolve_project_dirs("*")
        assert len(dirs) == 2

    def test_named_projects(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        projects.mkdir()
        (projects / "proj1").mkdir()
        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)
        dirs = resolve_project_dirs(["proj1"])
        assert len(dirs) == 1
        assert dirs[0].name == "proj1"

    def test_missing_project_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        projects.mkdir()
        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)
        with pytest.raises(ValueError, match="not found"):
            resolve_project_dirs(["nonexistent"])

    def test_invalid_spec_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            resolve_project_dirs(42)  # type: ignore[arg-type]


class TestIngest:
    def test_basic_ingestion(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        proj = projects / "myproject"
        proj.mkdir(parents=True)
        (proj / "sess-001.jsonl").write_text(json.dumps(SAMPLE_RECORD) + "\n")
        (proj / "sessions-index.json").write_text(json.dumps(SESSIONS_INDEX))

        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)

        db_path = tmp_path / "out.duckdb"
        config = IngestConfig(output=db_path, projects="*", include_history=False)
        ingest(config)

        db = duckdb.connect(str(db_path))
        assert _scalar(db, "SELECT count(*) FROM records") == 1
        assert _scalar(db, "SELECT summary FROM sessions") == "Test session"
        db.close()

    def test_content_filter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        proj = projects / "myproject"
        proj.mkdir(parents=True)

        r1 = {
            **SAMPLE_RECORD,
            "uuid": "match",
            "message": {"content": "target keyword"},
        }
        r2 = {**SAMPLE_RECORD, "uuid": "skip", "message": {"content": "other"}}
        (proj / "match.jsonl").write_text(json.dumps(r1) + "\n")
        (proj / "skip.jsonl").write_text(json.dumps(r2) + "\n")

        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)

        db_path = tmp_path / "out.duckdb"
        config = IngestConfig(
            output=db_path,
            projects="*",
            content_filter="target",
            include_history=False,
        )
        ingest(config)

        db = duckdb.connect(str(db_path))
        assert _scalar(db, "SELECT count(*) FROM records") == 1
        db.close()

    def test_history_ingestion(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projects = tmp_path / "projects"
        projects.mkdir()

        history = tmp_path / "history.jsonl"
        entry = {
            "timestamp": 100,
            "display": "cmd",
            "sessionId": "s1",
            "project": "p1",
        }
        history.write_text(json.dumps(entry) + "\n")

        monkeypatch.setattr("ingest_sessions.cli._claude_dir", lambda: tmp_path)

        db_path = tmp_path / "out.duckdb"
        config = IngestConfig(output=db_path, projects="*", include_history=True)
        ingest(config)

        db = duckdb.connect(str(db_path))
        assert _scalar(db, "SELECT count(*) FROM history") == 1
        db.close()


class TestLoadProfile:
    def test_missing_config_exits(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(
            "ingest_sessions.cli._config_file",
            lambda: tmp_path / "nonexistent.toml",
        )
        with pytest.raises(SystemExit):
            load_profile("default")

    def test_missing_profile_exits(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        config = tmp_path / "profiles.toml"
        config.write_text('[other]\noutput = "/tmp/out.db"\n')
        monkeypatch.setattr("ingest_sessions.cli._config_file", lambda: config)
        with pytest.raises(SystemExit):
            load_profile("default")

    def test_loads_valid_profile(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        config = tmp_path / "profiles.toml"
        config.write_text('[myprofile]\noutput = "/tmp/out.db"\n')
        monkeypatch.setattr("ingest_sessions.cli._config_file", lambda: config)
        result = load_profile("myprofile")
        assert result["output"] == "/tmp/out.db"
