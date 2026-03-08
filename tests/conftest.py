"""Shared test fixtures for ingest_sessions."""

from pathlib import Path

import duckdb
import pytest

from ingest_sessions.core import create_tables


@pytest.fixture
def db(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with all tables created."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    create_tables(conn)
    return conn


@pytest.fixture
def blob_root(tmp_path: Path) -> Path:
    """Temporary blob storage root directory."""
    return tmp_path / "blobs"
