"""Unit tests for the supersession link store (pebble is-565.3).

The supersession table + API is the project-AGNOSTIC substrate for
supersession-aware ranking: a general link store that consumer-side
adapters (git-revert / tracker-close / ADR-amend detectors, which live
outside this repo) feed.  These tests pin the schema and the
functional-core API in core.py / supersession.py.
"""

from __future__ import annotations

import duckdb
import pytest

from ingest_sessions.supersession import (
    add_supersession,
    add_supersessions,
    get_superseded_ids,
    get_supersessions_for,
    is_superseded,
)


def _columns(db: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    rows = db.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return {name: dtype for name, dtype in rows}


def test_table_exists_with_expected_columns(db: duckdb.DuckDBPyConnection) -> None:
    cols = _columns(db, "supersessions")
    assert set(cols) == {
        "superseding_id",
        "superseded_id",
        "source",
        "created_at",
    }


def test_index_on_superseded_id_exists(db: duckdb.DuckDBPyConnection) -> None:
    indexes = db.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'supersessions'"
    ).fetchall()
    names = {row[0] for row in indexes}
    assert "idx_supersessions_superseded_id" in names


def test_add_supersession_inserts(db: duckdb.DuckDBPyConnection) -> None:
    inserted = add_supersession(db, "new-1", "old-1", "manual")
    assert inserted is True
    rows = db.execute(
        "SELECT superseding_id, superseded_id, source FROM supersessions"
    ).fetchall()
    assert rows == [("new-1", "old-1", "manual")]


def test_add_supersession_preserves_source(db: duckdb.DuckDBPyConnection) -> None:
    add_supersession(db, "new-1", "old-1", "git-revert")
    row = db.execute(
        "SELECT source FROM supersessions WHERE superseding_id = 'new-1'"
    ).fetchone()
    assert row is not None
    assert row[0] == "git-revert"


def test_add_supersession_sets_created_at(db: duckdb.DuckDBPyConnection) -> None:
    add_supersession(db, "new-1", "old-1", "manual")
    row = db.execute(
        "SELECT created_at FROM supersessions WHERE superseding_id = 'new-1'"
    ).fetchone()
    assert row is not None
    assert isinstance(row[0], int)
    assert row[0] > 0


def test_add_supersession_idempotent(db: duckdb.DuckDBPyConnection) -> None:
    first = add_supersession(db, "new-1", "old-1", "manual")
    second = add_supersession(db, "new-1", "old-1", "manual")
    assert first is True
    assert second is False
    count = db.execute("SELECT count(*) FROM supersessions").fetchone()
    assert count is not None
    assert count[0] == 1


def test_add_supersession_rejects_self_link(db: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(ValueError):
        add_supersession(db, "same", "same", "manual")
    count = db.execute("SELECT count(*) FROM supersessions").fetchone()
    assert count is not None
    assert count[0] == 0


def test_is_superseded_true_and_false(db: duckdb.DuckDBPyConnection) -> None:
    add_supersession(db, "new-1", "old-1", "manual")
    assert is_superseded(db, "old-1") is True
    assert is_superseded(db, "new-1") is False
    assert is_superseded(db, "unknown") is False


def test_get_superseded_ids(db: duckdb.DuckDBPyConnection) -> None:
    add_supersession(db, "new-1", "old-1", "manual")
    add_supersession(db, "new-2", "old-2", "derived")
    add_supersession(db, "new-3", "old-1", "manual")  # old-1 superseded twice
    assert get_superseded_ids(db) == {"old-1", "old-2"}


def test_get_superseded_ids_empty(db: duckdb.DuckDBPyConnection) -> None:
    assert get_superseded_ids(db) == set()


def test_add_supersessions_batch_returns_new_count(
    db: duckdb.DuckDBPyConnection,
) -> None:
    links = [
        ("new-1", "old-1", "manual"),
        ("new-2", "old-2", "derived"),
        ("new-3", "old-3", "git-revert"),
    ]
    count = add_supersessions(db, links)
    assert count == 3
    total = db.execute("SELECT count(*) FROM supersessions").fetchone()
    assert total is not None
    assert total[0] == 3


def test_add_supersessions_idempotent_on_rerun(db: duckdb.DuckDBPyConnection) -> None:
    links = [
        ("new-1", "old-1", "manual"),
        ("new-2", "old-2", "derived"),
    ]
    first = add_supersessions(db, links)
    second = add_supersessions(db, links)
    assert first == 2
    assert second == 0
    total = db.execute("SELECT count(*) FROM supersessions").fetchone()
    assert total is not None
    assert total[0] == 2


def test_add_supersessions_partial_new(db: duckdb.DuckDBPyConnection) -> None:
    add_supersessions(db, [("new-1", "old-1", "manual")])
    count = add_supersessions(
        db,
        [
            ("new-1", "old-1", "manual"),  # existing
            ("new-2", "old-2", "derived"),  # new
        ],
    )
    assert count == 1


def test_add_supersessions_rejects_self_link(db: duckdb.DuckDBPyConnection) -> None:
    with pytest.raises(ValueError):
        add_supersessions(db, [("a", "b", "manual"), ("same", "same", "manual")])


def test_get_supersessions_for(db: duckdb.DuckDBPyConnection) -> None:
    add_supersession(db, "new-1", "old-1", "manual")
    add_supersession(db, "new-2", "old-1", "git-revert")
    add_supersession(db, "new-3", "old-2", "derived")

    rows = get_supersessions_for(db, "old-1")
    superseding = {r["superseding_id"] for r in rows}
    assert superseding == {"new-1", "new-2"}
    for r in rows:
        assert r["superseded_id"] == "old-1"
        assert "source" in r
        assert "created_at" in r
        assert isinstance(r["created_at"], int)


def test_get_supersessions_for_none(db: duckdb.DuckDBPyConnection) -> None:
    assert get_supersessions_for(db, "unknown") == []
