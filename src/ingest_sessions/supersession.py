"""Supersession link store for ingest_sessions (pebble is-565.3).

The GENERAL, project-AGNOSTIC substrate for supersession-aware ranking. A
supersession link records that one record (``superseding_id``) supersedes
another (``superseded_id``) — newer reasoning displacing older. The
ranking layer (sibling work) reads these links to down-weight or flag the
superseded records.

This module is deliberately *dumb* about provenance: ``source`` is an
opaque tag (``'manual'``, ``'derived'``, ``'git-revert'``,
``'tracker-close'``, ``'adr-amend'``, …) supplied by consumer-side
adapters that live OUTSIDE this repo. We never parse git reverts, issue
closes, or ADR amendments here — we only store the links such adapters
ingest and answer queries over them.

Functional-core style: the DuckDB handle is passed in; functions take data
and return data. The only control-flow exception is the explicit
fail-fast ValueError on a self-link (a record cannot supersede itself).

# See .claude doctrine: functional core, imperative shell — these are the
# data-in/data-out query+ingest primitives over the supersessions table
# defined in core.create_tables().
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Any

import duckdb

SupersessionLink = tuple[str, str, str]
"""A (superseding_id, superseded_id, source) tuple as ingested by adapters."""


def _now_ms() -> int:
    """Current wall-clock time in milliseconds (the boundary clock read)."""
    return int(time.time() * 1000)


def _reject_self_link(superseding_id: str, superseded_id: str) -> None:
    """Fail fast on a self-link — a record cannot supersede itself."""
    if superseding_id == superseded_id:
        raise ValueError(f"a record cannot supersede itself: {superseding_id!r}")


def add_supersession(
    db: duckdb.DuckDBPyConnection,
    superseding_id: str,
    superseded_id: str,
    source: str,
) -> bool:
    """Insert a single supersession link, idempotently.

    Returns True if a new row was inserted, False if the link already
    existed (same superseding/superseded pair). Raises ValueError on a
    self-link.
    """
    _reject_self_link(superseding_id, superseded_id)
    before = _link_count(db)
    db.execute(
        "INSERT OR IGNORE INTO supersessions "
        "(superseding_id, superseded_id, source, created_at) "
        "VALUES (?, ?, ?, ?)",
        [superseding_id, superseded_id, source, _now_ms()],
    )
    return _link_count(db) > before


def add_supersessions(
    db: duckdb.DuckDBPyConnection,
    links: Iterable[SupersessionLink],
) -> int:
    """Batch-ingest supersession links, idempotently.

    Returns the count of newly-inserted rows (existing links are skipped by
    INSERT OR IGNORE and not counted). Raises ValueError on the first
    self-link encountered — offending tuples are not silently dropped.
    """
    rows = list(links)
    for superseding_id, superseded_id, _source in rows:
        _reject_self_link(superseding_id, superseded_id)
    if not rows:
        return 0
    now = _now_ms()
    before = _link_count(db)
    db.executemany(
        "INSERT OR IGNORE INTO supersessions "
        "(superseding_id, superseded_id, source, created_at) "
        "VALUES (?, ?, ?, ?)",
        [(sup, sub, src, now) for sup, sub, src in rows],
    )
    return _link_count(db) - before


def is_superseded(db: duckdb.DuckDBPyConnection, record_id: str) -> bool:
    """True iff *record_id* appears as a superseded_id in any link."""
    row = db.execute(
        "SELECT 1 FROM supersessions WHERE superseded_id = ? LIMIT 1",
        [record_id],
    ).fetchone()
    return row is not None


def get_superseded_ids(db: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the full set of superseded record ids (bulk down-weight input)."""
    rows = db.execute("SELECT DISTINCT superseded_id FROM supersessions").fetchall()
    return {row[0] for row in rows}


def get_supersessions_for(
    db: duckdb.DuckDBPyConnection,
    record_id: str,
) -> list[dict[str, Any]]:
    """Return the links where *record_id* is the superseded_id.

    Each row is a dict with superseding_id, superseded_id, source and
    created_at — i.e. who superseded *record_id*, with provenance and time.
    Ordered newest-first for stable presentation.
    """
    rows = db.execute(
        "SELECT superseding_id, superseded_id, source, created_at "
        "FROM supersessions WHERE superseded_id = ? "
        "ORDER BY created_at DESC, superseding_id",
        [record_id],
    ).fetchall()
    return [
        {
            "superseding_id": superseding_id,
            "superseded_id": superseded_id,
            "source": source,
            "created_at": created_at,
        }
        for superseding_id, superseded_id, source, created_at in rows
    ]


def _link_count(db: duckdb.DuckDBPyConnection) -> int:
    """Total row count of the supersessions table (insert-delta helper)."""
    row = db.execute("SELECT count(*) FROM supersessions").fetchone()
    assert row is not None  # count(*) always returns exactly one row
    return int(row[0])
