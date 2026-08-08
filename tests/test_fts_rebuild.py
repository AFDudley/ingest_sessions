"""Unit tests for the off-thread / atomic / throttled FTS rebuild (pebble is-e10).

`core.rebuild_fts_index` used to run the whole rebuild on the single DB thread:
the ~45-min ``json.loads`` + ``record_text`` parse over ~873k rows blocked every
query for the whole window, and a non-transactional DELETE+INSERT could leave
``record_fts`` partial against a stale index snapshot. These tests pin the three
pieces of the fix at the pure-function level:

  * ``project_record_texts`` — the expensive parse, now a pure function the
    server runs OFF the DB thread; it must equal the old inline mapping.
  * ``write_fts_index`` — the DELETE+INSERT+create_fts_index now run in ONE
    transaction, so a failure mid-write rolls back to the prior consistent
    ``(record_fts, index)`` state (atomicity by construction).
  * ``_should_rebuild_fts`` — the pure throttle decision the periodic sweep
    uses to stop re-triggering the costly rebuild every embedding cycle.
"""

from __future__ import annotations

import json

import duckdb
import pytest

import ingest_sessions.server as srv
from ingest_sessions.core import (
    append_fts_rows,
    fetch_record_raws,
    fetch_unindexed_record_raws,
    project_record_texts,
    rebuild_fts_index,
    rebuild_fts_snapshot,
    write_fts_index,
)
from ingest_sessions.embeddings import record_text
from ingest_sessions.retrieval import search_lexical


def _user_record(uuid: str, session_id: str, text: str) -> tuple:
    raw = {
        "uuid": uuid,
        "sessionId": session_id,
        "type": "user",
        "message": {"role": "user", "content": text},
    }
    return (uuid, session_id, "user", None, None, json.dumps(raw))


def _insert(db: duckdb.DuckDBPyConnection, *rows: tuple) -> None:
    db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)", rows)


def _fts_count(db: duckdb.DuckDBPyConnection) -> int:
    row = db.execute("SELECT count(*) FROM record_fts").fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# project_record_texts — the pure off-thread parse
# ---------------------------------------------------------------------------


def test_project_record_texts_equals_inline_record_text() -> None:
    """The off-thread projection must equal the old inline (uuid, text) mapping.

    This is the equivalence the off-thread move relies on: moving the parse to a
    worker thread must not change a single produced text, or the lexical and
    vector arms would diverge.
    """
    raws = [
        ("a", json.dumps({"message": {"role": "user", "content": "quick brown fox"}})),
        ("b", json.dumps({"message": {"role": "assistant", "content": "zorblax"}})),
        ("c", json.dumps({"message": {"role": "user", "content": ""}})),
        ("d", json.dumps({"not_a_message": True})),
    ]
    projected = project_record_texts(raws)
    expected = [(uuid, record_text(json.loads(raw))) for uuid, raw in raws]
    assert projected == expected


def test_project_record_texts_empty() -> None:
    assert project_record_texts([]) == []


def test_fetch_then_project_matches_rebuild(db: duckdb.DuckDBPyConnection) -> None:
    """fetch_record_raws → project_record_texts produces exactly what the index
    is built over — the server's split (fetch on DB thread, project off it) is
    behaviourally identical to the monolithic rebuild."""
    _insert(
        db,
        _user_record("a", "s1", "the quick brown fox"),
        _user_record("b", "s2", "configuring the zorblax cache"),
    )
    projection = project_record_texts(fetch_record_raws(db))
    write_fts_index(db, projection)
    stored = db.execute("SELECT uuid, text FROM record_fts ORDER BY uuid").fetchall()
    assert stored == sorted(projection)


# ---------------------------------------------------------------------------
# write_fts_index — single-transaction atomicity
# ---------------------------------------------------------------------------


def test_write_fts_index_happy_path(db: duckdb.DuckDBPyConnection) -> None:
    _insert(
        db,
        _user_record("a", "s1", "the quick brown fox"),
        _user_record("b", "s2", "configuring the zorblax cache"),
    )
    n = write_fts_index(db, project_record_texts(fetch_record_raws(db)))
    assert n == 2
    hits = search_lexical(db, "zorblax")
    assert [h["uuid"] for h in hits] == ["b"]


def test_write_fts_index_rolls_back_on_failure(db: duckdb.DuckDBPyConnection) -> None:
    """A failure mid-write leaves record_fts AND the index at the prior state.

    This is the atomicity fix: the DELETE + INSERT + create_fts_index run in one
    transaction, so an interrupt rolls them back together — the partial
    ``record_fts`` vs stale-index inconsistency of is-e10 is unrepresentable.

    The failure is forced with a projection containing a DUPLICATE uuid, which
    violates ``record_fts``'s primary key on INSERT (a real constraint failure,
    not a mock) after the DELETE has already run inside the transaction.
    """
    _insert(
        db,
        _user_record("a", "s1", "the quick brown fox"),
        _user_record("b", "s2", "configuring the zorblax cache"),
    )
    # Establish a good, committed prior index.
    rebuild_fts_index(db)
    assert [h["uuid"] for h in search_lexical(db, "zorblax")] == ["b"]

    # Duplicate uuid 'a' -> PK violation on the second INSERT, mid-transaction.
    bad_projection = [("a", "wibble wobble"), ("a", "second a row")]
    with pytest.raises(duckdb.Error):
        write_fts_index(db, bad_projection)

    # Rolled back: record_fts unchanged (still 2 rows), the OLD query still
    # works, and the would-be new term is absent — table + index consistent.
    assert _fts_count(db) == 2
    assert [h["uuid"] for h in search_lexical(db, "zorblax")] == ["b"]
    assert search_lexical(db, "wibble") == []


def test_write_fts_index_empty_projection(db: duckdb.DuckDBPyConnection) -> None:
    """An empty corpus builds an empty index (no rows), not an error."""
    assert write_fts_index(db, []) == 0
    assert _fts_count(db) == 0
    assert search_lexical(db, "anything") == []


# ---------------------------------------------------------------------------
# append_fts_rows / rebuild_fts_snapshot — the incremental refresh the sweep uses
#
# The full REPLACE form re-writes every row on every pass, and bundles that
# write with the whole-corpus create_fts_index into ONE op. Over the DB
# thread's per-op budget it was interrupted, rolled back, and retried on the
# next sweep forever: the drift never shrank and nothing was ever indexed. The
# sweep must instead drain the drift in COMMITTED batches, then snapshot once.
# ---------------------------------------------------------------------------


def test_fetch_unindexed_returns_only_records_missing_from_fts(
    db: duckdb.DuckDBPyConnection,
) -> None:
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, project_record_texts(fetch_record_raws(db)))

    _insert(db, _user_record("b", "s2", "configuring the zorblax cache"))
    assert [uuid for uuid, _raw in fetch_unindexed_record_raws(db)] == ["b"]


def test_fetch_unindexed_respects_the_batch_limit(
    db: duckdb.DuckDBPyConnection,
) -> None:
    _insert(
        db,
        _user_record("a", "s1", "one"),
        _user_record("b", "s2", "two"),
        _user_record("c", "s3", "three"),
    )
    assert len(fetch_unindexed_record_raws(db, limit=2)) == 2


def test_appended_rows_are_committed_so_the_drift_shrinks(
    db: duckdb.DuckDBPyConnection,
) -> None:
    """Each batch leaves the anti-join permanently smaller.

    This is the termination property: if the write were rolled into the same
    transaction as the index snapshot, an over-budget interrupt would undo it
    and the next pass would re-select the same rows indefinitely.
    """
    _insert(
        db,
        _user_record("a", "s1", "one"),
        _user_record("b", "s2", "two"),
        _user_record("c", "s3", "three"),
    )

    batch = fetch_unindexed_record_raws(db, limit=2)
    assert append_fts_rows(db, project_record_texts(batch)) == 2
    assert [uuid for uuid, _raw in fetch_unindexed_record_raws(db)] == ["c"]

    batch = fetch_unindexed_record_raws(db, limit=2)
    assert append_fts_rows(db, project_record_texts(batch)) == 1
    assert fetch_unindexed_record_raws(db) == []


def test_snapshot_after_append_makes_new_and_old_rows_searchable(
    db: duckdb.DuckDBPyConnection,
) -> None:
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, project_record_texts(fetch_record_raws(db)))

    _insert(db, _user_record("b", "s2", "configuring the zorblax cache"))
    append_fts_rows(db, project_record_texts(fetch_unindexed_record_raws(db)))

    assert rebuild_fts_snapshot(db) == 2
    assert [h["uuid"] for h in search_lexical(db, "zorblax")] == ["b"]
    assert [h["uuid"] for h in search_lexical(db, "quick")] == ["a"]


def test_appending_without_a_snapshot_leaves_old_hits_resolvable(
    db: duckdb.DuckDBPyConnection,
) -> None:
    """Appending alone never produces a hit that resolves to nothing.

    record_fts only grows, so an index built before the append covers a subset
    of the table: results are incomplete until the next snapshot, never
    dangling. That asymmetry is what makes splitting the write from the
    snapshot safe, where the DELETE-first replace must stay transactional.
    """
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, project_record_texts(fetch_record_raws(db)))

    _insert(db, _user_record("b", "s2", "configuring the zorblax cache"))
    append_fts_rows(db, project_record_texts(fetch_unindexed_record_raws(db)))

    assert [h["uuid"] for h in search_lexical(db, "quick")] == ["a"]
    assert search_lexical(db, "zorblax") == []


def test_snapshot_with_nothing_appended_leaves_the_index_usable(
    db: duckdb.DuckDBPyConnection,
) -> None:
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, project_record_texts(fetch_record_raws(db)))

    assert append_fts_rows(db, []) == 0
    assert rebuild_fts_snapshot(db) == 1
    assert [h["uuid"] for h in search_lexical(db, "quick")] == ["a"]


# ---------------------------------------------------------------------------
# _run_sync_async — which refresh each caller gets
# ---------------------------------------------------------------------------


def _sync_harness(
    db: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run `_run_sync_async`'s DB ops inline against *db*, with embedding stubbed.

    The sweep's embedding arm is irrelevant here and would load ~180MB of ONNX;
    only its "did anything change?" signal matters, because that is what gates
    the FTS refresh.
    """

    async def _execute(fn):  # type: ignore[no-untyped-def]
        return fn(db)

    monkeypatch.setattr(srv, "_db_execute", _execute)
    monkeypatch.setattr(srv, "embed_texts", lambda texts: [[0.0] * 384] * len(texts))
    monkeypatch.setattr(srv, "_backfill_running", False)
    monkeypatch.setattr(srv, "_last_fts_rebuild_at", None)


@pytest.mark.asyncio
async def test_periodic_sync_indexes_new_records_without_rewriting_old_ones(
    db: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-force sweep adds the missing record and leaves indexed rows alone.

    'a' is deliberately indexed under text that does NOT match its projection.
    A full replace would re-derive and overwrite it; the incremental refresh
    the sweep must use skips every uuid already in `record_fts`, so the
    planted text survives. That is the observable difference between the two
    refresh forms.
    """
    _sync_harness(db, monkeypatch)
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, [("a", "planted placeholder")])
    _insert(db, _user_record("b", "s2", "configuring the zorblax cache"))

    result = await srv._run_sync_async(throttle_fts=True)

    assert result["fts_rebuilt"] is True
    assert _fts_count(db) == 2
    assert [h["uuid"] for h in search_lexical(db, "zorblax")] == ["b"]
    assert [h["uuid"] for h in search_lexical(db, "planted")] == ["a"]
    assert search_lexical(db, "quick") == []


@pytest.mark.asyncio
async def test_force_fts_re_derives_every_row(
    db: duckdb.DuckDBPyConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """force_fts stays the repair path: it rewrites rows the extend would skip."""
    _sync_harness(db, monkeypatch)
    _insert(db, _user_record("a", "s1", "the quick brown fox"))
    write_fts_index(db, [("a", "planted placeholder")])

    result = await srv._run_sync_async(force_fts=True)

    assert result["fts_rebuilt"] is True
    assert _fts_count(db) == 1
    assert [h["uuid"] for h in search_lexical(db, "quick")] == ["a"]
    assert search_lexical(db, "planted") == []


# ---------------------------------------------------------------------------
# _should_rebuild_fts — the pure throttle decision
# ---------------------------------------------------------------------------


def test_should_rebuild_fts_force_always() -> None:
    # force=True rebuilds even with 0 embedded and within the interval — the
    # operator repair path.
    assert (
        srv._should_rebuild_fts(
            embedded=0, throttle=True, force=True, now=0.0, last=0.0, min_interval=3600
        )
        is True
    )


def test_should_rebuild_fts_no_new_records() -> None:
    # No new record text -> nothing to reindex (unless forced).
    assert (
        srv._should_rebuild_fts(
            embedded=0,
            throttle=False,
            force=False,
            now=10.0,
            last=None,
            min_interval=3600,
        )
        is False
    )


def test_should_rebuild_fts_manual_not_throttled() -> None:
    # Manual /api/sync (throttle=False): any new records rebuild, no throttle.
    assert (
        srv._should_rebuild_fts(
            embedded=5,
            throttle=False,
            force=False,
            now=10.0,
            last=0.0,
            min_interval=3600,
        )
        is True
    )


def test_should_rebuild_fts_throttled_first_time() -> None:
    # Throttled but never rebuilt this process (last=None) -> rebuild.
    assert (
        srv._should_rebuild_fts(
            embedded=5,
            throttle=True,
            force=False,
            now=10.0,
            last=None,
            min_interval=3600,
        )
        is True
    )


def test_should_rebuild_fts_throttled_within_interval() -> None:
    # Throttled, new records, but < min_interval since the last rebuild -> skip.
    assert (
        srv._should_rebuild_fts(
            embedded=5,
            throttle=True,
            force=False,
            now=100.0,
            last=0.0,
            min_interval=3600,
        )
        is False
    )


def test_should_rebuild_fts_throttled_interval_elapsed() -> None:
    # Throttled, new records, >= min_interval since the last rebuild -> rebuild.
    assert (
        srv._should_rebuild_fts(
            embedded=5,
            throttle=True,
            force=False,
            now=4000.0,
            last=0.0,
            min_interval=3600,
        )
        is True
    )
