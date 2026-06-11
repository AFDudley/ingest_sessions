"""Capture-invariant tests (pebble is-a1f).

The invariant: every non-blank line in an ingested JSONL file lands in
exactly one of two tables — parseable dict records in ``records``,
everything else in ``malformed_lines``.  Duplicate uuids dedupe; nothing
is silently dropped; blob-offloaded content stays reachable via the
``read_blob`` SQL function.
"""

import json
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import (
    create_tables,
    ingest_jsonl,
    migrate_capture_v2,
    register_functions,
)


def _scalar(db: duckdb.DuckDBPyConnection, sql: str) -> Any:
    """Fetch a single scalar; asserts the query returned a row (mypy narrowing)."""
    row = db.execute(sql).fetchone()
    assert row is not None
    return row[0]


def _record_line(uuid: str | None, session_id: str, content: str) -> str:
    """Build a JSONL line; omits the uuid key entirely when uuid is None."""
    rec = {
        "sessionId": session_id,
        "type": "user",
        "timestamp": "2026-06-11T12:00:00.000Z",
        "parentUuid": None,
        "message": {"role": "user", "content": content},
    }
    if uuid is not None:
        rec["uuid"] = uuid
    return json.dumps(rec)


def _write_session(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n")


SESSION = "sess-cap"


def _build_lines() -> list[str]:
    """9 non-blank lines: 4 unique records, 3 uuid-less, 1 malformed, 1 dup."""
    large = "x" * 150_000
    return [
        _record_line("u1", SESSION, "first"),
        _record_line("u2", SESSION, "second"),
        _record_line("u3", SESSION, "third"),
        _record_line(None, SESSION, "no uuid one"),
        _record_line(None, SESSION, "no uuid two"),
        _record_line(None, SESSION, "no uuid three"),
        _record_line("u4", SESSION, large),  # blob-extracted
        '{"truncated": "json line without closing brace',  # malformed
        _record_line("u1", SESSION, "duplicate uuid"),  # PK dup, ignored
    ]


def test_every_line_lands_somewhere(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """non-blank lines == records + malformed + PK duplicates."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    lines = _build_lines()
    _write_session(jsonl, lines)

    count, bytes_read = ingest_jsonl(db, jsonl, blob_root=blob_root)

    records = _scalar(db, "SELECT count(*) FROM records")
    malformed = _scalar(db, "SELECT count(*) FROM malformed_lines")
    assert records == 7  # u1-u4 + 3 surrogate-keyed
    assert malformed == 1
    assert count == 8  # batch included the dup; PK dedup happens in the DB
    assert bytes_read == jsonl.stat().st_size
    # The invariant: every non-blank line accounted for.
    duplicates = 1
    assert len(lines) == records + malformed + duplicates


def test_uuidless_lines_all_survive(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Distinct uuid-less lines get distinct surrogate keys — none collapse."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    _write_session(jsonl, _build_lines())

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    rows = db.execute(
        "SELECT uuid, raw FROM records WHERE uuid LIKE 'nouuid_%' ORDER BY uuid"
    ).fetchall()
    assert len(rows) == 3
    contents = {json.loads(r[1])["message"]["content"] for r in rows}
    assert contents == {"no uuid one", "no uuid two", "no uuid three"}
    # No empty-string PK row exists.
    assert _scalar(db, "SELECT count(*) FROM records WHERE uuid = ''") == 0


def test_blob_content_reachable_via_sql(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Offloaded content is rehydratable with read_blob() in SQL."""
    register_functions(db, blob_root=blob_root)
    jsonl = tmp_path / f"{SESSION}.jsonl"
    _write_session(jsonl, _build_lines())

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    raw = _scalar(db, "SELECT raw FROM records WHERE uuid = 'u4'")
    content = json.loads(raw)["message"]["content"]
    assert content.startswith("[Large Content: file_")
    file_id = content.split("[Large Content: ")[1].split("]")[0]

    # Full SQL round-trip through the read_blob macro.
    assert _scalar(db, f"SELECT read_blob('{file_id}')") == "x" * 150_000
    # Unknown ids return NULL, not an error.
    assert _scalar(db, "SELECT read_blob('file_doesnotexist00')") is None


def test_untouched_lines_stored_verbatim(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Records not modified by blob extraction keep their original bytes."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    line = _record_line("u1", SESSION, "small")
    _write_session(jsonl, [line])

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    assert _scalar(db, "SELECT raw FROM records WHERE uuid = 'u1'") == line


def test_malformed_line_captured_with_offset(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Malformed lines are stored with file path and correct byte offset."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    good = _record_line("u1", SESSION, "ok")
    bad = "not json at all"
    _write_session(jsonl, [good, bad])

    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute(
        "SELECT file_path, byte_offset, line_text FROM malformed_lines"
    ).fetchone()
    assert row == (str(jsonl), len(good.encode()) + 1, bad)


def test_non_dict_json_captured_as_malformed(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Valid JSON that is not an object cannot be a record — captured, not crashed."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    _write_session(jsonl, ['"just a string"', "12345"])

    count, _ = ingest_jsonl(db, jsonl, blob_root=blob_root)

    assert count == 0
    assert _scalar(db, "SELECT count(*) FROM malformed_lines") == 2


def test_reingest_is_idempotent(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Full re-ingest from offset 0 changes nothing."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    _write_session(jsonl, _build_lines())

    ingest_jsonl(db, jsonl, blob_root=blob_root)
    before = db.execute(
        "SELECT (SELECT count(*) FROM records), (SELECT count(*) FROM malformed_lines)"
    ).fetchone()
    ingest_jsonl(db, jsonl, blob_root=blob_root)
    after = db.execute(
        "SELECT (SELECT count(*) FROM records), (SELECT count(*) FROM malformed_lines)"
    ).fetchone()
    assert before == after == (7, 1)


def test_incremental_append_captures_new_lines(
    db: duckdb.DuckDBPyConnection, blob_root: Path, tmp_path: Path
):
    """Appended lines (with and without uuid) ingest from the saved offset."""
    jsonl = tmp_path / f"{SESSION}.jsonl"
    _write_session(jsonl, _build_lines())
    _, bytes_read = ingest_jsonl(db, jsonl, blob_root=blob_root)

    with open(jsonl, "a") as f:
        f.write(_record_line("u5", SESSION, "appended") + "\n")
        f.write(_record_line(None, SESSION, "appended no uuid") + "\n")

    count, new_bytes = ingest_jsonl(
        db, jsonl, byte_offset=bytes_read, blob_root=blob_root
    )

    assert count == 2
    assert new_bytes == jsonl.stat().st_size
    assert _scalar(db, "SELECT count(*) FROM records") == 9


def test_migrate_capture_v2(tmp_path: Path):
    """Migration deletes the '' survivor row and forces full re-ingest."""
    conn = duckdb.connect(str(tmp_path / "old.duckdb"))
    create_tables(conn)  # sets the capture_v2 marker on a fresh DB

    # Simulate a pre-fix database: marker absent, '' row and mtimes present.
    conn.execute("DELETE FROM schema_meta WHERE key = 'capture_v2'")
    conn.execute(
        "INSERT INTO records VALUES ('', 'sess-old', 'user', NULL, NULL, '{}')"
    )
    conn.execute("INSERT INTO file_mtimes VALUES ('/tmp/sess-old.jsonl', 1, 100)")

    migrate_capture_v2(conn)

    assert _scalar(conn, "SELECT count(*) FROM records") == 0
    assert _scalar(conn, "SELECT count(*) FROM file_mtimes") == 0
    # Marker set; second run is a no-op.
    conn.execute("INSERT INTO file_mtimes VALUES ('/tmp/x.jsonl', 2, 50)")
    migrate_capture_v2(conn)
    assert _scalar(conn, "SELECT count(*) FROM file_mtimes") == 1
    conn.close()
