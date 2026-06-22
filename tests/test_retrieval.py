"""Tests for the retrieval surface (pebble is-565.1).

get_full_session() is the on-demand FETCH primitive: given a session_id it
reassembles the complete transcript in chronological order, rehydrating
every ``[Large Content: file_...]`` marker from the blob store so a labbook
``session_id`` citation resolves to the actual content.
"""

import json
from pathlib import Path

import duckdb

from ingest_sessions.core import ingest_jsonl
from ingest_sessions.retrieval import format_session_text, get_full_session


def _record(
    uuid: str,
    session_id: str,
    content: object,
    *,
    rtype: str = "assistant",
    timestamp: str = "2026-03-01T12:00:00.000Z",
) -> str:
    """Build a JSONL line."""
    return json.dumps(
        {
            "uuid": uuid,
            "sessionId": session_id,
            "type": rtype,
            "timestamp": timestamp,
            "parentUuid": None,
            "message": {"role": rtype, "content": content},
        }
    )


def test_empty_session_returns_no_records(db: duckdb.DuckDBPyConnection) -> None:
    result = get_full_session(db, "nonexistent")
    assert result["session_id"] == "nonexistent"
    assert result["record_count"] == 0
    assert result["records"] == []
    assert result["missing_blobs"] == []


def test_records_returned_in_chronological_order(
    db: duckdb.DuckDBPyConnection, blob_root: Path
) -> None:
    jsonl = blob_root.parent / "sess-ord.jsonl"
    jsonl.write_text(
        _record("c", "sess-ord", "third", timestamp="2026-03-01T12:00:03.000Z")
        + "\n"
        + _record("a", "sess-ord", "first", timestamp="2026-03-01T12:00:01.000Z")
        + "\n"
        + _record("b", "sess-ord", "second", timestamp="2026-03-01T12:00:02.000Z")
        + "\n"
    )
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    result = get_full_session(db, "sess-ord", blob_root=blob_root)
    assert result["record_count"] == 3
    contents = [r["raw"]["message"]["content"] for r in result["records"]]
    assert contents == ["first", "second", "third"]


def test_blob_marker_rehydrated(db: duckdb.DuckDBPyConnection, blob_root: Path) -> None:
    """A session whose content exceeds the inline blob threshold round-trips."""
    large = "x" * 150_000
    jsonl = blob_root.parent / "sess-blob.jsonl"
    jsonl.write_text(
        _record("a", "sess-blob", "intro", timestamp="2026-03-01T12:00:01.000Z")
        + "\n"
        + _record("b", "sess-blob", large, timestamp="2026-03-01T12:00:02.000Z")
        + "\n"
    )
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    # The stored record must hold a marker, not the content (precondition).
    stored = db.execute("SELECT raw FROM records WHERE uuid = 'b'").fetchone()
    assert stored is not None
    assert "[Large Content:" in stored[0]

    result = get_full_session(db, "sess-blob", blob_root=blob_root)
    assert result["missing_blobs"] == []
    rehydrated = result["records"][1]["raw"]["message"]["content"]
    assert rehydrated == large
    assert "[Large Content:" not in rehydrated


def test_blob_marker_in_nested_block_rehydrated(
    db: duckdb.DuckDBPyConnection, blob_root: Path
) -> None:
    """Markers inside content-block lists and tool_results are rehydrated."""
    large = "z" * 150_000
    blocks = [
        {"type": "text", "text": "preamble"},
        {"type": "tool_result", "tool_use_id": "t1", "content": large},
    ]
    jsonl = blob_root.parent / "sess-nest.jsonl"
    jsonl.write_text(_record("a", "sess-nest", blocks) + "\n")
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    result = get_full_session(db, "sess-nest", blob_root=blob_root)
    content = result["records"][0]["raw"]["message"]["content"]
    assert content[0]["text"] == "preamble"
    assert content[1]["content"] == large
    assert result["missing_blobs"] == []


def test_include_blobs_false_leaves_markers(
    db: duckdb.DuckDBPyConnection, blob_root: Path
) -> None:
    large = "x" * 150_000
    jsonl = blob_root.parent / "sess-nob.jsonl"
    jsonl.write_text(_record("b", "sess-nob", large) + "\n")
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    result = get_full_session(db, "sess-nob", include_blobs=False, blob_root=blob_root)
    content = result["records"][0]["raw"]["message"]["content"]
    assert "[Large Content:" in content


def test_missing_blob_reported_marker_preserved(
    db: duckdb.DuckDBPyConnection, blob_root: Path
) -> None:
    """If a referenced blob file is gone, the marker stays and is reported."""
    large = "x" * 150_000
    jsonl = blob_root.parent / "sess-miss.jsonl"
    jsonl.write_text(_record("b", "sess-miss", large) + "\n")
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    # Delete the blob from disk to simulate loss.
    for p in blob_root.rglob("file_*"):
        p.unlink()

    result = get_full_session(db, "sess-miss", blob_root=blob_root)
    content = result["records"][0]["raw"]["message"]["content"]
    assert "[Large Content:" in content  # marker preserved, not silently blanked
    assert len(result["missing_blobs"]) == 1
    assert result["missing_blobs"][0].startswith("file_")


def test_format_session_text_is_chronological_plaintext(
    db: duckdb.DuckDBPyConnection, blob_root: Path
) -> None:
    jsonl = blob_root.parent / "sess-fmt.jsonl"
    jsonl.write_text(
        _record(
            "a",
            "sess-fmt",
            "hello",
            rtype="user",
            timestamp="2026-03-01T12:00:01.000Z",
        )
        + "\n"
        + _record(
            "b",
            "sess-fmt",
            "hi there",
            rtype="assistant",
            timestamp="2026-03-01T12:00:02.000Z",
        )
        + "\n"
    )
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    result = get_full_session(db, "sess-fmt", blob_root=blob_root)
    text = format_session_text(result)
    assert "hello" in text
    assert "hi there" in text
    assert text.index("hello") < text.index("hi there")
