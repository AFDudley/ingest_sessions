"""Tests for DAG maintenance logic.

Tests the database operations (insert/query summaries) without LLM calls.
The summarization functions are mocked.
"""

import json
from unittest.mock import patch

import duckdb

from ingest_sessions.dag import (
    BINDLE_THRESHOLD,
    SPRIG_CHUNK_SIZE,
    assemble_context_for_session,
    get_latest_summary_for_session,
    get_sprigs_for_session,
    get_unsummarized_messages,
    insert_bindle,
    insert_sprig,
    run_summarize_session,
)


def _seed_records(
    db: duckdb.DuckDBPyConnection, session_id: str, count: int
) -> list[str]:
    """Insert count records and return their UUIDs."""
    uuids = []
    for i in range(count):
        uuid = f"rec-{session_id}-{i:04d}"
        raw = json.dumps(
            {
                "message": {
                    "role": "user" if i % 2 == 0 else "assistant",
                    "content": f"Message {i} content here",
                },
            }
        )
        db.execute(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            [
                uuid,
                session_id,
                "user" if i % 2 == 0 else "assistant",
                f"2026-03-01T{10 + i // 60:02d}:{i % 60:02d}:00.000Z",
                None,
                raw,
            ],
        )
        uuids.append(uuid)
    return uuids


def test_get_unsummarized_messages_all_new(db: duckdb.DuckDBPyConnection):
    """All messages are unsummarized when no summaries exist."""
    uuids = _seed_records(db, "sess-1", 10)
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 10
    assert result[0]["uuid"] == uuids[0]


def test_get_unsummarized_messages_some_covered(db: duckdb.DuckDBPyConnection):
    """Messages covered by a sprig are excluded."""
    uuids = _seed_records(db, "sess-1", 10)
    insert_sprig(db, "sess-1", "test summary", uuids[:5])
    result = get_unsummarized_messages(db, "sess-1")
    assert len(result) == 5
    result_uuids = [r["uuid"] for r in result]
    assert uuids[5] in result_uuids
    assert uuids[0] not in result_uuids


def test_insert_sprig(db: duckdb.DuckDBPyConnection):
    """insert_sprig creates a sprig row with correct fields."""
    uuids = _seed_records(db, "sess-1", 5)
    sid = insert_sprig(db, "sess-1", "summary content", uuids)
    assert sid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [sid]).fetchone()
    assert row is not None
    assert row[1] == "sess-1"  # session_id
    assert row[2] == "sprig"  # kind


def test_insert_bindle(db: duckdb.DuckDBPyConnection):
    """insert_bindle creates a bindle row referencing parent sprigs."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:])
    bid = insert_bindle(db, "sess-1", "condensed content", [s1, s2])
    assert bid.startswith("sum_")
    row = db.execute("SELECT * FROM summaries WHERE summary_id = ?", [bid]).fetchone()
    assert row is not None
    assert row[2] == "bindle"  # kind
    assert row[3] == 2  # condensation_order


def test_get_sprigs_for_session(db: duckdb.DuckDBPyConnection):
    """get_sprigs_for_session returns sprigs not yet condensed into bindles."""
    uuids = _seed_records(db, "sess-1", 15)
    s1 = insert_sprig(db, "sess-1", "summary 1", uuids[:5])
    s2 = insert_sprig(db, "sess-1", "summary 2", uuids[5:10])
    s3 = insert_sprig(db, "sess-1", "summary 3", uuids[10:])
    # Condense s1 and s2 into a bindle
    insert_bindle(db, "sess-1", "condensed", [s1, s2])
    # Only s3 should be uncondensed
    uncondensed = get_sprigs_for_session(db, "sess-1", uncondensed_only=True)
    assert len(uncondensed) == 1
    assert uncondensed[0]["summary_id"] == s3


def test_get_latest_summary_for_session(db: duckdb.DuckDBPyConnection):
    """Returns the highest-order summary (bindle > sprig) for context assembly."""
    uuids = _seed_records(db, "sess-1", 10)
    s1 = insert_sprig(db, "sess-1", "sprig 1", uuids[:5])
    s2 = insert_sprig(db, "sess-1", "sprig 2", uuids[5:])
    insert_bindle(db, "sess-1", "the bindle", [s1, s2])
    latest = get_latest_summary_for_session(db, "sess-1")
    assert latest is not None
    assert latest["kind"] == "bindle"
    assert latest["content"] == "the bindle"


def test_run_summarize_session_creates_sprigs(db: duckdb.DuckDBPyConnection):
    """run_summarize_session chunks messages into sprigs."""
    _seed_records(db, "sess-1", SPRIG_CHUNK_SIZE * 2 + 3)
    with patch("ingest_sessions.dag.summarize_messages", return_value="mock summary"):
        result = run_summarize_session(db, "sess-1")
    assert result["sprigs_created"] == 2  # 2 full chunks, remainder < chunk size
    sprigs = get_sprigs_for_session(db, "sess-1")
    assert len(sprigs) == 2


def test_run_summarize_session_creates_bindles(db: duckdb.DuckDBPyConnection):
    """run_summarize_session condenses sprigs into bindles when threshold met."""
    # Seed enough messages for BINDLE_THRESHOLD + 1 full chunks
    record_count = SPRIG_CHUNK_SIZE * (BINDLE_THRESHOLD + 1)
    _seed_records(db, "sess-1", record_count)

    call_count = {"summarize": 0, "condense": 0}

    def mock_summarize(records, previous_summary_context=None):
        call_count["summarize"] += 1
        return f"mock sprig summary {call_count['summarize']}"

    def mock_condense(summaries, previous_summary_context=None):
        call_count["condense"] += 1
        return f"mock bindle from {len(summaries)} sprigs"

    with patch("ingest_sessions.dag.summarize_messages", side_effect=mock_summarize):
        with patch("ingest_sessions.dag.condense_summaries", side_effect=mock_condense):
            result = run_summarize_session(db, "sess-1")

    assert result["sprigs_created"] == BINDLE_THRESHOLD + 1
    assert result["bindles_created"] == 1
    assert call_count["condense"] == 1

    # Verify bindle exists in DB
    row = db.execute(
        "SELECT kind, content FROM summaries WHERE kind = 'bindle'"
    ).fetchone()
    assert row is not None
    assert "mock bindle" in row[1]

    # Verify uncondensed count is 0 (all sprigs consumed)
    uncondensed = get_sprigs_for_session(db, "sess-1", uncondensed_only=True)
    assert len(uncondensed) == 0


def test_run_summarize_session_condenses_preexisting_sprigs(
    db: duckdb.DuckDBPyConnection,
):
    """run_summarize_session creates bindles from pre-existing sprigs."""
    # Pre-create sprigs (simulating prior runs)
    for i in range(BINDLE_THRESHOLD + 2):
        uuids = [f"rec-{i}-{j}" for j in range(3)]
        for uuid in uuids:
            raw = json.dumps({"message": {"role": "user", "content": f"Msg {uuid}"}})
            db.execute(
                "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
                [uuid, "sess-1", "user", f"2026-03-01T10:{i:02d}:00.000Z", None, raw],
            )
        insert_sprig(db, "sess-1", f"pre-existing sprig {i}", uuids)

    uncondensed_before = get_sprigs_for_session(db, "sess-1", uncondensed_only=True)
    assert len(uncondensed_before) >= BINDLE_THRESHOLD

    def mock_condense(summaries, previous_summary_context=None):
        return f"condensed {len(summaries)} sprigs"

    # No new messages, but should still condense existing sprigs
    with patch("ingest_sessions.dag.summarize_messages", return_value="unused"):
        with patch("ingest_sessions.dag.condense_summaries", side_effect=mock_condense):
            result = run_summarize_session(db, "sess-1")

    assert result["sprigs_created"] == 0  # no new messages
    assert result["bindles_created"] == 1


def test_assemble_context_includes_tail(db: duckdb.DuckDBPyConnection):
    """assemble_context_for_session includes unsummarized tail messages."""
    _seed_records(db, "sess-1", 5)
    ctx = assemble_context_for_session(db, "sess-1")
    assert ctx is not None
    assert "Unsummarized Tail" in ctx
    assert "Message 0 content here" in ctx


def test_assemble_context_empty_session(db: duckdb.DuckDBPyConnection):
    """assemble_context_for_session returns None for empty sessions."""
    ctx = assemble_context_for_session(db, "nonexistent")
    assert ctx is None


def test_sprig_captures_file_ids(db: duckdb.DuckDBPyConnection):
    """Sprig insertion extracts file_ids from content."""
    content = (
        "The user read a large file.\n"
        "[Large Content: file_abcdef0123456789] [Tokens: ~5000]\n"
        "Then they asked about another.\n"
        "[Large Content: file_1234567890abcdef] [Tokens: ~3000]"
    )
    summary_id = insert_sprig(db, "sess-001", content, ["msg-1", "msg-2"])
    row = db.execute(
        "SELECT file_ids FROM summaries WHERE summary_id = ?",
        [summary_id],
    ).fetchone()
    assert row is not None
    assert "file_abcdef0123456789" in row[0]
    assert "file_1234567890abcdef" in row[0]


def test_bindle_propagates_file_ids(db: duckdb.DuckDBPyConnection):
    """Bindle collects file_ids from parent sprigs."""
    content1 = "Summary with [Large Content: file_aaaa000000000000] [Tokens: ~1000]"
    content2 = "Summary with [Large Content: file_bbbb000000000000] [Tokens: ~2000]"
    insert_sprig(db, "sess-001", content1, ["msg-1"])
    insert_sprig(db, "sess-001", content2, ["msg-2"])

    sprigs = get_sprigs_for_session(db, "sess-001")
    parent_ids = [s["summary_id"] for s in sprigs]
    bindle_content = "Condensed summary mentioning both files."
    bindle_id = insert_bindle(db, "sess-001", bindle_content, parent_ids)

    row = db.execute(
        "SELECT file_ids FROM summaries WHERE summary_id = ?",
        [bindle_id],
    ).fetchone()
    assert row is not None
    assert "file_aaaa000000000000" in row[0]
    assert "file_bbbb000000000000" in row[0]
