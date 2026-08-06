"""Unit tests for ingest_sessions.omp (pebble is-5a7.1).

Covers the pure parsing/reconstruction/field-mapping helpers directly;
the acceptance-level end-to-end behavior is covered by the
scripts/oracle_omp_*.py probes the derived spec gates on.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingest_sessions.core import create_tables
from ingest_sessions.omp import (
    OMP_ENTRY_KINDS,
    discover_subagent_files,
    extract_message_text,
    ingest_omp_jsonl,
    ingest_omp_session,
    ingest_omp_session_metadata,
    omp_entry_to_record_row,
    parse_entry,
    parse_header,
    record_type_for_entry,
    reconstruct_conversation_order,
)


@pytest.fixture
def db() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


HEADER = {
    "type": "session",
    "version": 3,
    "id": "sess-omp-1",
    "cwd": "/repo",
    "title": "a session",
}


class TestParseHeader:
    def test_valid_header(self) -> None:
        header = parse_header(json.dumps(HEADER))
        assert header is not None
        assert header["id"] == "sess-omp-1"

    def test_not_a_session_type_rejected(self) -> None:
        assert parse_header(json.dumps({"type": "message", "id": "x"})) is None

    def test_missing_id_rejected(self) -> None:
        assert parse_header(json.dumps({"type": "session"})) is None

    def test_malformed_json_rejected(self) -> None:
        assert parse_header("{not json") is None


class TestParseEntry:
    def test_recognizes_all_13_kinds(self) -> None:
        for kind in OMP_ENTRY_KINDS:
            obj = {"id": f"e-{kind}", "type": kind}
            assert parse_entry(obj, 3) is not None

    def test_unknown_kind_rejected(self) -> None:
        assert parse_entry({"id": "e1", "type": "not_a_real_kind"}, 3) is None

    def test_missing_id_rejected(self) -> None:
        assert parse_entry({"type": "message"}, 3) is None

    def test_not_a_dict_rejected(self) -> None:
        assert parse_entry("not a dict", 3) is None  # type: ignore[arg-type]

    def test_v1_reads_legacy_parent_field(self) -> None:
        entry = parse_entry({"id": "e1", "type": "message", "parent": "root"}, 1)
        assert entry is not None
        assert entry["parentId"] == "root"

    def test_v3_reads_parent_id_field(self) -> None:
        entry = parse_entry({"id": "e1", "type": "message", "parentId": "root"}, 3)
        assert entry is not None
        assert entry["parentId"] == "root"

    def test_v1_ignores_new_style_field(self) -> None:
        # v1 wrote `parent`, not `parentId` -- a v1 entry carrying only
        # `parentId` has no parent link under v1's own rule.
        entry = parse_entry({"id": "e1", "type": "message", "parentId": "root"}, 1)
        assert entry is not None
        assert entry["parentId"] is None


class TestExtractMessageText:
    def test_string_content(self) -> None:
        assert extract_message_text({"role": "user", "content": "hi"}) == "hi"

    def test_list_content(self) -> None:
        message = {
            "role": "assistant",
            "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}],
        }
        assert extract_message_text(message) == "a\nb"

    def test_none_message(self) -> None:
        assert extract_message_text(None) is None

    def test_empty_content(self) -> None:
        assert extract_message_text({"role": "user", "content": ""}) is None


class TestRecordTypeForEntry:
    def test_message_uses_role(self) -> None:
        entry = {"type": "message", "message": {"role": "user"}}
        assert record_type_for_entry(entry) == "user"

    def test_branch_summary_is_synthetic_assistant_by_default(self) -> None:
        assert record_type_for_entry({"type": "branch_summary"}) == "assistant"

    def test_other_kind_uses_kind_name(self) -> None:
        assert record_type_for_entry({"type": "model_change"}) == "model_change"


class TestOmpEntryToRecordRow:
    def test_maps_six_fields(self) -> None:
        entry = {
            "id": "e1",
            "parentId": "root",
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hi"},
        }
        row = omp_entry_to_record_row(entry, "sess-1")
        uuid, session_id, rtype, timestamp, parent_uuid, raw = row
        assert uuid == "e1"
        assert session_id == "sess-1"
        assert rtype == "user"
        assert timestamp == "2026-01-01T00:00:00.000Z"
        assert parent_uuid == "root"
        assert json.loads(raw)["id"] == "e1"

    def test_synthesizes_message_for_custom_message(self) -> None:
        entry = {
            "id": "e1",
            "parentId": "root",
            "type": "custom_message",
            "content": "synthetic text",
        }
        row = omp_entry_to_record_row(entry, "sess-1")
        raw = json.loads(row[5])
        assert raw["message"]["content"] == "synthetic text"
        assert row[2] == "assistant"


class TestReconstructConversationOrder:
    def _entries(self) -> list[dict[str, Any]]:
        return [
            {
                "id": "root",
                "parentId": None,
                "type": "message",
                "timestamp": "t0",
                "message": {"role": "user", "content": "root"},
            },
            {
                "id": "mid",
                "parentId": "root",
                "type": "message",
                "timestamp": "t1",
                "message": {"role": "assistant", "content": "mid"},
            },
            {
                "id": "leaf",
                "parentId": "mid",
                "type": "message",
                "timestamp": "t2",
                "message": {"role": "user", "content": "leaf"},
            },
        ]

    def test_walks_parent_chain_not_file_order(self) -> None:
        entries = list(reversed(self._entries()))  # scrambled file order
        order = reconstruct_conversation_order(entries, leaf_id="leaf")
        assert [e["id"] for e in order] == ["root", "mid", "leaf"]

    def test_defaults_to_latest_leaf(self) -> None:
        entries = self._entries()
        order = reconstruct_conversation_order(entries)
        assert order[-1]["id"] == "leaf"

    def test_off_branch_sibling_excluded(self) -> None:
        entries = self._entries()
        entries.append(
            {
                "id": "other-branch",
                "parentId": "mid",
                "type": "message",
                "timestamp": "t1.5",
                "message": {"role": "user", "content": "off branch"},
            }
        )
        order = reconstruct_conversation_order(entries, leaf_id="leaf")
        assert "other-branch" not in [e["id"] for e in order]

    def test_v3_compaction_drops_history_before_first_kept(self) -> None:
        entries: list[dict[str, Any]] = [
            {
                "id": "root",
                "parentId": None,
                "type": "message",
                "timestamp": "t0",
                "message": {"role": "user", "content": "root"},
                "omp_version": 3,
            },
            {
                "id": "kept",
                "parentId": "root",
                "type": "message",
                "timestamp": "t1",
                "message": {"role": "assistant", "content": "kept"},
                "omp_version": 3,
            },
            {
                "id": "compaction",
                "parentId": "kept",
                "type": "compaction",
                "timestamp": "t2",
                "firstKeptEntryId": "kept",
                "omp_version": 3,
            },
            {
                "id": "leaf",
                "parentId": "compaction",
                "type": "message",
                "timestamp": "t3",
                "message": {"role": "user", "content": "leaf"},
                "omp_version": 3,
            },
        ]
        order = reconstruct_conversation_order(entries, leaf_id="leaf")
        ids = [e["id"] for e in order]
        assert "root" not in ids
        assert ids == ["kept", "compaction", "leaf"]

    def test_v2_compaction_does_not_truncate(self) -> None:
        entries: list[dict[str, Any]] = [
            {
                "id": "root",
                "parentId": None,
                "type": "message",
                "timestamp": "t0",
                "message": {"role": "user", "content": "root"},
                "omp_version": 2,
            },
            {
                "id": "compaction",
                "parentId": "root",
                "type": "compaction",
                "timestamp": "t1",
                "firstKeptEntryId": "root",
                "omp_version": 2,
            },
            {
                "id": "leaf",
                "parentId": "compaction",
                "type": "message",
                "timestamp": "t2",
                "message": {"role": "user", "content": "leaf"},
                "omp_version": 2,
            },
        ]
        order = reconstruct_conversation_order(entries, leaf_id="leaf")
        assert "root" in [e["id"] for e in order]

    def test_branch_summary_present_with_synthetic_message(self) -> None:
        entries: list[dict[str, Any]] = [
            {
                "id": "root",
                "parentId": None,
                "type": "message",
                "timestamp": "t0",
                "message": {"role": "user", "content": "root"},
            },
            {
                "id": "summary",
                "parentId": "root",
                "type": "branch_summary",
                "timestamp": "t1",
                "summary": "a summary",
            },
        ]
        order = reconstruct_conversation_order(entries, leaf_id="summary")
        summary_entry = order[-1]
        assert summary_entry["type"] == "branch_summary"
        assert summary_entry["message"]["content"] == "a summary"


class TestIngestOmpJsonl:
    def test_header_never_becomes_a_record(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        entry = {
            "id": "e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hi"},
        }
        jsonl = tmp_path / "sess.jsonl"
        jsonl.write_text("\n".join(json.dumps(x) for x in [HEADER, entry]) + "\n")

        count, malformed, header = ingest_omp_jsonl(db, jsonl)
        assert count == 1
        assert malformed == 0
        assert header is not None
        rows = db.execute("SELECT uuid FROM records").fetchall()
        assert rows == [("e1",)]

    def test_skips_malformed_lines(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        entry = {
            "id": "e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hi"},
        }
        jsonl = tmp_path / "sess.jsonl"
        jsonl.write_text(
            "\n".join([json.dumps(HEADER), json.dumps(entry), "not valid json"]) + "\n"
        )
        count, malformed, _header = ingest_omp_jsonl(db, jsonl)
        assert count == 1
        assert malformed == 1

    def test_not_an_omp_file_returns_none_header(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        jsonl = tmp_path / "sess.jsonl"
        jsonl.write_text('{"type": "message", "id": "e1"}\n')
        count, malformed, header = ingest_omp_jsonl(db, jsonl)
        assert (count, malformed, header) == (0, 0, None)

    def test_deduplicates_on_uuid(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        entry = {
            "id": "e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "hi"},
        }
        jsonl = tmp_path / "sess.jsonl"
        jsonl.write_text("\n".join(json.dumps(x) for x in [HEADER, entry]) + "\n")
        ingest_omp_jsonl(db, jsonl)
        ingest_omp_jsonl(db, jsonl)
        row = db.execute("SELECT count(*) FROM records").fetchone()
        assert row == (1,)


class TestIngestOmpSessionMetadata:
    def test_maps_header_fields(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        entry = {
            "id": "e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "my prompt"},
        }
        jsonl = tmp_path / "sess.jsonl"
        jsonl.write_text("\n".join(json.dumps(x) for x in [HEADER, entry]) + "\n")
        _count, _malformed, header = ingest_omp_jsonl(db, jsonl)
        assert header is not None
        ingest_omp_session_metadata(db, header)

        row = db.execute(
            "SELECT session_id, summary, project_path, first_prompt, message_count "
            "FROM sessions WHERE session_id = ?",
            [HEADER["id"]],
        ).fetchone()
        assert row == (HEADER["id"], HEADER["title"], HEADER["cwd"], "my prompt", 1)


class TestDiscoverSubagentFiles:
    def test_no_sibling_dir_returns_empty(self, tmp_path: Path) -> None:
        session_path = tmp_path / "sess.jsonl"
        session_path.write_text("{}\n")
        assert discover_subagent_files(session_path) == []

    def test_finds_recursively(self, tmp_path: Path) -> None:
        session_path = tmp_path / "sess.jsonl"
        session_path.write_text("{}\n")
        sub_dir = tmp_path / "sess"
        sub_dir.mkdir()
        (sub_dir / "agent-1.jsonl").write_text("{}\n")
        nested_dir = sub_dir / "agent-1"
        nested_dir.mkdir()
        (nested_dir / "agent-1a.jsonl").write_text("{}\n")

        found = discover_subagent_files(session_path)
        assert len(found) == 2
        assert {p.name for p in found} == {"agent-1.jsonl", "agent-1a.jsonl"}


class TestIngestOmpSession:
    def test_ingests_parent_and_subagents(
        self, db: duckdb.DuckDBPyConnection, tmp_path: Path
    ) -> None:
        parent_entry = {
            "id": "p1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "delegate"},
        }
        parent_path = tmp_path / "sess.jsonl"
        parent_path.write_text(
            "\n".join(json.dumps(x) for x in [HEADER, parent_entry]) + "\n"
        )

        sub_dir = tmp_path / "sess"
        sub_dir.mkdir()
        sub_header = {**HEADER, "id": "agent-1", "parentSession": HEADER["id"]}
        sub_entry = {
            "id": "s1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:30.000Z",
            "message": {"role": "assistant", "content": "working"},
        }
        (sub_dir / "agent-1.jsonl").write_text(
            "\n".join(json.dumps(x) for x in [sub_header, sub_entry]) + "\n"
        )

        result = ingest_omp_session(db, parent_path)
        assert result == {
            "records": 2,
            "malformed": 0,
            "sessions": 2,
            "subagent_files": 1,
        }
        session_ids = {
            row[0]
            for row in db.execute("SELECT DISTINCT session_id FROM records").fetchall()
        }
        assert session_ids == {HEADER["id"], "agent-1"}
