#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c5.

Feeds the adapter one omp entry whose message content is a list of
separate content blocks (not a plain string), and observes: the
resulting record's session_id/uuid/parent map from the header id/entry
id/entry parentId, and the extracted message text matches the text
pulled out of the block list. Judges nothing -- prints one stdout_json
object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables
from ingest_sessions.omp import extract_message_text, ingest_omp_jsonl


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        header = {
            "type": "session",
            "version": 3,
            "id": "omp-fieldmap-sess",
            "cwd": "/repo",
            "title": "field mapping test",
        }
        root = {
            "id": "omp-fieldmap-root",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {"role": "user", "content": "root prompt"},
        }
        blocks = [
            {"type": "text", "text": "first block "},
            {"type": "text", "text": "second block"},
        ]
        entry = {
            "id": "omp-fieldmap-e1",
            "parentId": "omp-fieldmap-root",
            "type": "message",
            "timestamp": "2026-01-01T00:01:00.000Z",
            "message": {"role": "assistant", "content": blocks},
        }
        omp_path = tmp_path / "omp-fieldmap-sess.jsonl"
        omp_path.write_text(
            "\n".join(json.dumps(x) for x in [header, root, entry]) + "\n"
        )

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)
        ingest_omp_jsonl(db, omp_path)

        row = db.execute(
            "SELECT session_id, uuid, parent_uuid, raw FROM records WHERE uuid = ?",
            [entry["id"]],
        ).fetchone()
        assert row is not None
        session_id, uuid, parent_uuid, raw = row
        stored = json.loads(raw)

        # Mirrors retrieval._content_to_text's block-join behavior (the
        # flattener extract_message_text reuses under the hood).
        expected_text = "\n".join(b["text"] for b in blocks)
        extracted_text = extract_message_text(stored.get("message"))

        db.close()

    print(
        json.dumps(
            {
                "session_id_from_header_id": session_id == header["id"],
                "uuid_from_entry_id": uuid == entry["id"],
                "parent_from_parent_id": parent_uuid == entry["parentId"],
                "list_content_text_extracted": extracted_text == expected_text
                and bool(extracted_text),
            }
        )
    )


if __name__ == "__main__":
    main()
