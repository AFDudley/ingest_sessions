#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.4 acceptance tests c4, c5, c9, c10, c11, c12.

Cold-scans the real omp corpus (<real-home>/.omp/agent/sessions, resolved
from the invoking user's passwd entry -- never $HOME, never the live
database) and rebuilds it into a disposable throwaway DuckDB via the SAME
shared router (`core.ingest_routed_file`) production ingestion uses.

Independently of that rebuild, this probe first re-scans every file's own
leading lines to know, ahead of time, which lines are `title` preamble and
what title each header itself carries -- so the numbers it reports about
the rebuilt database (how many title lines became records, how many were
counted malformed, whose stored title didn't come from the header) are
checked against ground truth read straight off disk, not against the
adapter's own bookkeeping. Judges nothing -- prints one stdout_json
object.
"""

from __future__ import annotations

import json
import os
import pwd
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.core import create_tables, ingest_routed_file
from ingest_sessions.omp import HEADER_SCAN_BOUND, find_header


def _real_omp_root() -> Path:
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    return real_home / ".omp" / "agent" / "sessions"


def _scan_ground_truth(paths: list[Path]) -> tuple[set[str], dict[str, Any], int]:
    """Independently read each file's own leading lines off disk.

    Returns (omp session ids, {session id: header title}, title preamble
    line count) -- computed WITHOUT relying on what the rebuild below
    stores, so the rebuild's numbers can be checked against it.
    """
    omp_session_ids: set[str] = set()
    header_title_by_session_id: dict[str, Any] = {}
    preamble_lines_total = 0

    for path in paths:
        raw_lines = path.read_bytes().split(b"\n")
        leading = [
            raw_lines[i].decode("utf-8", errors="replace")
            for i in range(min(HEADER_SCAN_BOUND, len(raw_lines)))
        ]
        header, header_index = find_header(leading)
        if header is None or header_index is None:
            continue
        omp_session_ids.add(header["id"])
        header_title_by_session_id[header["id"]] = header.get("title")
        for i in range(header_index):
            try:
                obj = json.loads(leading[i])
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict) and obj.get("type") == "title":
                preamble_lines_total += 1

    return omp_session_ids, header_title_by_session_id, preamble_lines_total


def main() -> None:
    root = _real_omp_root()
    paths = sorted(root.rglob("*.jsonl")) if root.is_dir() else []

    omp_session_ids, header_title_by_session_id, preamble_lines_total = (
        _scan_ground_truth(paths)
    )
    ids_list = sorted(omp_session_ids)

    # In-memory throwaway db: this rebuild walks the FULL real corpus through
    # the real router, so it stays off disk entirely -- never the live
    # database, and no tempfile I/O to slow the full-corpus pass down.
    db = duckdb.connect(":memory:")
    create_tables(db)
    for path in paths:
        ingest_routed_file(db, path)

    omp_records = 0
    records_with_surrogate_uuid = 0
    parent_uuid_non_null = 0
    preamble_lines_inserted_as_records = 0
    omp_session_rows = 0
    sessions_with_title_not_from_header = 0

    if ids_list:
        row = db.execute(
            "SELECT count(*), "
            "sum(CASE WHEN uuid LIKE 'nouuid_%' THEN 1 ELSE 0 END), "
            "sum(CASE WHEN parent_uuid IS NOT NULL THEN 1 ELSE 0 END), "
            "sum(CASE WHEN json_extract_string(raw, '$.type') = 'title' "
            "THEN 1 ELSE 0 END) "
            "FROM records WHERE session_id = ANY(?)",
            [ids_list],
        ).fetchone()
        assert row is not None
        omp_records = row[0] or 0
        records_with_surrogate_uuid = row[1] or 0
        parent_uuid_non_null = row[2] or 0
        preamble_lines_inserted_as_records = row[3] or 0

        session_rows = db.execute(
            "SELECT session_id, summary FROM sessions WHERE session_id = ANY(?)",
            [ids_list],
        ).fetchall()
        omp_session_rows = len(session_rows)
        sessions_with_title_not_from_header = sum(
            1
            for sid, summary in session_rows
            if summary != header_title_by_session_id.get(sid)
        )

    malformed_rows = db.execute("SELECT line_text FROM malformed_lines").fetchall()
    preamble_lines_counted_malformed = 0
    for (line_text,) in malformed_rows:
        try:
            obj = json.loads(line_text)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("type") == "title":
            preamble_lines_counted_malformed += 1

    db.close()

    parent_uuid_non_null_pct = (
        100.0 * parent_uuid_non_null / omp_records if omp_records else 0.0
    )

    print(
        json.dumps(
            {
                "files_total": len(paths),
                "omp_session_ids_found": len(omp_session_ids),
                "preamble_lines_total": preamble_lines_total,
                "preamble_lines_inserted_as_records": preamble_lines_inserted_as_records,
                "preamble_lines_counted_malformed": preamble_lines_counted_malformed,
                "omp_records": omp_records,
                "omp_session_rows": omp_session_rows,
                "sessions_with_title_not_from_header": sessions_with_title_not_from_header,
                "records_with_surrogate_uuid": records_with_surrogate_uuid,
                "parent_uuid_non_null_pct": parent_uuid_non_null_pct,
            }
        )
    )


if __name__ == "__main__":
    main()
