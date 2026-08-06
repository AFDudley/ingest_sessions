#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c9.

Ingests one omp session and one Claude-Code-style session, backfills the
same vector/lexical indexes both arms share, then asks the shared
`retrieve_relevant` tool a question the omp session's content answers.
Observes: the omp record comes back in the results, and its result dict
has the same shape as a Claude Code hit's -- proving no separate
query-path change was needed. Judges nothing -- prints one stdout_json
object.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb

from ingest_sessions.core import create_tables, ingest_jsonl, rebuild_fts_index
from ingest_sessions.embeddings import backfill_embeddings
from ingest_sessions.omp import ingest_omp_session
from ingest_sessions.retrieval import retrieve_relevant

# The shape retrieve_relevant guarantees on every hit regardless of the
# candidate's tier or which arm(s) surfaced it (see retrieval.py's
# retrieve_relevant docstring).
_CORE_HIT_KEYS = {
    "uuid",
    "session_id",
    "raw",
    "fused_score",
    "rerank_score",
    "final_score",
    "superseded",
    "recency",
    "tier",
}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        omp_header = {
            "type": "session",
            "version": 3,
            "id": "omp-parity-sess",
            "cwd": "/repo",
            "title": "parity test",
        }
        omp_entry = {
            "id": "omp-parity-e1",
            "parentId": None,
            "type": "message",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "message": {
                "role": "assistant",
                "content": "we chose the quokka migration protocol for cross-shard transfers",
            },
        }
        omp_path = tmp_path / "omp-parity-sess.jsonl"
        omp_path.write_text(
            "\n".join(json.dumps(x) for x in [omp_header, omp_entry]) + "\n"
        )

        claude_record = {
            "uuid": "claude-parity-r1",
            "sessionId": "claude-parity-sess",
            "type": "assistant",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "parentUuid": None,
            "message": {
                "role": "assistant",
                "content": "the deployment runs on a kubernetes cluster in us-east",
            },
        }
        claude_path = tmp_path / "claude-parity-sess.jsonl"
        claude_path.write_text(json.dumps(claude_record) + "\n")

        db = duckdb.connect(str(tmp_path / "db.duckdb"))
        create_tables(db)

        ingest_omp_session(db, omp_path)
        ingest_jsonl(db, claude_path)

        backfill_embeddings(db)
        rebuild_fts_index(db)

        omp_hits = retrieve_relevant(
            db, "what protocol did we choose for cross-shard transfers?", k=5
        )
        claude_hits = retrieve_relevant(
            db, "what cluster does the deployment run on?", k=5
        )

        omp_record_returned_by_retrieve_relevant = any(
            hit.get("uuid") == "omp-parity-e1" for hit in omp_hits
        )

        omp_hit = next((h for h in omp_hits if h.get("uuid") == "omp-parity-e1"), None)
        claude_hit = next(
            (h for h in claude_hits if h.get("uuid") == "claude-parity-r1"), None
        )
        result_shape_matches_claude_session_result = (
            omp_hit is not None
            and claude_hit is not None
            and _CORE_HIT_KEYS.issubset(omp_hit.keys())
            and _CORE_HIT_KEYS.issubset(claude_hit.keys())
        )

        db.close()

    print(
        json.dumps(
            {
                "omp_record_returned_by_retrieve_relevant": omp_record_returned_by_retrieve_relevant,
                "result_shape_matches_claude_session_result": result_shape_matches_claude_session_result,
            }
        )
    )


if __name__ == "__main__":
    main()
