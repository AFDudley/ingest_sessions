#!/usr/bin/env python3
"""Emitting probe for derived-is-5a7.1 acceptance test c4.

Builds a small omp session whose entries are written out of reply-chain
order, including a branch point and a compaction entry pointing back at
an earlier 'first kept' entry, and observes: the reconstructed order
follows the actual parent chain (not file order), history before the
compaction's kept point is dropped, and branch_summary/custom_message
entries appear as regular messages. Judges nothing -- prints one
stdout_json object.
"""

from __future__ import annotations

import json
from typing import Any

from ingest_sessions.omp import (
    parse_entry,
    parse_header,
    reconstruct_conversation_order,
)


def main() -> None:
    header: dict[str, Any] = {
        "type": "session",
        "version": 3,
        "id": "omp-dag-sess",
        "cwd": "/repo",
        "title": "dag order test",
    }

    root: dict[str, Any] = {
        "id": "e-root",
        "parentId": None,
        "type": "message",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "message": {"role": "user", "content": "root"},
    }
    kept: dict[str, Any] = {
        "id": "e-kept",
        "parentId": "e-root",
        "type": "message",
        "timestamp": "2026-01-01T00:01:00.000Z",
        "message": {"role": "assistant", "content": "kept start"},
    }
    compaction: dict[str, Any] = {
        "id": "e-compaction",
        "parentId": "e-kept",
        "type": "compaction",
        "timestamp": "2026-01-01T00:02:00.000Z",
        "firstKeptEntryId": "e-kept",
    }
    branch_a: dict[str, Any] = {
        "id": "e-branch-a",
        "parentId": "e-compaction",
        "type": "message",
        "timestamp": "2026-01-01T00:03:00.000Z",
        "message": {"role": "user", "content": "branch A -- off the chosen leaf"},
    }
    branch_b: dict[str, Any] = {
        "id": "e-branch-b",
        "parentId": "e-compaction",
        "type": "branch_summary",
        "timestamp": "2026-01-01T00:03:30.000Z",
        "summary": "summary of the branch taken",
    }
    leaf: dict[str, Any] = {
        "id": "e-leaf",
        "parentId": "e-branch-b",
        "type": "custom_message",
        "timestamp": "2026-01-01T00:04:00.000Z",
        "content": "the final custom message",
    }

    # File order deliberately does NOT match the reply-to chain.
    file_order_entries = [leaf, root, branch_a, compaction, branch_b, kept]
    file_order_ids = [e["id"] for e in file_order_entries]

    parsed: dict[str, dict[str, Any]] = {}
    for e in file_order_entries:
        normalized = parse_entry(e, header["version"])
        assert normalized is not None
        parsed[normalized["id"]] = normalized

    # header round-trips through parse_header too, exercising the real
    # header-consumption path this reconstruction is fed from.
    assert parse_header(json.dumps(header)) is not None

    order = reconstruct_conversation_order(list(parsed.values()), leaf_id="e-leaf")
    order_ids = [e["id"] for e in order]

    reconstructed_order_matches_parent_chain = order_ids == [
        "e-kept",
        "e-compaction",
        "e-branch-b",
        "e-leaf",
    ] and all(order[i]["parentId"] == order[i - 1]["id"] for i in range(1, len(order)))
    reconstructed_order_matches_file_order = order_ids == file_order_ids
    history_before_first_kept_entry_dropped = "e-root" not in order_ids
    branch_summary_and_custom_message_present_as_messages = all(
        isinstance(e.get("message"), dict) and e["message"].get("content")
        for e in order
        if e["type"] in ("branch_summary", "custom_message")
    ) and any(e["type"] in ("branch_summary", "custom_message") for e in order)

    print(
        json.dumps(
            {
                "reconstructed_order_matches_parent_chain": reconstructed_order_matches_parent_chain,
                "reconstructed_order_matches_file_order": reconstructed_order_matches_file_order,
                "history_before_first_kept_entry_dropped": history_before_first_kept_entry_dropped,
                "branch_summary_and_custom_message_present_as_messages": branch_summary_and_custom_message_present_as_messages,
            }
        )
    )


if __name__ == "__main__":
    main()
