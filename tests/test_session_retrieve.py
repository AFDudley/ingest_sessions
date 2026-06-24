"""Tests for the SessionStart relevance-injection hook (pebble is-565.7).

Covers the pure pieces — seed-query construction from a repo's git focus and
the injection formatting — plus fail-open behavior. The live retrieval round
trip is exercised by the server e2e tests; here we test the hook's own logic.
"""

import subprocess
from pathlib import Path

from ingest_sessions.hooks.session_retrieve import (
    build_query,
    format_injection,
)


def _git_repo(path: Path) -> None:
    """Initialize a git repo at *path* with one commit on a known branch."""
    run = lambda *a: subprocess.run(  # noqa: E731 - terse local helper
        ["git", "-C", str(path), *a], check=True, capture_output=True
    )
    run("init", "-q", "-b", "feat/retrieval")
    run("config", "user.email", "t@t")
    run("config", "user.name", "t")
    (path / "f.txt").write_text("x")
    run("add", "-A")
    run("commit", "-q", "-m", "add retrieval pipeline rerank")


def test_build_query_from_git_focus(tmp_path: Path) -> None:
    _git_repo(tmp_path)
    q = build_query(str(tmp_path))
    assert q is not None
    assert "feat/retrieval" in q
    assert "add retrieval pipeline rerank" in q


def test_build_query_none_when_not_a_repo(tmp_path: Path) -> None:
    assert build_query(str(tmp_path)) is None


def test_build_query_none_when_empty_cwd() -> None:
    assert build_query("") is None


def test_format_injection_records_and_summaries() -> None:
    results = [
        {
            "session_id": "11112222-aaaa",
            "final_score": 0.91,
            "superseded": False,
            "raw": {"message": {"content": "we chose the in-server backfill"}},
        },
        {
            "session_id": "33334444-bbbb",
            "summary_id": "sum-1",
            "final_score": 0.40,
            "superseded": True,
            "content": "early plan: exclusive-lock backfill script",
        },
    ]
    out = format_injection(results)
    assert out is not None
    assert "Relevant prior reasoning (ingest-sessions)" in out
    assert "get_session" in out
    assert "in-server backfill" in out
    assert "session 11112222" in out
    assert "summary 33334444" in out
    assert "[superseded]" in out  # the overturned plan is flagged


def test_format_injection_none_when_no_snippets() -> None:
    # Hits with no usable text yield no injection (fail-open to nothing).
    assert format_injection([{"session_id": "x", "raw": {}}]) is None
    assert format_injection([]) is None


def test_format_injection_handles_content_blocks() -> None:
    results = [
        {
            "session_id": "5555",
            "raw": {
                "message": {
                    "content": [
                        {"type": "text", "text": "blockwise reasoning here"},
                    ]
                }
            },
        }
    ]
    out = format_injection(results)
    assert out is not None
    assert "blockwise reasoning here" in out
