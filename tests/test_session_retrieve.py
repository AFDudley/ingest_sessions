"""Tests for the SessionStart relevance-injection hook (pebble is-565.7).

Covers the pure pieces — seed-query construction from a repo's git focus and
the injection formatting — plus fail-open behavior. The live retrieval round
trip is exercised by the server e2e tests; here we test the hook's own logic.
"""

import io
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from ingest_sessions.hooks import _retrieve_common as rc
from ingest_sessions.hooks import session_retrieve as sr
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


# ---------------------------------------------------------------------------
# main(): invocation logging + shared de-dup marker (is-408)
# ---------------------------------------------------------------------------

_HIT = {
    "session_id": "11112222-aaaa",
    "final_score": 0.91,
    "raw": {"message": {"content": "we chose the in-server backfill"}},
}


def _run_main(
    monkeypatch: pytest.MonkeyPatch,
    hook_input: dict[str, Any],
    retrieve_result: list[dict[str, Any]],
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(hook_input)))
    monkeypatch.setattr(sr, "build_query", lambda cwd: "seed" if cwd else None)
    monkeypatch.setattr(sr, "_retrieve", lambda q: retrieve_result)


def test_main_injects_and_writes_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    _run_main(
        monkeypatch, {"session_id": "s1", "cwd": "/repo", "source": "resume"}, [_HIT]
    )
    sr.main()
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["hookSpecificOutput"]["hookEventName"] == "SessionStart"
    # Marker records the source so the fallback can judge known-good-ness.
    assert rc.read_marker("s1") == {"event": "SessionStart", "source": "resume"}
    # Invocation log went to stderr, not stdout.
    assert "[ingest-sessions] session_retrieve: source=resume" in captured.err
    assert "injected=" in captured.err


def test_main_logs_and_no_marker_when_nothing_to_inject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    _run_main(
        monkeypatch, {"session_id": "s2", "cwd": "/repo", "source": "startup"}, []
    )
    sr.main()
    captured = capsys.readouterr()
    assert captured.out.strip() == ""
    assert rc.read_marker("s2") is None
    assert "source=startup" in captured.err
    assert "injected=0chars" in captured.err
