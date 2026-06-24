"""Tests for the UserPromptSubmit fallback hook (is-408).

The gate logic — known-good marker → no output; absent/untrusted marker →
inject + write a UserPromptSubmit marker — is the load-bearing behavior. The
live retrieval round trip is patched out; the pipeline atoms are covered in
test_session_retrieve / test_retrieve_common.
"""

import io
import json
from pathlib import Path
from typing import Any

import pytest

from ingest_sessions.hooks import _retrieve_common as rc
from ingest_sessions.hooks import first_prompt_retrieve as fpr

HIT = {
    "session_id": "11112222-aaaa",
    "final_score": 0.91,
    "raw": {"message": {"content": "we chose the in-server backfill"}},
}


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def _run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    hook_input: dict[str, Any],
    retrieve_result: list[dict[str, Any]] | None = None,
) -> str:
    """Run the hook with stdin=hook_input and a patched retrieval; return stdout."""
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(hook_input)))
    monkeypatch.setattr(fpr, "build_query", lambda cwd: "seed query" if cwd else None)
    monkeypatch.setattr(
        fpr,
        "_retrieve",
        lambda q: retrieve_result if retrieve_result is not None else [HIT],
    )
    fpr.main()
    return capsys.readouterr().out


def test_injects_and_writes_marker_when_no_marker(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(monkeypatch, capsys, {"session_id": "s1", "cwd": "/repo"})
    payload = json.loads(out)
    assert payload["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    assert "in-server backfill" in payload["hookSpecificOutput"]["additionalContext"]
    # Marker now records a known-good UserPromptSubmit injection.
    marker = rc.read_marker("s1")
    assert marker == {"event": "UserPromptSubmit"}
    assert rc.injection_is_known_good(marker) is True


def test_skips_when_known_good_marker_present(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    rc.write_marker("s2", event="SessionStart", source="resume")
    out = _run(monkeypatch, capsys, {"session_id": "s2", "cwd": "/repo"})
    assert out.strip() == ""  # no injection


def test_skips_when_prior_user_prompt_injection(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    rc.write_marker("s3", event="UserPromptSubmit")
    out = _run(monkeypatch, capsys, {"session_id": "s3", "cwd": "/repo"})
    assert out.strip() == ""


def test_injects_when_startup_marker_present(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # is-408: a startup SessionStart marker is NOT known-good (may be dropped),
    # so the fallback STILL injects.
    rc.write_marker("s4", event="SessionStart", source="startup")
    out = _run(monkeypatch, capsys, {"session_id": "s4", "cwd": "/repo"})
    payload = json.loads(out)
    assert payload["hookSpecificOutput"]["hookEventName"] == "UserPromptSubmit"
    # Marker upgraded to the known-good UserPromptSubmit injection.
    assert rc.read_marker("s4") == {"event": "UserPromptSubmit"}


def test_no_output_when_no_repo_focus(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(monkeypatch, capsys, {"session_id": "s5", "cwd": ""})
    assert out.strip() == ""
    assert rc.read_marker("s5") is None  # nothing injected → no marker


def test_no_output_when_retrieval_empty(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(
        monkeypatch, capsys, {"session_id": "s6", "cwd": "/repo"}, retrieve_result=[]
    )
    assert out.strip() == ""
    assert rc.read_marker("s6") is None


def test_empty_stdin_is_noop(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    fpr.main()
    assert capsys.readouterr().out.strip() == ""
