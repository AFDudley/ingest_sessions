"""Tests for the shared retrieval-injection core (is-408).

Covers the per-session injection marker and the de-dup contract
(``injection_is_known_good``) that lets the SessionStart hook and the
UserPromptSubmit fallback inject exactly once per session — and always at
least once even when SessionStart-startup is silently dropped.
"""

from pathlib import Path

import pytest

from ingest_sessions.hooks import _retrieve_common as rc


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the marker cache at a temp dir so tests never touch ~/.cache."""
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def test_marker_round_trip() -> None:
    assert rc.read_marker("sess-1") is None
    rc.write_marker("sess-1", event="UserPromptSubmit")
    marker = rc.read_marker("sess-1")
    assert marker == {"event": "UserPromptSubmit"}


def test_marker_records_source_for_session_start() -> None:
    rc.write_marker("sess-2", event="SessionStart", source="resume")
    assert rc.read_marker("sess-2") == {"event": "SessionStart", "source": "resume"}


def test_marker_path_honors_xdg(tmp_path: Path) -> None:
    path = rc.marker_path("abc")
    assert path == tmp_path / "cache" / "ingest-sessions" / "injected" / "abc"


def test_write_marker_empty_session_is_noop() -> None:
    rc.write_marker("", event="UserPromptSubmit")
    assert rc.read_marker("") is None


def test_read_marker_corrupt_file_returns_none(tmp_path: Path) -> None:
    path = rc.marker_path("bad")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json")
    assert rc.read_marker("bad") is None


# ---- de-dup contract: injection_is_known_good ----


def test_user_prompt_injection_is_known_good() -> None:
    assert rc.injection_is_known_good({"event": "UserPromptSubmit"}) is True


def test_session_start_trusted_sources_are_known_good() -> None:
    for src in ("resume", "clear", "compact"):
        assert (
            rc.injection_is_known_good({"event": "SessionStart", "source": src}) is True
        )


def test_session_start_startup_is_not_known_good() -> None:
    # The is-408 core: a startup marker does NOT suppress the fallback.
    assert (
        rc.injection_is_known_good({"event": "SessionStart", "source": "startup"})
        is False
    )


def test_session_start_missing_source_is_not_known_good() -> None:
    assert rc.injection_is_known_good({"event": "SessionStart"}) is False
