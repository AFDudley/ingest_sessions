"""Tests for the hook installer."""

import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

from ingest_sessions.install import (
    HOOK_DEFS,
    MCP_SERVER_NAME,
    MCP_URL,
    _hook_entry,
    _is_our_hook,
    check_hooks,
    check_mcp_server,
    install_hooks,
    install_mcp_server,
    uninstall_hooks,
    uninstall_mcp_server,
)


def _fake_hooks_dir(tmp_path: Path) -> Path:
    """Create a fake hooks dir with all expected script files."""
    hooks_dir = tmp_path / "hooks"
    hooks_dir.mkdir()
    for _event, filename, _timeout in HOOK_DEFS:
        (hooks_dir / filename).write_text("#!/usr/bin/env python3\n")
    return hooks_dir


def test_is_our_hook_matches(tmp_path: Path):
    """Recognizes our hook entry by exact command match."""
    hooks_dir = _fake_hooks_dir(tmp_path)
    script = hooks_dir / "session_start.py"
    entry = _hook_entry(script, 15)
    assert _is_our_hook(entry, script) is True


def test_is_our_hook_rejects_other(tmp_path: Path):
    """Does not match unrelated hook entries."""
    hooks_dir = _fake_hooks_dir(tmp_path)
    script = hooks_dir / "session_start.py"
    entry = {
        "hooks": [
            {"type": "command", "command": "echo hello"},
        ]
    }
    assert _is_our_hook(entry, script) is False


def test_install_hooks_creates_entries(tmp_path: Path):
    """install_hooks adds both SessionStart and PreCompact entries."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        added = install_hooks()

    assert "SessionStart" in added
    assert "PreCompact" in added
    settings = json.loads(settings_path.read_text())
    assert "hooks" in settings
    assert len(settings["hooks"]["SessionStart"]) == 1
    assert len(settings["hooks"]["PreCompact"]) == 1


def test_install_hooks_idempotent(tmp_path: Path):
    """install_hooks does not duplicate if already installed."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        install_hooks()
        added = install_hooks()

    assert added == []
    settings = json.loads(settings_path.read_text())
    assert len(settings["hooks"]["SessionStart"]) == 1
    assert len(settings["hooks"]["PreCompact"]) == 1


def test_install_hooks_preserves_existing(tmp_path: Path):
    """install_hooks preserves existing settings and other hooks."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "env": {"FOO": "bar"},
                "hooks": {
                    "PreToolUse": [
                        {"hooks": [{"type": "command", "command": "echo pre"}]}
                    ]
                },
            }
        )
    )
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        install_hooks()

    settings = json.loads(settings_path.read_text())
    assert settings["env"]["FOO"] == "bar"
    assert len(settings["hooks"]["PreToolUse"]) == 1
    assert len(settings["hooks"]["SessionStart"]) == 1
    assert len(settings["hooks"]["PreCompact"]) == 1


def test_uninstall_hooks_removes_entries(tmp_path: Path):
    """uninstall_hooks removes all our hooks and cleans up empty containers."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        install_hooks()
        removed = uninstall_hooks()

    assert "SessionStart" in removed
    assert "PreCompact" in removed
    settings = json.loads(settings_path.read_text())
    assert "hooks" not in settings


def test_uninstall_hooks_noop_if_missing(tmp_path: Path):
    """uninstall_hooks returns empty list if no hooks found."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        removed = uninstall_hooks()

    assert removed == []


def test_check_hooks_reports_status(tmp_path: Path):
    """check_hooks returns correct nested status dict."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        status = check_hooks()

    assert status["all_installed"] is False
    assert status["all_scripts_exist"] is True
    assert status["python3_available"] is True
    assert "SessionStart" in status["hooks"]
    assert "PreCompact" in status["hooks"]
    assert status["hooks"]["SessionStart"]["installed"] is False
    assert status["hooks"]["PreCompact"]["installed"] is False


def test_check_hooks_after_install(tmp_path: Path):
    """check_hooks reports all_installed after install."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text("{}")
    hooks_dir = _fake_hooks_dir(tmp_path)

    with (
        patch("ingest_sessions.install.SETTINGS_PATH", settings_path),
        patch("ingest_sessions.install.HOOKS_DIR", hooks_dir),
    ):
        install_hooks()
        status = check_hooks()

    assert status["all_installed"] is True
    assert status["hooks"]["SessionStart"]["installed"] is True
    assert status["hooks"]["PreCompact"]["installed"] is True


# ---------------------------------------------------------------------------
# MCP server registration tests
# ---------------------------------------------------------------------------


def _mock_run_not_registered(
    *args: Any, **kwargs: Any
) -> subprocess.CompletedProcess[str]:
    """Simulate ``claude mcp get`` when server is not registered."""
    cmd: list[str] = args[0]
    if cmd[1:3] == ["mcp", "get"]:
        return subprocess.CompletedProcess(cmd, returncode=1, stdout="", stderr="")
    # ``claude mcp add`` succeeds
    return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")


def _mock_run_already_registered(
    *args: Any, **kwargs: Any
) -> subprocess.CompletedProcess[str]:
    """Simulate ``claude mcp get`` when server is already registered."""
    cmd: list[str] = args[0]
    if cmd[1:3] == ["mcp", "get"]:
        return subprocess.CompletedProcess(
            cmd, returncode=0, stdout=f"URL: {MCP_URL}\n", stderr=""
        )
    return subprocess.CompletedProcess(cmd, returncode=0, stdout="", stderr="")


def test_install_mcp_server_registers():
    """install_mcp_server calls claude mcp add when not registered."""
    with (
        patch(
            "ingest_sessions.install._find_claude_cli", return_value="/usr/bin/claude"
        ),
        patch("subprocess.run", side_effect=_mock_run_not_registered) as mock_run,
    ):
        result = install_mcp_server()

    assert result is True
    # Should have called get (check) then add
    assert mock_run.call_count == 2
    add_call = mock_run.call_args_list[1]
    assert "add" in add_call[0][0]
    assert "--transport" in add_call[0][0]
    assert "http" in add_call[0][0]


def test_install_mcp_server_idempotent():
    """install_mcp_server skips when already registered."""
    with (
        patch(
            "ingest_sessions.install._find_claude_cli", return_value="/usr/bin/claude"
        ),
        patch("subprocess.run", side_effect=_mock_run_already_registered) as mock_run,
    ):
        result = install_mcp_server()

    assert result is False
    assert mock_run.call_count == 1  # Only the get call


def test_install_mcp_server_no_claude():
    """install_mcp_server returns False when claude CLI is missing."""
    with patch("ingest_sessions.install._find_claude_cli", return_value=None):
        result = install_mcp_server()

    assert result is False


def test_uninstall_mcp_server():
    """uninstall_mcp_server calls claude mcp remove."""
    mock_result = subprocess.CompletedProcess([], returncode=0, stdout="", stderr="")
    with (
        patch(
            "ingest_sessions.install._find_claude_cli", return_value="/usr/bin/claude"
        ),
        patch("subprocess.run", return_value=mock_result) as mock_run,
    ):
        result = uninstall_mcp_server()

    assert result is True
    cmd = mock_run.call_args[0][0]
    assert "remove" in cmd
    assert MCP_SERVER_NAME in cmd


def test_check_mcp_server_registered():
    """check_mcp_server reports registered when get succeeds."""
    with (
        patch(
            "ingest_sessions.install._find_claude_cli", return_value="/usr/bin/claude"
        ),
        patch("subprocess.run", side_effect=_mock_run_already_registered),
    ):
        status = check_mcp_server()

    assert status["registered"] is True
    assert status["claude_cli"] is True
    assert status["url"] == MCP_URL


def test_check_mcp_server_not_registered():
    """check_mcp_server reports not registered."""
    with (
        patch(
            "ingest_sessions.install._find_claude_cli", return_value="/usr/bin/claude"
        ),
        patch("subprocess.run", side_effect=_mock_run_not_registered),
    ):
        status = check_mcp_server()

    assert status["registered"] is False
    assert status["claude_cli"] is True
