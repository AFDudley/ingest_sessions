"""Install/repair ingest_sessions hooks, MCP server, and systemd service.

Idempotent: safe to run on first install or as a repair operation.

Manages three components:
1. Claude Code hooks (SessionStart, PreCompact) in ~/.claude/settings.json
2. MCP server registration via ``claude mcp add``
3. systemd user service for the ingest-sessions-server process

# See docs/plans/2026-03-07-lcm-summary-dag.md
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


SETTINGS_PATH = Path.home() / ".claude" / "settings.json"

HOOKS_DIR = Path(__file__).parent / "hooks"

# Each hook: (event_name, script_filename, timeout_seconds)
HOOK_DEFS: list[tuple[str, str, int]] = [
    ("SessionStart", "session_start.py", 15),
    ("PreCompact", "pre_compact.py", 180),
]

MCP_SERVER_NAME = "ingest-sessions"
DEFAULT_PORT = int(os.environ.get("INGEST_SESSIONS_PORT", "8741"))
MCP_URL = f"http://127.0.0.1:{DEFAULT_PORT}/mcp"

SERVICE_NAME = "ingest-sessions"
SERVICE_DIR = Path.home() / ".config" / "systemd" / "user"
SERVICE_PATH = SERVICE_DIR / f"{SERVICE_NAME}.service"

# The server binary is installed by uv into ~/.local/bin/.
# %h expands to $HOME in systemd unit files.
SERVICE_UNIT = """\
[Unit]
Description=ingest-sessions MCP server
After=network.target

[Service]
Type=simple
ExecStart=%h/.local/bin/ingest-sessions-server
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"""


def _hook_command(script_path: Path) -> str:
    """Build the hook command string pointing to the installed script."""
    return f"python3 {script_path}"


def _load_settings() -> dict[str, Any]:
    """Load existing settings, or return empty dict if none exist."""
    if not SETTINGS_PATH.exists():
        return {}
    return json.loads(SETTINGS_PATH.read_text())


def _save_settings(settings: dict[str, Any]) -> None:
    """Write settings back, creating parent dirs if needed."""
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(settings, indent=2) + "\n")


def _hook_entry(script_path: Path, timeout: int) -> dict[str, Any]:
    """Build the hook matcher entry for a given event."""
    return {
        "hooks": [
            {
                "type": "command",
                "command": _hook_command(script_path),
                "timeout": timeout,
            }
        ]
    }


def _is_our_hook(entry: dict[str, Any], script_path: Path) -> bool:
    """Check if a hook matcher entry is ours (by exact command match)."""
    cmd = _hook_command(script_path)
    hooks = entry.get("hooks", [])
    return any(h.get("command") == cmd for h in hooks)


def install_hooks() -> list[str]:
    """Install all hooks into Claude Code settings.

    Returns list of event names that were added (empty if all present).
    """
    added: list[str] = []
    settings = _load_settings()
    hooks_section = settings.setdefault("hooks", {})

    for event_name, script_filename, timeout in HOOK_DEFS:
        script_path = HOOKS_DIR / script_filename
        if not script_path.exists():
            print(
                f"Hook script not found at {script_path}. "
                "Is ingest-sessions installed correctly?",
                file=sys.stderr,
            )
            raise FileNotFoundError(script_path)

        event_hooks = hooks_section.setdefault(event_name, [])
        already = any(_is_our_hook(e, script_path) for e in event_hooks)
        if not already:
            event_hooks.append(_hook_entry(script_path, timeout))
            added.append(event_name)

    if added:
        _save_settings(settings)
    return added


def uninstall_hooks() -> list[str]:
    """Remove all our hooks from Claude Code settings.

    Returns list of event names that were removed (empty if none found).
    """
    removed: list[str] = []
    settings = _load_settings()
    hooks_section = settings.get("hooks", {})

    for event_name, script_filename, _timeout in HOOK_DEFS:
        script_path = HOOKS_DIR / script_filename
        event_hooks = hooks_section.get(event_name, [])
        filtered = [e for e in event_hooks if not _is_our_hook(e, script_path)]
        if len(filtered) < len(event_hooks):
            removed.append(event_name)
            if filtered:
                hooks_section[event_name] = filtered
            else:
                hooks_section.pop(event_name, None)

    if removed:
        if not hooks_section:
            settings.pop("hooks", None)
        _save_settings(settings)
    return removed


def check_hooks() -> dict[str, Any]:
    """Check installation status. Returns a status dict."""
    settings = _load_settings()
    hooks_section = settings.get("hooks", {})
    python3_available = shutil.which("python3") is not None

    hook_statuses: dict[str, Any] = {}
    for event_name, script_filename, _timeout in HOOK_DEFS:
        script_path = HOOKS_DIR / script_filename
        event_hooks = hooks_section.get(event_name, [])
        hook_statuses[event_name] = {
            "installed": any(_is_our_hook(e, script_path) for e in event_hooks),
            "script_exists": script_path.exists(),
            "script_path": str(script_path),
            "command": _hook_command(script_path) if script_path.exists() else None,
        }

    all_installed = all(h["installed"] for h in hook_statuses.values())
    all_scripts_exist = all(h["script_exists"] for h in hook_statuses.values())

    return {
        "all_installed": all_installed,
        "all_scripts_exist": all_scripts_exist,
        "python3_available": python3_available,
        "settings_path": str(SETTINGS_PATH),
        "hooks": hook_statuses,
    }


def _find_claude_cli() -> str | None:
    """Find the ``claude`` CLI binary."""
    return shutil.which("claude")


def install_mcp_server() -> bool:
    """Register the ingest-sessions MCP server with Claude Code.

    Uses ``claude mcp add --transport http`` at user scope.
    Returns True if registered, False if already present or claude CLI missing.
    """
    claude = _find_claude_cli()
    if not claude:
        print(
            "claude CLI not found in PATH; skipping MCP registration", file=sys.stderr
        )
        return False

    # Check if already registered
    result = subprocess.run(
        [claude, "mcp", "get", MCP_SERVER_NAME],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and MCP_URL in result.stdout:
        return False

    subprocess.run(
        [claude, "mcp", "add", "--transport", "http", MCP_SERVER_NAME, MCP_URL],
        capture_output=True,
        text=True,
        check=True,
    )
    return True


def uninstall_mcp_server() -> bool:
    """Remove the ingest-sessions MCP server from Claude Code.

    Returns True if removed, False if not found or claude CLI missing.
    """
    claude = _find_claude_cli()
    if not claude:
        return False

    result = subprocess.run(
        [claude, "mcp", "remove", MCP_SERVER_NAME, "-s", "user"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def check_mcp_server() -> dict[str, Any]:
    """Check MCP server registration status."""
    claude = _find_claude_cli()
    if not claude:
        return {"registered": False, "claude_cli": False, "url": MCP_URL}

    result = subprocess.run(
        [claude, "mcp", "get", MCP_SERVER_NAME],
        capture_output=True,
        text=True,
    )
    registered = result.returncode == 0 and MCP_URL in result.stdout
    return {"registered": registered, "claude_cli": True, "url": MCP_URL}


def _systemctl(*args: str) -> subprocess.CompletedProcess[str]:
    """Run systemctl --user with XDG_RUNTIME_DIR set."""
    env = {**os.environ, "XDG_RUNTIME_DIR": f"/run/user/{os.getuid()}"}
    return subprocess.run(
        ["systemctl", "--user", *args],
        capture_output=True,
        text=True,
        env=env,
    )


def install_service() -> bool:
    """Install and enable the systemd user service.

    Writes the unit file, reloads systemd, and enables + starts the service.
    Returns True if the unit file was written (new or updated).
    """
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    existing = SERVICE_PATH.read_text() if SERVICE_PATH.exists() else ""
    changed = existing != SERVICE_UNIT
    if changed:
        SERVICE_PATH.write_text(SERVICE_UNIT)
        _systemctl("daemon-reload")
    _systemctl("enable", SERVICE_NAME)
    _systemctl("start", SERVICE_NAME)
    return changed


def uninstall_service() -> bool:
    """Stop, disable, and remove the systemd user service.

    Returns True if the unit file existed and was removed.
    """
    if not SERVICE_PATH.exists():
        return False
    _systemctl("stop", SERVICE_NAME)
    _systemctl("disable", SERVICE_NAME)
    SERVICE_PATH.unlink()
    _systemctl("daemon-reload")
    return True


def restart_service() -> bool:
    """Restart the systemd user service.

    Returns True if the service is active after restart.
    """
    _systemctl("restart", SERVICE_NAME)
    result = _systemctl("is-active", SERVICE_NAME)
    return result.stdout.strip() == "active"


def check_service() -> dict[str, Any]:
    """Check systemd service status."""
    unit_exists = SERVICE_PATH.exists()
    result = _systemctl("is-active", SERVICE_NAME)
    active = result.stdout.strip() == "active"
    result = _systemctl("is-enabled", SERVICE_NAME)
    enabled = result.stdout.strip() == "enabled"
    return {
        "unit_file": str(SERVICE_PATH),
        "unit_exists": unit_exists,
        "enabled": enabled,
        "active": active,
    }


def cli() -> None:
    """Entry point for ``ingest-sessions-install``.

    Usage::

        ingest-sessions-install              # install hooks + service
        ingest-sessions-install --check      # check status
        ingest-sessions-install --uninstall  # remove hooks + service
        ingest-sessions-install --restart    # restart the server service
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Install/repair ingest-sessions hooks, MCP server, and systemd service"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check installation status without modifying anything",
    )
    parser.add_argument(
        "--uninstall",
        action="store_true",
        help="Remove all ingest-sessions hooks, MCP server, and systemd service",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Restart the ingest-sessions-server systemd service",
    )
    args = parser.parse_args()

    if args.check:
        hook_status = check_hooks()
        mcp_status = check_mcp_server()
        svc_status = check_service()
        combined = {**hook_status, "mcp_server": mcp_status, "service": svc_status}
        print(json.dumps(combined, indent=2))
        ok = (
            hook_status["all_installed"]
            and hook_status["all_scripts_exist"]
            and mcp_status["registered"]
            and svc_status["active"]
        )
        sys.exit(0 if ok else 1)

    if args.restart:
        if restart_service():
            print("Service restarted successfully.")
        else:
            print("Service restart failed.", file=sys.stderr)
            sys.exit(1)
        return

    if args.uninstall:
        removed = uninstall_hooks()
        if removed:
            print(f"Hooks removed: {', '.join(removed)}")
        else:
            print("No ingest-sessions hooks found in settings.")
        mcp_removed = uninstall_mcp_server()
        if mcp_removed:
            print("MCP server registration removed.")
        else:
            print("MCP server not registered (nothing to remove).")
        svc_removed = uninstall_service()
        if svc_removed:
            print("Systemd service stopped, disabled, and removed.")
        else:
            print("Systemd service not installed (nothing to remove).")
        return

    added = install_hooks()
    if added:
        print(f"Hooks installed in {SETTINGS_PATH}: {', '.join(added)}")
    else:
        print("All hooks already installed. No changes made.")

    mcp_added = install_mcp_server()
    if mcp_added:
        print(f"MCP server registered: {MCP_SERVER_NAME} -> {MCP_URL}")
    else:
        print("MCP server already registered. No changes made.")

    svc_changed = install_service()
    if svc_changed:
        print(f"Systemd service installed: {SERVICE_PATH}")
    else:
        print("Systemd service already installed. Ensured running.")

    hook_status = check_hooks()
    if not hook_status["python3_available"]:
        print("WARNING: python3 not found in PATH", file=sys.stderr)


if __name__ == "__main__":
    cli()
