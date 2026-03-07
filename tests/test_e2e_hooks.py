"""End-to-end tests for the LCM summary DAG hook pipeline.

Tests the full cycle:
1. Server starts with a JSONL transcript
2. PreCompact hook triggers refresh + summarization
3. SessionStart hook recovers context after /clear

Requires mock-claude on PATH (``uv tool install mock-claude``).
The mock detects summarization prompts and returns plausible text,
exercising the real subprocess boundary in summarize._call_claude.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

SERVER_SCRIPT = str(
    Path(__file__).resolve().parent.parent / "src" / "ingest_sessions" / "server.py"
)
HOOKS_DIR = Path(__file__).resolve().parent.parent / "src" / "ingest_sessions" / "hooks"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _mock_claude_bin() -> str | None:
    """Find mock-claude binary. Returns None if not installed."""
    return shutil.which("mock-claude")


def _make_record(uuid: str, session_id: str, msg_type: str, content: str) -> dict:
    """Build a JSONL record matching Claude Code transcript format."""
    return {
        "uuid": uuid,
        "sessionId": session_id,
        "type": msg_type,
        "timestamp": f"2026-03-01T12:00:{int(uuid.split('-')[-1]):02d}.000Z",
        "parentUuid": None,
        "message": {"role": msg_type, "content": content},
    }


def _write_transcript(path: Path, session_id: str, num_messages: int) -> None:
    """Write a fake JSONL transcript with alternating user/assistant messages."""
    with open(path, "w") as f:
        for i in range(num_messages):
            msg_type = "user" if i % 2 == 0 else "assistant"
            content = f"Message {i}: {'What about topic X?' if msg_type == 'user' else 'Here is information about topic X with details.'}"
            record = _make_record(f"msg-{i}", session_id, msg_type, content)
            f.write(json.dumps(record) + "\n")


def _wait_for_server(port: int, timeout: float = 10.0) -> None:
    """Block until the server accepts TCP connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError(f"Server did not start within {timeout}s on port {port}")


def _api_post(port: int, path: str, payload: dict, timeout: int = 120) -> dict:
    """POST JSON to the server REST API."""
    import urllib.request

    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


@pytest.fixture
def mock_claude_path() -> str:
    """Ensure mock-claude is available, skip if not installed."""
    path = _mock_claude_bin()
    if path is None:
        pytest.skip("mock-claude not installed (uv tool install mock-claude)")
    assert path is not None  # mypy narrowing after pytest.skip
    return path


@pytest.fixture
def server_env(tmp_path: Path, mock_claude_path: str) -> dict[str, str]:
    """Build environment dict for server and hook subprocesses.

    Creates a temp bin dir with a ``claude`` symlink pointing to mock-claude,
    and puts it first on PATH so summarize._call_claude finds the mock.
    """
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()

    # Create a bin dir where `claude` -> mock-claude
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "claude").symlink_to(mock_claude_path)

    return {
        **os.environ,
        "INGEST_SESSIONS_DB": str(tmp_path / "test.duckdb"),
        "INGEST_SESSIONS_PROJECTS_DIR": str(projects_dir),
        "INGEST_SESSIONS_HISTORY_FILE": str(tmp_path / "history.jsonl"),
        "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}",
    }


@pytest.fixture
def server(tmp_path: Path, server_env: dict[str, str]):
    """Start the HTTP server on a random port, yield (port, projects_dir)."""
    port = _free_port()
    env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, SERVER_SCRIPT, "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _wait_for_server(port)
    yield port, Path(server_env["INGEST_SESSIONS_PROJECTS_DIR"])
    proc.terminate()
    proc.wait(timeout=5)


def _run_hook(
    hook_script: str,
    stdin_data: dict,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run a hook script as a subprocess with JSON on stdin."""
    script_path = str(HOOKS_DIR / hook_script)
    return subprocess.run(
        [sys.executable, script_path],
        input=json.dumps(stdin_data),
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_precompact_creates_sprigs(server, server_env):
    """PreCompact hook ingests messages and creates sprig summaries."""
    port, projects_dir = server
    session_id = "sess-precompact-001"

    # Write transcript with enough messages for at least one sprig (20+)
    proj_dir = projects_dir / "test-project"
    proj_dir.mkdir()
    transcript = proj_dir / f"{session_id}.jsonl"
    _write_transcript(transcript, session_id, 25)

    # Run PreCompact hook
    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}
    result = _run_hook(
        "pre_compact.py",
        {
            "session_id": session_id,
            "transcript_path": str(transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "PreCompact",
            "trigger": "auto",
        },
        hook_env,
    )
    assert result.returncode == 0, f"Hook stderr: {result.stderr}"

    # Verify sprigs were created via REST API
    resp = _api_post(port, "/api/context", {"session_id": session_id})
    assert resp["context"] is not None
    assert "Summary" in resp["context"] or "summary" in resp["context"].lower()


def test_precompact_with_many_messages(server, server_env):
    """PreCompact with many messages creates multiple sprigs."""
    port, projects_dir = server
    session_id = "sess-many-001"

    # 100 messages = 5 sprig chunks
    proj_dir = projects_dir / "test-project"
    proj_dir.mkdir(exist_ok=True)
    transcript = proj_dir / f"{session_id}.jsonl"
    _write_transcript(transcript, session_id, 100)

    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}
    result = _run_hook(
        "pre_compact.py",
        {
            "session_id": session_id,
            "transcript_path": str(transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "PreCompact",
            "trigger": "auto",
        },
        hook_env,
    )
    assert result.returncode == 0, f"Hook stderr: {result.stderr}"

    resp = _api_post(port, "/api/context", {"session_id": session_id})
    ctx = resp["context"]
    assert ctx is not None
    # Multiple summaries should be present
    assert ctx.count("Summary ID:") >= 2


def test_session_start_recovers_context(server, server_env):
    """SessionStart hook after /clear returns recovery context."""
    port, projects_dir = server
    session_id = "sess-recovery-001"

    # Set up transcript and create summaries via PreCompact
    proj_dir = projects_dir / "test-project"
    proj_dir.mkdir(exist_ok=True)
    transcript = proj_dir / f"{session_id}.jsonl"
    _write_transcript(transcript, session_id, 25)

    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}

    # Run PreCompact first to create summaries
    result = _run_hook(
        "pre_compact.py",
        {
            "session_id": session_id,
            "transcript_path": str(transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "PreCompact",
            "trigger": "auto",
        },
        hook_env,
    )
    assert result.returncode == 0, f"PreCompact stderr: {result.stderr}"

    # Now simulate /clear — new session ID, same project dir
    new_session_id = "sess-recovery-002"
    new_transcript = proj_dir / f"{new_session_id}.jsonl"
    new_transcript.write_text("")  # Empty — new session after /clear

    result = _run_hook(
        "session_start.py",
        {
            "session_id": new_session_id,
            "transcript_path": str(new_transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "SessionStart",
            "source": "clear",
        },
        hook_env,
    )
    assert result.returncode == 0, f"SessionStart stderr: {result.stderr}"

    # Hook should output hookSpecificOutput with additionalContext
    stdout = result.stdout.strip()
    assert stdout, f"No output from SessionStart hook. stderr: {result.stderr}"
    output = json.loads(stdout)
    assert "hookSpecificOutput" in output
    ctx = output["hookSpecificOutput"]["additionalContext"]
    assert "summary" in ctx.lower() or "continued" in ctx.lower()


def test_session_start_noop_on_startup(server, server_env):
    """SessionStart hook with source=startup does nothing."""
    port, projects_dir = server
    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}

    result = _run_hook(
        "session_start.py",
        {
            "session_id": "sess-fresh",
            "transcript_path": str(projects_dir / "test" / "sess-fresh.jsonl"),
            "cwd": "/tmp",
            "hook_event_name": "SessionStart",
            "source": "startup",
        },
        hook_env,
    )
    assert result.returncode == 0
    # No output — hook exits early for non-clear sources
    assert result.stdout.strip() == ""


def test_full_cycle_ingest_summarize_recover(server, server_env):
    """Full cycle: write transcript → PreCompact → /clear → SessionStart → context."""
    port, projects_dir = server
    session_id = "sess-full-001"
    proj_dir = projects_dir / "test-project"
    proj_dir.mkdir(exist_ok=True)
    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}

    # Phase 1: Write initial transcript
    transcript = proj_dir / f"{session_id}.jsonl"
    _write_transcript(transcript, session_id, 45)

    # Phase 2: PreCompact triggers summarization
    result = _run_hook(
        "pre_compact.py",
        {
            "session_id": session_id,
            "transcript_path": str(transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "PreCompact",
            "trigger": "auto",
        },
        hook_env,
    )
    assert result.returncode == 0

    # Phase 3: Verify summaries exist
    resp = _api_post(port, "/api/context", {"session_id": session_id})
    assert resp["context"] is not None

    # Phase 4: Simulate /clear
    new_session_id = "sess-full-002"
    new_transcript = proj_dir / f"{new_session_id}.jsonl"
    new_transcript.write_text("")

    result = _run_hook(
        "session_start.py",
        {
            "session_id": new_session_id,
            "transcript_path": str(new_transcript),
            "cwd": str(proj_dir),
            "hook_event_name": "SessionStart",
            "source": "clear",
        },
        hook_env,
    )
    assert result.returncode == 0

    # Phase 5: Verify recovered context
    output = json.loads(result.stdout.strip())
    ctx = output["hookSpecificOutput"]["additionalContext"]
    assert "continued after a /clear" in ctx
    assert "Recent Activity" in ctx or "Condensed" in ctx or "Unsummarized" in ctx


def test_precompact_empty_stdin(server, server_env):
    """PreCompact hook handles empty stdin gracefully."""
    port, _ = server
    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(port)}

    script = str(HOOKS_DIR / "pre_compact.py")
    result = subprocess.run(
        [sys.executable, script],
        input="",
        capture_output=True,
        text=True,
        env=hook_env,
        timeout=10,
    )
    assert result.returncode == 0


def test_precompact_no_server(server_env):
    """PreCompact hook handles unreachable server gracefully."""
    # Use a port where nothing is listening
    dead_port = _free_port()
    hook_env = {**server_env, "INGEST_SESSIONS_PORT": str(dead_port)}

    result = _run_hook(
        "pre_compact.py",
        {
            "session_id": "sess-noserver",
            "transcript_path": "/tmp/fake.jsonl",
            "cwd": "/tmp",
            "hook_event_name": "PreCompact",
            "trigger": "auto",
        },
        hook_env,
    )
    # Should not crash — errors go to stderr
    assert result.returncode == 0
    assert "error" in result.stderr.lower() or "not reachable" in result.stderr.lower()
