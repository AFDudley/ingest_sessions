"""Tests for the summarize module.

These test the pure functions (formatting, ID generation, escalation)
without LLM calls. LLM-dependent tests use mocks.
"""

import json
from unittest.mock import patch

from ingest_sessions.summarize import (
    _call_claude,
    _should_accept_output,
    build_deterministic_fallback,
    condense_summaries,
    estimate_tokens,
    format_messages_for_summary,
    generate_summary_id,
    summarize_messages,
)


def test_generate_summary_id_deterministic():
    """Same content + timestamp = same ID."""
    id1 = generate_summary_id("hello world", 1000)
    id2 = generate_summary_id("hello world", 1000)
    assert id1 == id2
    assert id1.startswith("sum_")
    assert len(id1) == 20  # "sum_" + 16 hex chars


def test_generate_summary_id_differs_on_content():
    """Different content = different ID."""
    id1 = generate_summary_id("hello", 1000)
    id2 = generate_summary_id("world", 1000)
    assert id1 != id2


def test_format_messages_for_summary():
    """Messages are formatted with role and content."""
    records = [
        {
            "uuid": "r1",
            "type": "user",
            "raw": json.dumps(
                {
                    "message": {"role": "user", "content": "hello"},
                }
            ),
        },
        {
            "uuid": "r2",
            "type": "assistant",
            "raw": json.dumps(
                {
                    "message": {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "hi there"}],
                    },
                }
            ),
        },
    ]
    result = format_messages_for_summary(records)
    assert "[Message r1] (user)" in result
    assert "hello" in result
    assert "[Message r2] (assistant)" in result
    assert "hi there" in result


def test_format_messages_malformed_json():
    """Malformed JSON records produce [malformed record] placeholder."""
    records = [{"uuid": "bad", "type": "user", "raw": "not json{{{"}]
    result = format_messages_for_summary(records)
    assert "[malformed record]" in result


def test_should_accept_output_shorter():
    """Accept output that is shorter than input."""
    assert _should_accept_output("short summary", 100) is True


def test_should_accept_output_longer():
    """Reject output that is longer than input."""
    long_text = "word " * 500
    assert _should_accept_output(long_text, 10) is False


def test_should_accept_output_empty():
    """Reject empty output."""
    assert _should_accept_output("", 100) is False
    assert _should_accept_output("   ", 100) is False


def test_build_deterministic_fallback():
    """Fallback truncates to fit under token budget."""
    source = "word " * 200
    result = build_deterministic_fallback(source, input_tokens=50)
    assert estimate_tokens(result) < 50
    assert "truncated" in result.lower()


def test_estimate_tokens():
    """Rough token estimation works."""
    tokens = estimate_tokens("hello world this is a test")
    assert 4 <= tokens <= 10  # ~6 tokens, rough estimate


def test_summarize_messages_falls_back_on_llm_failure():
    """summarize_messages uses deterministic fallback when _call_claude raises."""
    records = [
        {
            "uuid": f"r{i}",
            "type": "user",
            "raw": json.dumps(
                {"message": {"role": "user", "content": f"Message {i} " + "x" * 100}}
            ),
        }
        for i in range(5)
    ]
    with patch(
        "ingest_sessions.summarize._call_claude",
        side_effect=RuntimeError("claude --print failed"),
    ):
        result = summarize_messages(records)

    # Should fall through to deterministic fallback, not raise
    assert "truncated" in result.lower()
    assert len(result) > 0


def test_condense_summaries_falls_back_on_llm_failure():
    """condense_summaries uses deterministic fallback when _call_claude raises."""
    summaries = [
        {
            "summary_id": f"sum_{i:016x}",
            "content": f"Sprig summary {i} with enough content " + "y" * 200,
            "token_count": 100,
        }
        for i in range(6)
    ]
    with patch(
        "ingest_sessions.summarize._call_claude",
        side_effect=TimeoutError("subprocess timed out"),
    ):
        result = condense_summaries(summaries)

    # Should fall through to deterministic fallback, not raise
    assert "truncated" in result.lower()
    assert len(result) > 0


def test_call_claude_handles_null_bytes():
    """_call_claude replaces null bytes instead of crashing."""
    stdout_with_null = b"hello\x00world"
    stderr_bytes = b""
    mock_result = type(
        "CompletedProcess",
        (),
        {"returncode": 0, "stdout": stdout_with_null, "stderr": stderr_bytes},
    )()
    with patch(
        "ingest_sessions.summarize._find_claude", return_value="/usr/bin/claude"
    ):
        with patch("subprocess.run", return_value=mock_result):
            output = _call_claude("system", "user")
    assert "\x00" not in output
    assert "hello" in output
    assert "world" in output


def test_call_claude_null_bytes_in_stderr():
    """_call_claude handles null bytes in stderr on failure."""
    mock_result = type(
        "CompletedProcess",
        (),
        {"returncode": 1, "stdout": b"", "stderr": b"error\x00msg"},
    )()
    with patch(
        "ingest_sessions.summarize._find_claude", return_value="/usr/bin/claude"
    ):
        with patch("subprocess.run", return_value=mock_result):
            try:
                _call_claude("system", "user")
                assert False, "Should have raised"
            except RuntimeError as exc:
                assert "error" in str(exc)
                assert "\x00" not in str(exc)
