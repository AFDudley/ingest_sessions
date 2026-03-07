"""Tests for the summarize module.

These test the pure functions (formatting, ID generation, escalation)
without LLM calls. LLM-dependent tests are in test_summarize_e2e.py.
"""

import json

from ingest_sessions.summarize import (
    build_deterministic_fallback,
    estimate_tokens,
    format_messages_for_summary,
    generate_summary_id,
    should_accept_output,
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


def test_should_accept_output_shorter():
    """Accept output that is shorter than input."""
    assert should_accept_output("short summary", 100) is True


def test_should_accept_output_longer():
    """Reject output that is longer than input."""
    long_text = "word " * 500
    assert should_accept_output(long_text, 10) is False


def test_should_accept_output_empty():
    """Reject empty output."""
    assert should_accept_output("", 100) is False
    assert should_accept_output("   ", 100) is False


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
