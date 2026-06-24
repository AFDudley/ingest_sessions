# Content-Addressable Blob Store Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract large message content at ingestion time into a content-addressable blob store on disk, replacing inline content with markers in DuckDB records to improve query performance and keep summarization prompts under ARG_MAX.

**Architecture:** During JSONL ingestion (`ingest_jsonl`), each record's message content blocks are checked against a size threshold (100KB / 25K tokens, matching Volt). Oversized blocks are written to disk as `~/.local/share/ingest_sessions/blobs/<aa>/<file_id>` (git-style sharding by first 2 hex chars of the SHA-256 ID). The record stored in DuckDB gets the content replaced with a compact marker `[Large Content: file_<hash>] [Tokens: ~N]`. A `blob_meta` table in DuckDB tracks metadata (file_id, session_id, token_count, original_size, created_at). The summarizer sees only markers, never raw large content. Retrieval by file_id reads from disk.

**Tech Stack:** Python 3.12, DuckDB, hashlib (SHA-256), pathlib

**Repo root:** `/home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions/`

---

## Prerequisite: Revert uncommitted truncation changes

Before starting, revert the dirty `_extract_content_text` truncation in `src/ingest_sessions/summarize.py` — the blob store replaces that approach. Keep the `_call_claude` single-path fix (using `--append-system-prompt`).

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git checkout src/ingest_sessions/summarize.py
```

Then re-apply only the `_call_claude` fix (the `--append-system-prompt` change from commit `1dc6eaf`). The blob store will handle the size problem properly.

---

### Task 1: Blob store module — write and read blobs

**Files:**
- Create: `src/ingest_sessions/blobs.py`
- Create: `tests/test_blobs.py`

**Step 1: Write the failing tests**

```python
"""Tests for the content-addressable blob store."""

import os
from pathlib import Path

from ingest_sessions.blobs import (
    BLOB_THRESHOLD_BYTES,
    blob_dir,
    generate_blob_id,
    is_large_content,
    read_blob,
    write_blob,
)


def test_generate_blob_id_deterministic():
    """Same content produces same ID."""
    content = "hello world" * 1000
    id1 = generate_blob_id(content)
    id2 = generate_blob_id(content)
    assert id1 == id2
    assert id1.startswith("file_")
    assert len(id1) == 5 + 16  # "file_" + 16 hex chars


def test_generate_blob_id_different_content():
    """Different content produces different IDs."""
    id1 = generate_blob_id("content A")
    id2 = generate_blob_id("content B")
    assert id1 != id2


def test_is_large_content_under_threshold():
    """Small content is not large."""
    assert is_large_content("small") is False


def test_is_large_content_over_threshold():
    """Content over 100KB is large."""
    large = "x" * (BLOB_THRESHOLD_BYTES + 1)
    assert is_large_content(large) is True


def test_write_and_read_blob(tmp_path: Path):
    """Write a blob, then read it back."""
    content = "hello world" * 5000
    file_id = generate_blob_id(content)

    write_blob(content, blob_root=tmp_path)
    result = read_blob(file_id, blob_root=tmp_path)

    assert result == content


def test_write_blob_creates_sharded_path(tmp_path: Path):
    """Blob is stored at <root>/<first 2 hex>/<file_id>."""
    content = "test content for sharding"
    file_id = generate_blob_id(content)
    hex_prefix = file_id[5:7]  # first 2 hex chars after "file_"

    write_blob(content, blob_root=tmp_path)

    expected_path = tmp_path / hex_prefix / file_id
    assert expected_path.exists()
    assert expected_path.read_text() == content


def test_write_blob_idempotent(tmp_path: Path):
    """Writing the same content twice doesn't error or change the file."""
    content = "idempotent content"
    write_blob(content, blob_root=tmp_path)
    write_blob(content, blob_root=tmp_path)

    file_id = generate_blob_id(content)
    assert read_blob(file_id, blob_root=tmp_path) == content


def test_read_blob_missing(tmp_path: Path):
    """Reading a nonexistent blob returns None."""
    result = read_blob("file_0000000000000000", blob_root=tmp_path)
    assert result is None


def test_blob_dir_default():
    """Default blob dir is under ~/.local/share/ingest_sessions/blobs."""
    d = blob_dir()
    assert d == Path.home() / ".local" / "share" / "ingest_sessions" / "blobs"
```

**Step 2: Run tests to verify they fail**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blobs.py -v`
Expected: FAIL with `ModuleNotFoundError` or `ImportError`

**Step 3: Write minimal implementation**

```python
"""Content-addressable blob store for large message content.

Large content blocks extracted during ingestion are stored on disk
at ``~/.local/share/ingest_sessions/blobs/<aa>/<file_id>`` where
``<aa>`` is the first two hex characters of the file ID (git-style
sharding). The file ID is ``file_`` + first 16 chars of SHA-256.

Content-addressable: same content = same ID = idempotent writes.

# See docs/plans/2026-03-08-blob-store.md
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

# Volt uses 100KB / 25K tokens. We match the byte threshold.
BLOB_THRESHOLD_BYTES = 100_000


def blob_dir() -> Path:
    """Default blob storage root."""
    env = os.environ.get("INGEST_SESSIONS_BLOBS_DIR")
    if env:
        return Path(env)
    return Path.home() / ".local" / "share" / "ingest_sessions" / "blobs"


def generate_blob_id(content: str) -> str:
    """Deterministic blob ID: file_ + SHA-256(content)[:16]."""
    h = hashlib.sha256(content.encode()).hexdigest()[:16]
    return f"file_{h}"


def is_large_content(content: str) -> bool:
    """Check if content exceeds the blob threshold."""
    return len(content) > BLOB_THRESHOLD_BYTES


def _blob_path(file_id: str, blob_root: Path | None = None) -> Path:
    """Compute the sharded path for a blob."""
    root = blob_root or blob_dir()
    hex_prefix = file_id[5:7]  # first 2 hex chars after "file_"
    return root / hex_prefix / file_id


def write_blob(content: str, blob_root: Path | None = None) -> str:
    """Write content to the blob store. Returns the file_id.

    Idempotent: if the blob already exists, this is a no-op.
    """
    file_id = generate_blob_id(content)
    path = _blob_path(file_id, blob_root)
    if path.exists():
        return file_id
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return file_id


def read_blob(file_id: str, blob_root: Path | None = None) -> str | None:
    """Read a blob by ID. Returns None if not found."""
    path = _blob_path(file_id, blob_root)
    if not path.exists():
        return None
    return path.read_text()
```

**Step 4: Run tests to verify they pass**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blobs.py -v`
Expected: all PASS

**Step 5: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/blobs.py tests/test_blobs.py
git commit -m "[ingest-sessions] Add content-addressable blob store module"
```

---

### Task 2: blob_meta table in DuckDB

**Files:**
- Modify: `src/ingest_sessions/core.py:45-111` (add table + index in `create_tables`)
- Modify: `src/ingest_sessions/blobs.py` (add `insert_blob_meta`, `get_blob_meta`)
- Modify: `tests/test_blobs.py` (add DuckDB metadata tests)

**Step 1: Write the failing tests**

Add to `tests/test_blobs.py`:

```python
import duckdb

from ingest_sessions.blobs import get_blob_meta, insert_blob_meta
from ingest_sessions.core import create_tables


def test_insert_and_get_blob_meta():
    """Insert blob metadata and retrieve it."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    insert_blob_meta(
        db,
        file_id="file_abcdef0123456789",
        session_id="sess-001",
        token_count=5000,
        original_size=80000,
    )

    meta = get_blob_meta(db, "file_abcdef0123456789")
    assert meta is not None
    assert meta["file_id"] == "file_abcdef0123456789"
    assert meta["session_id"] == "sess-001"
    assert meta["token_count"] == 5000
    assert meta["original_size"] == 80000


def test_insert_blob_meta_idempotent():
    """Inserting the same file_id twice is a no-op."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    insert_blob_meta(db, "file_abcdef0123456789", "sess-001", 5000, 80000)
    insert_blob_meta(db, "file_abcdef0123456789", "sess-001", 5000, 80000)

    meta = get_blob_meta(db, "file_abcdef0123456789")
    assert meta is not None


def test_get_blob_meta_missing():
    """Getting nonexistent blob meta returns None."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    assert get_blob_meta(db, "file_0000000000000000") is None
```

**Step 2: Run tests to verify they fail**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blobs.py -v -k "blob_meta"`
Expected: FAIL with `ImportError`

**Step 3: Write minimal implementation**

Add to `create_tables` in `src/ingest_sessions/core.py` (after the `summaries` table):

```python
    db.execute("""
        CREATE TABLE IF NOT EXISTS blob_meta (
            file_id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            token_count INTEGER NOT NULL,
            original_size INTEGER NOT NULL,
            created_at BIGINT NOT NULL
        )
    """)
```

And add an index at the end of `create_tables`:

```python
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_blob_meta_session_id ON blob_meta(session_id)"
    )
```

Add to `src/ingest_sessions/blobs.py`:

```python
import time
from typing import Any

import duckdb


def insert_blob_meta(
    db: duckdb.DuckDBPyConnection,
    file_id: str,
    session_id: str,
    token_count: int,
    original_size: int,
) -> None:
    """Record blob metadata in DuckDB. Idempotent."""
    ts = int(time.time() * 1000)
    db.execute(
        "INSERT OR IGNORE INTO blob_meta VALUES (?, ?, ?, ?, ?)",
        [file_id, session_id, token_count, original_size, ts],
    )


def get_blob_meta(
    db: duckdb.DuckDBPyConnection, file_id: str
) -> dict[str, Any] | None:
    """Get blob metadata by file_id. Returns None if not found."""
    row = db.execute(
        "SELECT file_id, session_id, token_count, original_size, created_at "
        "FROM blob_meta WHERE file_id = ?",
        [file_id],
    ).fetchone()
    if row is None:
        return None
    return {
        "file_id": row[0],
        "session_id": row[1],
        "token_count": row[2],
        "original_size": row[3],
        "created_at": row[4],
    }
```

**Step 4: Run tests to verify they pass**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blobs.py -v`
Expected: all PASS

**Step 5: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/blobs.py src/ingest_sessions/core.py tests/test_blobs.py
git commit -m "[ingest-sessions] Add blob_meta table and DuckDB metadata functions"
```

---

### Task 3: Extract large content at ingestion time

**Files:**
- Modify: `src/ingest_sessions/core.py:216-257` (`ingest_jsonl` — extract blobs before inserting records)
- Create: `tests/test_blob_ingestion.py`

**Step 1: Write the failing tests**

```python
"""Tests for blob extraction during JSONL ingestion."""

import json
from pathlib import Path

import duckdb

from ingest_sessions.blobs import blob_dir, generate_blob_id, read_blob
from ingest_sessions.core import create_tables, ingest_jsonl


def _make_record(uuid: str, session_id: str, content: str) -> str:
    """Build a JSONL line with the given content."""
    return json.dumps({
        "uuid": uuid,
        "sessionId": session_id,
        "type": "assistant",
        "timestamp": "2026-03-01T12:00:00.000Z",
        "parentUuid": None,
        "message": {"role": "assistant", "content": content},
    })


def _make_block_record(uuid: str, session_id: str, blocks: list[dict]) -> str:
    """Build a JSONL line with content blocks."""
    return json.dumps({
        "uuid": uuid,
        "sessionId": session_id,
        "type": "assistant",
        "timestamp": "2026-03-01T12:00:00.000Z",
        "parentUuid": None,
        "message": {"role": "assistant", "content": blocks},
    })


def test_small_content_not_extracted(tmp_path: Path):
    """Content under threshold stays inline in the record."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    jsonl = tmp_path / "sess-001.jsonl"
    jsonl.write_text(_make_record("msg-1", "sess-001", "small content") + "\n")

    blob_root = tmp_path / "blobs"
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    raw = json.loads(row[0])
    assert raw["message"]["content"] == "small content"


def test_large_string_content_extracted(tmp_path: Path):
    """Large string content is replaced with a marker in the record."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    large = "x" * 150_000
    jsonl = tmp_path / "sess-001.jsonl"
    jsonl.write_text(_make_record("msg-1", "sess-001", large) + "\n")

    blob_root = tmp_path / "blobs"
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    # Record should have a marker, not the original content
    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    raw = json.loads(row[0])
    content = raw["message"]["content"]
    assert "[Large Content:" in content
    assert "file_" in content
    assert len(content) < 1000  # marker is compact

    # Blob should be on disk
    file_id = generate_blob_id(large)
    assert read_blob(file_id, blob_root=blob_root) == large

    # Metadata should be in DuckDB
    meta = db.execute(
        "SELECT file_id, token_count FROM blob_meta WHERE file_id = ?",
        [file_id],
    ).fetchone()
    assert meta is not None
    assert meta[0] == file_id


def test_large_text_block_extracted(tmp_path: Path):
    """Large text block in content array is replaced with marker."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    large_text = "y" * 150_000
    blocks = [
        {"type": "text", "text": "small preamble"},
        {"type": "text", "text": large_text},
    ]
    jsonl = tmp_path / "sess-001.jsonl"
    jsonl.write_text(
        _make_block_record("msg-1", "sess-001", blocks) + "\n"
    )

    blob_root = tmp_path / "blobs"
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    raw = json.loads(row[0])
    content_blocks = raw["message"]["content"]
    assert content_blocks[0]["text"] == "small preamble"
    assert "[Large Content:" in content_blocks[1]["text"]

    file_id = generate_blob_id(large_text)
    assert read_blob(file_id, blob_root=blob_root) == large_text


def test_large_tool_result_extracted(tmp_path: Path):
    """Large tool_result content is replaced with marker."""
    db = duckdb.connect(":memory:")
    create_tables(db)

    large_result = "z" * 150_000
    blocks = [
        {"type": "tool_result", "tool_use_id": "tool-1", "content": large_result},
    ]
    jsonl = tmp_path / "sess-001.jsonl"
    jsonl.write_text(
        _make_block_record("msg-1", "sess-001", blocks) + "\n"
    )

    blob_root = tmp_path / "blobs"
    ingest_jsonl(db, jsonl, blob_root=blob_root)

    row = db.execute("SELECT raw FROM records WHERE uuid = 'msg-1'").fetchone()
    raw = json.loads(row[0])
    content_blocks = raw["message"]["content"]
    assert "[Large Content:" in content_blocks[0]["content"]

    file_id = generate_blob_id(large_result)
    assert read_blob(file_id, blob_root=blob_root) == large_result
```

**Step 2: Run tests to verify they fail**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blob_ingestion.py -v`
Expected: FAIL — `ingest_jsonl` doesn't accept `blob_root` parameter, no extraction happens

**Step 3: Write minimal implementation**

Modify `ingest_jsonl` in `src/ingest_sessions/core.py`:

```python
def ingest_jsonl(
    db: duckdb.DuckDBPyConnection,
    jsonl_path: Path,
    byte_offset: int = 0,
    blob_root: Path | None = None,
) -> int:
    """Ingest a single JSONL session file.  Returns record count.

    When byte_offset > 0, only lines after that offset are read (JSONL is
    append-only).  The first partial line after the offset is silently
    skipped to avoid parsing a truncated JSON object.

    When blob_root is provided, large content blocks are extracted to the
    blob store and replaced with compact markers before insertion into
    DuckDB.  See blobs.py for threshold and storage details.
    """
    from ingest_sessions.blobs import (
        generate_blob_id,
        insert_blob_meta,
        is_large_content,
        write_blob,
    )

    with open(jsonl_path, "r") as f:
        if byte_offset > 0:
            f.seek(byte_offset)
            f.readline()  # skip potential partial line at boundary
        text = f.read()

    batch = []
    session_id = jsonl_path.stem
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Extract large content blocks to blob store
        if blob_root is not None:
            _extract_blobs(db, record, session_id, blob_root)
            line = json.dumps(record)

        batch.append(
            (
                record.get("uuid", ""),
                record.get("sessionId", session_id),
                record.get("type", ""),
                record.get("timestamp"),
                record.get("parentUuid"),
                line,
            )
        )
    if batch:
        db.executemany(
            "INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )
    return len(batch)
```

Add helper function `_extract_blobs` to `src/ingest_sessions/core.py`:

```python
def _make_blob_marker(file_id: str, token_count: int) -> str:
    """Build the compact marker that replaces large content."""
    return f"[Large Content: {file_id}] [Tokens: ~{token_count}]"


def _extract_blobs(
    db: duckdb.DuckDBPyConnection,
    record: dict,
    session_id: str,
    blob_root: Path,
) -> None:
    """Extract large content blocks from a record, mutating it in place.

    Writes blobs to disk, records metadata in DuckDB, and replaces
    content with compact markers.
    """
    from ingest_sessions.blobs import (
        generate_blob_id,
        insert_blob_meta,
        is_large_content,
        write_blob,
    )

    msg = record.get("message")
    if not isinstance(msg, dict):
        return
    content = msg.get("content")
    if content is None:
        return

    # Case 1: content is a plain string
    if isinstance(content, str) and is_large_content(content):
        file_id = write_blob(content, blob_root=blob_root)
        token_count = len(content) // 4
        insert_blob_meta(db, file_id, session_id, token_count, len(content))
        msg["content"] = _make_blob_marker(file_id, token_count)
        return

    # Case 2: content is a list of blocks
    if not isinstance(content, list):
        return
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = block.get("text", "")
            if is_large_content(text):
                file_id = write_blob(text, blob_root=blob_root)
                token_count = len(text) // 4
                insert_blob_meta(db, file_id, session_id, token_count, len(text))
                block["text"] = _make_blob_marker(file_id, token_count)
        elif block.get("type") == "tool_result":
            result_content = block.get("content", "")
            if isinstance(result_content, str) and is_large_content(result_content):
                file_id = write_blob(result_content, blob_root=blob_root)
                token_count = len(result_content) // 4
                insert_blob_meta(
                    db, file_id, session_id, token_count, len(result_content)
                )
                block["content"] = _make_blob_marker(file_id, token_count)
            elif isinstance(result_content, list):
                for sub in result_content:
                    if isinstance(sub, dict) and sub.get("type") == "text":
                        text = sub.get("text", "")
                        if is_large_content(text):
                            file_id = write_blob(text, blob_root=blob_root)
                            token_count = len(text) // 4
                            insert_blob_meta(
                                db, file_id, session_id, token_count, len(text)
                            )
                            sub["text"] = _make_blob_marker(file_id, token_count)
```

**Step 4: Run tests to verify they pass**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_blob_ingestion.py -v`
Expected: all PASS

**Step 5: Run existing tests to verify nothing is broken**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/ -x -q`
Expected: all PASS (existing callers don't pass `blob_root`, so behavior is unchanged)

**Step 6: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/core.py tests/test_blob_ingestion.py
git commit -m "[ingest-sessions] Extract large content to blob store during ingestion"
```

---

### Task 4: Wire blob_root through the server

**Files:**
- Modify: `src/ingest_sessions/server.py:140-148` (`_ingest_file_full` — pass `blob_root`)
- Modify: `src/ingest_sessions/server.py:151-175` (`_ingest_all` — pass `blob_root`)
- Modify: `tests/test_server.py` (add test for blob extraction via server)

**Step 1: Write the failing test**

Add to `tests/test_server.py` (or a new section at the end):

```python
def test_server_extracts_blobs_on_ingest(tmp_path: Path):
    """Server ingestion extracts large content to blob store."""
    # This test verifies that _ingest_file_full passes blob_root
    # by checking that large content in a transcript produces blobs on disk.
    # Detailed extraction logic is tested in test_blob_ingestion.py.
    pass  # Integration tested via E2E — server wiring is the unit under test
```

Actually, this is better verified by checking the server passes `blob_root` to `ingest_jsonl`. The real test: modify `_ingest_file_full` and `_ingest_all` to pass `blob_root=blobs.blob_dir()`.

**Step 2: Modify the server**

In `src/ingest_sessions/server.py`, modify `_ingest_file_full`:

```python
def _ingest_file_full(db: duckdb.DuckDBPyConnection, jsonl_path: Path) -> int:
    """Ingest records AND session metadata for a single JSONL file."""
    from ingest_sessions.blobs import blob_dir

    _, prev_size = file_changed(db, jsonl_path)
    session_id = jsonl_path.stem
    count = ingest_jsonl(db, jsonl_path, byte_offset=prev_size, blob_root=blob_dir())
    session_meta = build_session_metadata([jsonl_path.parent])
    ingest_session_metadata(db, session_id, session_meta)
    record_file(db, jsonl_path)
    return count
```

In `_ingest_all`, modify the `ingest_jsonl` call:

```python
    from ingest_sessions.blobs import blob_dir

    # ... existing code ...
            ingest_jsonl(db, jsonl_path, byte_offset=prev_size, blob_root=blob_dir())
```

**Step 3: Run all tests**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/ -x -q`
Expected: all PASS

**Step 4: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/server.py
git commit -m "[ingest-sessions] Wire blob_root through server ingestion paths"
```

---

### Task 5: Revert _call_claude to single path with --append-system-prompt

**Files:**
- Modify: `src/ingest_sessions/summarize.py:322-343` (`_call_claude`)

This task cleans up the committed fallback chain from `1dc6eaf`. With the blob store extracting large content at ingestion time, `_call_claude` will never receive prompts exceeding ARG_MAX.

**Step 1: Apply the fix**

Replace the current `_call_claude` (which has the size-conditional fallback) with:

```python
def _call_claude(system_prompt: str, user_message: str) -> str:
    """Call claude --print with a system prompt and user message.

    Returns the text response. Raises on non-zero exit.
    Strips CLAUDECODE from env to allow nested subprocess calls.
    """
    claude_bin = _find_claude()
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    result = subprocess.run(
        [
            claude_bin,
            "--print",
            "--append-system-prompt",
            system_prompt,
            "-p",
            user_message,
            "--max-turns",
            "1",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"claude --print failed (exit {result.returncode}): {result.stderr[:500]}"
        )
    return result.stdout.strip()
```

**Step 2: Run all tests**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/ -x -q`
Expected: all PASS

**Step 3: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/summarize.py
git commit -m "[ingest-sessions] Revert _call_claude to single path, remove fallback chain"
```

---

### Task 6: Populate file_ids on summaries from blob markers

**Files:**
- Modify: `src/ingest_sessions/summarize.py:284-300` (uncomment and adapt `extract_file_ids`)
- Modify: `src/ingest_sessions/dag.py:65-90` (`insert_sprig` — extract and store file_ids)
- Modify: `src/ingest_sessions/dag.py:93-118` (`insert_bindle` — propagate file_ids)
- Modify: `tests/test_dag.py` (add file_ids propagation tests)

**Step 1: Write the failing tests**

Add to `tests/test_dag.py`:

```python
def test_sprig_captures_file_ids(db):
    """Sprig insertion extracts file_ids from content."""
    content = (
        "The user read a large file.\n"
        "[Large Content: file_abcdef0123456789] [Tokens: ~5000]\n"
        "Then they asked about another.\n"
        "[Large Content: file_1234567890abcdef] [Tokens: ~3000]"
    )
    summary_id = insert_sprig(db, "sess-001", content, ["msg-1", "msg-2"])
    row = db.execute(
        "SELECT file_ids FROM summaries WHERE summary_id = ?",
        [summary_id],
    ).fetchone()
    assert "file_abcdef0123456789" in row[0]
    assert "file_1234567890abcdef" in row[0]


def test_bindle_propagates_file_ids(db):
    """Bindle collects file_ids from parent sprigs."""
    content1 = "Summary with [Large Content: file_aaaa000000000000] [Tokens: ~1000]"
    content2 = "Summary with [Large Content: file_bbbb000000000000] [Tokens: ~2000]"
    insert_sprig(db, "sess-001", content1, ["msg-1"])
    insert_sprig(db, "sess-001", content2, ["msg-2"])

    sprigs = get_sprigs_for_session(db, "sess-001")
    parent_ids = [s["summary_id"] for s in sprigs]
    bindle_content = "Condensed summary mentioning both files."
    bindle_id = insert_bindle(db, "sess-001", bindle_content, parent_ids)

    row = db.execute(
        "SELECT file_ids FROM summaries WHERE summary_id = ?",
        [bindle_id],
    ).fetchone()
    assert "file_aaaa000000000000" in row[0]
    assert "file_bbbb000000000000" in row[0]
```

**Step 2: Run tests to verify they fail**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/test_dag.py -v -k "file_ids"`
Expected: FAIL — file_ids are empty `[]`

**Step 3: Implement**

Uncomment and adapt `extract_file_ids` in `src/ingest_sessions/summarize.py`:

```python
import re

_FILE_ID_PATTERN = re.compile(r"file_[0-9a-f]{16}")


def extract_file_ids(content: str) -> list[str]:
    """Extract deduplicated, sorted file IDs from text."""
    return sorted(set(_FILE_ID_PATTERN.findall(content)))
```

Modify `insert_sprig` in `src/ingest_sessions/dag.py`:

```python
def insert_sprig(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    content: str,
    message_uuids: list[str],
) -> str:
    """Insert a sprig (d1) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)
    file_ids = extract_file_ids(content)
    db.execute(
        "INSERT OR IGNORE INTO summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary_id, session_id, "sprig", 1,
            content, token_count, [], message_uuids, file_ids, ts,
        ],
    )
    return summary_id
```

Modify `insert_bindle` in `src/ingest_sessions/dag.py`:

```python
def insert_bindle(
    db: duckdb.DuckDBPyConnection,
    session_id: str,
    content: str,
    parent_ids: list[str],
) -> str:
    """Insert a bindle (d2) summary. Returns the summary_id."""
    ts = int(time.time() * 1000)
    summary_id = generate_summary_id(content, ts)
    token_count = estimate_tokens(content)

    # Collect file_ids from all parent summaries
    all_file_ids: list[str] = []
    for pid in parent_ids:
        row = db.execute(
            "SELECT file_ids FROM summaries WHERE summary_id = ?", [pid]
        ).fetchone()
        if row and row[0]:
            all_file_ids.extend(row[0])
    # Also extract from bindle content itself
    all_file_ids.extend(extract_file_ids(content))
    file_ids = sorted(set(all_file_ids))

    db.execute(
        "INSERT OR IGNORE INTO summaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            summary_id, session_id, "bindle", 2,
            content, token_count, parent_ids, [], file_ids, ts,
        ],
    )
    return summary_id
```

Update the import in `dag.py`:

```python
from ingest_sessions.summarize import (
    condense_summaries,
    estimate_tokens,
    extract_file_ids,
    generate_summary_id,
    summarize_messages,
)
```

**Step 4: Run tests**

Run: `cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions && uv run pytest tests/ -x -q`
Expected: all PASS

**Step 5: Commit**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
git add src/ingest_sessions/summarize.py src/ingest_sessions/dag.py tests/test_dag.py
git commit -m "[ingest-sessions] Extract and propagate file_ids through summary DAG"
```

---

### Task 7: Reinstall, restart, verify

**Step 1: Run full test suite**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
uv run pytest tests/ -x -q
```

**Step 2: Reinstall and restart**

```bash
cd /home/rix/code/git_puller/repos/ingest_sessions/ingest_sessions
uv tool install --force --reinstall .
XDG_RUNTIME_DIR=/run/user/$(id -u) ingest-sessions-install --restart
```

**Step 3: Verify service is running**

```bash
XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user status ingest-sessions.service
curl -s -X POST http://127.0.0.1:8741/api/context -H 'Content-Type: application/json' -d '{"session_id": "test-ping"}'
```

**Step 4: Verify blobs directory exists after next ingestion**

```bash
ls ~/.local/share/ingest_sessions/blobs/
```

**Step 5: Commit any final fixes, then done**
