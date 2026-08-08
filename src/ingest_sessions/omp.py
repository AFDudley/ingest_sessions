"""omp (Oh My Pi) session transcript adapter (pebble is-5a7.1, is-5a7.4).

Parses omp's SessionEntry JSONL format into the SAME shared `records` /
`sessions` tables Claude Code sessions already populate (see core.py) --
selected by source (this module vs. `core.ingest_jsonl`), never a forked
schema or a second table.

The `type: "session"` HEADER (id, cwd, title, version, optional
parentSession) -- session metadata, never a data record -- is not always
line 1: omp keeps a fixed-width, rewritten-in-place `type: "title"` record
at the head of the file (see its `pad` field) so a session's title can be
updated without rewriting the whole transcript, which pushes the real
header to line 2 on the real corpus. `find_header` locates it by scanning
the leading `HEADER_SCAN_BOUND` lines for the first `type: "session"`
object with a non-empty `id`, rather than requiring index 0 -- the SAME
bounded scan `core.probe_session_format` uses, so classification and
ingestion can never disagree about whether a file is omp. Every line at
or before the header's index is session-level PREAMBLE: never inserted as
a record, never counted as malformed. Every other line is one member of
the omp SessionEntry union (`OMP_ENTRY_KINDS`), parsed according to the
header's declared schema version. Conversation order is NOT file order:
omp sessions can branch, so `reconstruct_conversation_order` walks each
entry's parentId back from a leaf to the root (matching
buildSessionContext semantics -- compaction entries truncate history at
`firstKeptEntryId`, branch_summary/custom_message entries become
synthetic messages).

# See contracts/specs/derived-is-5a7.1.spec.json, derived-is-5a7.4.spec.json
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import duckdb

from ingest_sessions.retrieval import _content_to_text

# The 13 SessionEntry kinds omp defines. Line 1 (`type: "session"`) is a
# separate HEADER, never one of these.
OMP_ENTRY_KINDS = frozenset(
    {
        "message",
        "thinking_level_change",
        "model_change",
        "service_tier_change",
        "compaction",
        "branch_summary",
        "custom",
        "custom_message",
        "label",
        "ttsr_injection",
        "session_init",
        "mode_change",
        "mcp_tool_selection",
    }
)

# branch_summary/custom_message entries carry prose directly on the entry
# rather than nested under `message` -- buildSessionContext treats them as
# synthetic messages, so this adapter synthesizes the same `message`
# shape the rest of the pipeline already reads.
_SYNTHETIC_MESSAGE_KINDS = frozenset({"branch_summary", "custom_message"})


# How many leading lines a header search may look at (pebble is-5a7.4).
# 2 covers every file on the real corpus today (title line + header line);
# kept small so a `session`-shaped line buried deep in a Claude Code
# transcript can never drag it into this adapter.
HEADER_SCAN_BOUND = 2


def parse_header(line: str) -> dict[str, Any] | None:
    """Parse one line of an omp session file as a session header.

    Returns None if the line isn't a valid `type: "session"` object with
    a non-empty `id` -- callers use this to detect a non-header line
    rather than raising.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict) or obj.get("type") != "session":
        return None
    if not obj.get("id"):
        return None
    return obj


def find_header(
    lines: list[str], bound: int = HEADER_SCAN_BOUND
) -> tuple[dict[str, Any] | None, int | None]:
    """Scan the first *bound* lines for the session header.

    Returns ``(header, index)`` for the first of the leading *bound*
    entries in *lines* that `parse_header`s as a valid header; ``(None,
    None)`` if none of them does -- callers use this to detect a non-omp
    file. `core.probe_session_format` calls this with the SAME bound over
    a file's first `HEADER_SCAN_BOUND` raw lines, so classification and
    ingestion (`ingest_omp_jsonl`) can never disagree about whether a file
    is omp.
    """
    for i, line in enumerate(lines[:bound]):
        header = parse_header(line)
        if header is not None:
            return header, i
    return None, None


def _parent_field(version: int) -> str:
    """Name of the parent-link field for this header version.

    v1 wrote the legacy `parent` field; v2 renamed it `parentId` (kept in
    v3) -- one of the version-specific interpretation rules `parse_entry`
    applies per c7.
    """
    return "parent" if version < 2 else "parentId"


def parse_entry(obj: Any, version: int) -> dict[str, Any] | None:
    """Normalize one SessionEntry line per its header's schema version.

    Returns None (malformed, caller skips) when *obj* isn't a dict, its
    `type` isn't one of `OMP_ENTRY_KINDS`, or it has no `id` -- matching
    the existing Claude Code path's non-fatal skip of unparseable lines.
    A recognized entry is returned with a normalized `parentId` (read
    from the version-appropriate source field, see `_parent_field`) and
    the owning `omp_version` so downstream reconstruction can apply
    version-specific rules (e.g. compaction truncation, v3+ only).
    """
    if not isinstance(obj, dict):
        return None
    if obj.get("type") not in OMP_ENTRY_KINDS:
        return None
    if not obj.get("id"):
        return None
    normalized = dict(obj)
    normalized["parentId"] = obj.get(_parent_field(version))
    normalized["omp_version"] = version
    return normalized


def extract_message_text(message: Any) -> str | None:
    """Extract text from a `message.content` field (string or block list).

    Reuses `retrieval._content_to_text` -- the same flattener the
    lexical/vector retrieval arms already use -- so a list-of-blocks
    `content` (each block's own `.text`) is read correctly instead of
    coming back empty, and omp records get identical text-extraction
    behavior to Claude Code records with zero query-path changes.
    """
    if not isinstance(message, dict):
        return None
    text = _content_to_text(message.get("content"))
    return text or None


def _synthesize_message(entry: dict[str, Any]) -> dict[str, Any]:
    """Build a `message` dict for a branch_summary/custom_message entry.

    These kinds carry their prose directly on the entry (`summary`,
    `text`, or `content`) rather than nested under `message` the way a
    `message` entry does.
    """
    text = entry.get("summary") or entry.get("text") or entry.get("content") or ""
    if not isinstance(text, str):
        text = ""
    role = entry.get("role") or "assistant"
    return {"role": role, "content": text}


def _with_synthetic_message(entry: dict[str, Any]) -> dict[str, Any]:
    """Attach a synthesized `message` to branch_summary/custom_message entries.

    A no-op for every other kind, and for an entry that already carries
    its own `message`.
    """
    if entry.get("type") in _SYNTHETIC_MESSAGE_KINDS and not isinstance(
        entry.get("message"), dict
    ):
        entry = dict(entry)
        entry["message"] = _synthesize_message(entry)
    return entry


def record_type_for_entry(entry: dict[str, Any]) -> str:
    """The `records.type` value for one normalized entry.

    `message` entries are typed by their own role (user/assistant),
    matching how Claude Code records are typed. branch_summary/
    custom_message are synthetic messages (role from
    `_synthesize_message`, default assistant). Every other kind is typed
    by its own kind name -- matching how Claude Code already types
    non-chat records (e.g. `summary`).
    """
    kind = entry.get("type")
    if kind == "message":
        message = entry.get("message")
        role = message.get("role") if isinstance(message, dict) else None
        return role or "message"
    if kind in _SYNTHETIC_MESSAGE_KINDS:
        return entry.get("role") or "assistant"
    return kind or ""


def omp_entry_to_record_row(
    entry: dict[str, Any], session_id: str
) -> tuple[str, str, str, Any, str | None, str]:
    """Map one normalized omp entry to the shared 6-column `records` row.

    header id -> session_id, entry id -> uuid, entry parentId ->
    parent_uuid. branch_summary/custom_message get a synthesized
    `message` (see `_with_synthetic_message`) so the rest of the
    pipeline (record_text, `core._extract_first_prompt`) sees the same
    `message.{role,content}` shape a Claude Code record has.
    """
    entry = _with_synthetic_message(entry)
    return (
        entry["id"],
        session_id,
        record_type_for_entry(entry),
        entry.get("timestamp"),
        entry.get("parentId"),
        json.dumps(entry),
    )


# ---------------------------------------------------------------------------
# DAG conversation-order reconstruction
# ---------------------------------------------------------------------------


def find_leaf_ids(entries: list[dict[str, Any]]) -> list[str]:
    """Entry ids that are nobody's parent -- the tips of the DAG's branches."""
    parented = {e["parentId"] for e in entries if e.get("parentId")}
    return [e["id"] for e in entries if e["id"] not in parented]


def _latest_leaf(entries: list[dict[str, Any]]) -> str | None:
    leaves = find_leaf_ids(entries)
    if not leaves:
        return None
    by_id = {e["id"]: e for e in entries}
    return max(leaves, key=lambda lid: (str(by_id[lid].get("timestamp") or ""), lid))


def reconstruct_conversation_order(
    entries: list[dict[str, Any]], leaf_id: str | None = None
) -> list[dict[str, Any]]:
    """Walk parentId back from a leaf to the root -- the DAG order, not file order.

    Only *leaf_id*'s ancestor chain is returned, so an off-branch sibling
    (the other side of a fork) never appears -- a branched session threads
    by parent linkage instead of replaying file order. Defaults to the
    latest leaf (an entry nobody else names as parentId) when *leaf_id* is
    omitted.

    Matches buildSessionContext semantics: a compaction entry from a v3+
    header causes every entry strictly older than its `firstKeptEntryId`
    to drop out of the reconstruction -- entries from `firstKeptEntryId`
    through the compaction entry itself are kept, everything before is
    not. (v1/v2 headers predate the compaction-truncation rule, so a
    compaction entry there is threaded like any other node.)
    branch_summary/custom_message entries come back with a synthesized
    `message`, i.e. as regular messages.
    """
    by_id = {e["id"]: e for e in entries}
    if leaf_id is None:
        leaf_id = _latest_leaf(entries)

    chain: list[dict[str, Any]] = []
    current = leaf_id
    visited: set[str] = set()
    stop_at: str | None = None
    while current is not None and current in by_id and current not in visited:
        visited.add(current)
        entry = by_id[current]
        chain.append(entry)
        if entry.get("type") == "compaction" and entry.get("omp_version", 3) >= 3:
            stop_at = entry.get("firstKeptEntryId")
        if stop_at is not None and current == stop_at:
            break
        current = entry.get("parentId")

    chain.reverse()
    return [_with_synthetic_message(e) for e in chain]


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


def _insert_new_records(
    db: duckdb.DuckDBPyConnection,
    *,
    session_id: str,
    batch: list[tuple[str, str, str, Any, str | None, str]],
) -> None:
    """Insert only the rows of *batch* not already stored for *session_id*.

    An omp transcript has no incremental byte-offset support (its header line
    must be seen on every pass), so every watchdog event re-parses the WHOLE
    file and arrives here with every row the file has ever held. Handing all
    of them to ``INSERT OR IGNORE`` looks free but is not: DuckDB executes one
    statement per row, and each one probes the ``records`` primary-key ART
    plus the three secondary ARTs (session_id, type, timestamp) over the
    full-corpus table. Measured on the live 436GB corpus that cost >10s per
    pass for a 12k-entry transcript against a 0.34s parse -- it monopolised
    the single DB thread (``server._db_loop``) so completely that inbound MCP
    queries expired on their bounded wait without ever executing.

    One indexed ``session_id`` lookup answers "which of these do we already
    have?" in a single scan, leaving only genuinely new rows to insert, so an
    append-only transcript costs work proportional to what was APPENDED. The
    result is identical to the unfiltered ``INSERT OR IGNORE``: a uuid already
    present under this session_id would have been ignored anyway, and a uuid
    present under a DIFFERENT session_id is not filtered here and still meets
    ``OR IGNORE`` on the primary key.
    """
    existing = {
        row[0]
        for row in db.execute(
            "SELECT uuid FROM records WHERE session_id = ?", [session_id]
        ).fetchall()
    }
    fresh = [row for row in batch if row[0] not in existing]
    if fresh:
        db.executemany("INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?, ?)", fresh)


def _insert_new_malformed(
    db: duckdb.DuckDBPyConnection,
    *,
    jsonl_path: Path,
    malformed: list[tuple[str, int, str, int]],
) -> None:
    """Insert only the malformed lines not already stored for *jsonl_path*.

    The ``_insert_new_records`` argument applies verbatim: a full re-parse
    re-offers every malformed line the file has ever held, and the
    ``malformed_lines`` primary key ``(file_path, byte_offset)`` makes the
    already-stored ones a no-op that still costs a per-row index probe.
    """
    existing = {
        row[0]
        for row in db.execute(
            "SELECT byte_offset FROM malformed_lines WHERE file_path = ?",
            [str(jsonl_path)],
        ).fetchall()
    }
    fresh = [row for row in malformed if row[1] not in existing]
    if fresh:
        db.executemany(
            "INSERT OR IGNORE INTO malformed_lines VALUES (?, ?, ?, ?)", fresh
        )


def ingest_omp_jsonl(
    db: duckdb.DuckDBPyConnection, jsonl_path: Path
) -> tuple[int, int, dict[str, Any] | None]:
    """Ingest one omp session file. Returns (record_count, malformed_count, header).

    The header is located by `find_header`'s bounded scan of the leading
    `HEADER_SCAN_BOUND` lines; *header* is None (0, 0 returned) when none
    of them is a valid `type: "session"` header, so callers can tell "not
    an omp file" from "empty file". Every line at or before the header's
    index -- including a `title` preamble line ahead of it -- is
    consumed as preamble: never inserted as a record, never counted as
    malformed. The header stays the sole source of session metadata
    (`ingest_omp_session_metadata` reads it, never the preamble). Every
    line after the header index is parsed per `parse_entry`, using the
    header's declared version; a line that fails JSON parsing or doesn't
    match a recognized SessionEntry kind is counted as malformed and
    skipped -- the ingest never aborts, mirroring
    `core.ingest_jsonl`'s existing non-fatal skip of malformed lines.

    The returned counts are what the FILE holds, not what was written: the
    whole file is re-parsed on every pass (no byte-offset support -- the
    header must be seen each time), and `_insert_new_records` /
    `_insert_new_malformed` write only the rows not already stored, so a
    re-ingest of an appended-to transcript costs work proportional to the
    APPEND rather than to the file.
    """
    raw = jsonl_path.read_bytes()
    now_ms = int(time.time() * 1000)
    raw_lines = raw.split(b"\n")

    leading = [
        raw_lines[i].decode("utf-8", errors="replace")
        for i in range(min(HEADER_SCAN_BOUND, len(raw_lines)))
    ]
    header, header_index = find_header(leading)
    if header is None or header_index is None:
        return 0, 0, None
    session_id = header["id"]
    version = header.get("version", 3)

    batch: list[tuple[str, str, str, Any, str | None, str]] = []
    malformed: list[tuple[str, int, str, int]] = []

    pos = 0
    for i, raw_line in enumerate(raw_lines):
        line_start = pos
        pos += len(raw_line) + 1
        if not raw_line.strip():
            continue
        if i <= header_index:
            continue  # session-level preamble (the header itself, or a
            # title line ahead of it) -- never a record, never malformed.
        line = raw_line.decode("utf-8", errors="replace")

        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            malformed.append((str(jsonl_path), line_start, line, now_ms))
            continue
        entry = parse_entry(obj, version)
        if entry is None:
            malformed.append((str(jsonl_path), line_start, line, now_ms))
            continue
        batch.append(omp_entry_to_record_row(entry, session_id))

    if batch:
        _insert_new_records(db, session_id=session_id, batch=batch)
    if malformed:
        _insert_new_malformed(db, jsonl_path=jsonl_path, malformed=malformed)
    return len(batch), len(malformed), header


def ingest_omp_session_metadata(
    db: duckdb.DuckDBPyConnection, header: dict[str, Any]
) -> None:
    """Insert session metadata for an omp session from its header + records.

    header id -> session_id, header cwd -> project_path, header title ->
    summary (the schema has no dedicated `title` column, and `summary` is
    otherwise unused by omp sessions). `created`/`modified`/
    `message_count`/`first_prompt` are derived from the already-ingested
    records, mirroring `core.derive_session_metadata`.
    """
    from ingest_sessions.core import _extract_first_prompt

    session_id = header["id"]
    row = db.execute(
        "SELECT min(timestamp), max(timestamp), count(*) FROM records "
        "WHERE session_id = ?",
        [session_id],
    ).fetchone()
    created, modified, message_count = row if row else (None, None, 0)

    first_user = db.execute(
        "SELECT raw FROM records WHERE session_id = ? AND type = 'user' "
        "ORDER BY timestamp ASC LIMIT 1",
        [session_id],
    ).fetchone()
    first_prompt = _extract_first_prompt(first_user[0]) if first_user else None

    db.execute(
        "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            session_id,
            header.get("title"),
            first_prompt,
            message_count,
            created,
            modified,
            None,
            header.get("cwd"),
        ],
    )


def discover_subagent_files(session_path: Path) -> list[Path]:
    """Find subagent transcript files beside *session_path*, recursively.

    A subagent transcript lives at `<session>/<AgentId>.jsonl` -- a
    directory sharing the parent session file's stem, holding one file
    per subagent. `rglob` walks that directory recursively, so a
    sub-subagent nested at `<session>/<AgentId>/<SubAgentId>.jsonl` is
    discovered from a single call at the top-level session file (c6).
    """
    sibling_dir = session_path.with_suffix("")
    if not sibling_dir.is_dir():
        return []
    return sorted(p for p in sibling_dir.rglob("*.jsonl") if p.is_file())


def ingest_omp_session(
    db: duckdb.DuckDBPyConnection, jsonl_path: Path
) -> dict[str, int]:
    """Ingest one omp session file plus every subagent file found beside it.

    Returns ``{"records", "malformed", "sessions", "subagent_files"}``.
    Each subagent file is itself a full omp session file (its own header
    + entries), ingested the same way as the parent.
    """
    subagent_files = discover_subagent_files(jsonl_path)
    total_records = 0
    total_malformed = 0
    sessions_seen = 0

    for path in [jsonl_path, *subagent_files]:
        count, malformed_count, header = ingest_omp_jsonl(db, path)
        total_records += count
        total_malformed += malformed_count
        if header is not None:
            ingest_omp_session_metadata(db, header)
            sessions_seen += 1

    return {
        "records": total_records,
        "malformed": total_malformed,
        "sessions": sessions_seen,
        "subagent_files": len(subagent_files),
    }
