"""Deterministic artifact -> supersession-link adapter (pebble is-565.6).

CONSUMER-SIDE tool. ingest_sessions is project-AGNOSTIC: it stores opaque
``(superseding_id, superseded_id, source)`` links and ranks over them, but it
never parses a consuming repo's docs. This standalone script is the piece that
DOES — it is RUN against a checkout of a consuming repo (e.g. an mtm clone),
scans its decision/labbook markdown + git history for supersession relations,
and POSTs SESSION-level links to a running ingest-sessions server. It imports
NOTHING from ingest_sessions and hardcodes NO consumer (mtm) semantics; it is a
generic markdown/git scanner.

What it parses
--------------
1. **Markdown supersession headers** under ``docs/decisions/`` and
   ``docs/labbook/``. An ADR/labbook entry declares a relation with a line
   like ``Superseded by: <ref>`` or ``Supersedes: <ref>`` (also matched in
   prose, e.g. "... superseded by ..."). The ``<ref>`` is a path/slug naming
   ANOTHER artifact. Each artifact is expected to cite its ingest-sessions
   **session of record** with a ``Session: <uuid>`` line. We resolve both ends
   of a relation to their cited session ids and emit a SESSION-level link
   ``(superseding_session_id supersedes superseded_session_id)`` tagged
   ``adr-supersedes`` / ``labbook-supersedes``.

2. **Git reverts** from ``git log``. A commit whose subject starts with
   ``Revert `` (or whose body says ``This reverts commit <sha>``) supersedes
   the reverted commit. We emit a link keyed by COMMIT SHA with source
   ``git-revert``.

   HONESTY / KNOWN LIMITATION: a commit sha is NOT a session id, and this
   adapter has no sha -> session mapping. So git-revert links are RECORDED
   (auditable) but will NOT affect ranking — ranking flags a candidate only
   when the superseded id matches a record uuid or a known session_id, and a
   bare sha matches neither. These links are surfaced separately and the
   dry-run labels them ``(inert: sha not session-mapped)``. We do NOT pretend
   a sha maps to a session.

Functional core, imperative shell: every parser
(:func:`parse_artifact`, :func:`resolve_session_links`,
:func:`parse_git_reverts`) is a PURE function over text/data and is unit-tested
without HTTP or a filesystem. The only IO is at the boundary —
:func:`scan_artifacts` (read markdown), :func:`read_git_log` (subprocess), and
:func:`post_links` (HTTP POST). Deterministic + fail-fast: parsers raise on
malformed input rather than guessing, and ordering is stable (sorted).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.request
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

# A canonical RFC-4122-shaped token: ingest-sessions session ids are uuids.
UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

# "Session: <uuid>" / "Session-of-record: <uuid>" / "session <uuid>" — the
# artifact's reference of record. Case-insensitive; the ``:``/``=`` is optional
# so prose citations are caught too.
SESSION_CITATION_RE = re.compile(
    r"(?i)session(?:[-\s]of[-\s]record)?\s*[:=]?\s*(" + UUID_RE.pattern + r")"
)

# "Superseded by: <ref>" / "superseded-by <ref>" — <ref> names the artifact
# that supersedes THIS one.
SUPERSEDED_BY_RE = re.compile(r"(?i)superseded[-\s]by\s*[:=]?\s*(\S+)")

# "Supersedes: <ref>" — <ref> names the artifact THIS one supersedes.
SUPERSEDES_RE = re.compile(r"(?i)\bsupersedes\s*[:=]?\s*(\S+)")

# A markdown inline link ``[text](target)`` — we want the target.
MD_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")

# Fenced (```...```) and inline (`...`) code spans. These hold DOCUMENTATION —
# a convention ADR / template SHOWS example `Session:` / `Supersedes:` lines in
# code blocks. Without stripping them, a doc that merely *describes* the format
# parses as real data and emits FALSE supersession links (observed: the
# session-of-record ADR's worked example using real slugs created two bogus
# links). Citations and relations are PLAIN lines by convention, never code, so
# stripping code loses no real data.
_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`[^`]*`")
# The filename of a (non-artifact) entry template; never parsed as a record.
_TEMPLATE_NAME = "template.md"


def strip_code(text: str) -> str:
    """Remove fenced + inline code so example lines are not parsed as data (pure).

    Fenced blocks are replaced with their newline count (line boundaries
    preserved so no two real lines merge); inline spans become a space.
    """
    no_fences = _CODE_FENCE_RE.sub(lambda m: "\n" * m.group(0).count("\n"), text)
    return _INLINE_CODE_RE.sub(" ", no_fences)


# The directories a consuming repo keeps decisions / labbook entries in, with
# the source tag a relation found there is stamped with.
ARTIFACT_DIRS: tuple[tuple[str, str], ...] = (
    ("docs/decisions", "adr-supersedes"),
    ("docs/labbook", "labbook-supersedes"),
)


@dataclass(frozen=True)
class Artifact:
    """A parsed decision/labbook markdown file (pure value)."""

    path: str
    """Repo-relative path, e.g. ``docs/decisions/2026-01-01-foo.md``."""
    source: str
    """Source tag for relations declared here (``adr-supersedes`` …)."""
    session_ids: tuple[str, ...]
    """ingest-sessions session ids this artifact cites as its record."""
    superseded_by: tuple[str, ...] = field(default_factory=tuple)
    """Refs (paths/slugs) of artifacts that supersede THIS one."""
    supersedes: tuple[str, ...] = field(default_factory=tuple)
    """Refs (paths/slugs) of artifacts THIS one supersedes."""


@dataclass(frozen=True)
class SessionLink:
    """A resolved SESSION-level supersession link, ready to POST (pure)."""

    superseding_id: str
    superseded_id: str
    source: str

    def as_dict(self) -> dict[str, str]:
        return {
            "superseding_id": self.superseding_id,
            "superseded_id": self.superseded_id,
            "source": self.source,
        }


@dataclass(frozen=True)
class RevertLink:
    """A git-revert link keyed by commit SHA (pure).

    INERT for ranking — a sha is neither a record uuid nor a session id — but
    recorded for audit. See module docstring.
    """

    superseding_id: str
    superseded_id: str
    source: str = "git-revert"

    def as_dict(self) -> dict[str, str]:
        return {
            "superseding_id": self.superseding_id,
            "superseded_id": self.superseded_id,
            "source": self.source,
        }


# --------------------------------------------------------------------------- #
# Pure parsers
# --------------------------------------------------------------------------- #


def _clean_ref(token: str) -> str:
    """Normalize a captured supersession ref to a comparable key (pure).

    Strips markdown-link syntax, backticks, surrounding punctuation/quotes and
    a trailing period so ``[ADR-7](./adr-0007-foo.md).`` and
    ``adr-0007-foo.md`` compare equal downstream via :func:`_ref_keys`.
    """
    link = MD_LINK_RE.search(token)
    if link:
        token = link.group(1)
    return token.strip().strip("`'\"<>()[].,;:")


def _ref_keys(ref: str) -> set[str]:
    """Return the set of keys a ref may match an artifact on (pure).

    A relation ref can be a full path, a basename, or a slug (stem). We index
    artifacts by all three so a cross-reference resolves regardless of which
    form the author used. A uuid ref (a direct session-id citation) is returned
    as-is so it can short-circuit artifact resolution.
    """
    cleaned = _clean_ref(ref)
    if not cleaned:
        return set()
    p = PurePosixPath(cleaned.replace("\\", "/"))
    return {cleaned, p.name, p.stem}


def extract_session_ids(text: str) -> tuple[str, ...]:
    """Return the cited session ids in *text*, de-duped, in first-seen order.

    A session id is a uuid appearing in a ``Session:``/``session-of-record``
    citation context (see :data:`SESSION_CITATION_RE`) — NOT every uuid in the
    document, so an incidental commit-tree hash or unrelated uuid is not
    mistaken for the artifact's reference of record.
    """
    seen: list[str] = []
    for match in SESSION_CITATION_RE.finditer(text):
        sid = match.group(1).lower()
        if sid not in seen:
            seen.append(sid)
    return tuple(seen)


def parse_artifact(path: str, source: str, content: str) -> Artifact:
    """Parse one markdown artifact into an :class:`Artifact` (pure).

    Extracts the cited session ids and the (possibly several) supersession
    refs declared in either direction. Direction convention:

      * ``Superseded by: X`` in artifact A  ⇒  X supersedes A.
      * ``Supersedes: X``    in artifact A  ⇒  A supersedes X.

    Code (fenced + inline) is stripped first so documentation examples — a
    convention ADR / template showing the format — are not parsed as real
    citations or relations.
    """
    content = strip_code(content)
    return Artifact(
        path=path,
        source=source,
        session_ids=extract_session_ids(content),
        superseded_by=tuple(
            _clean_ref(m.group(1)) for m in SUPERSEDED_BY_RE.finditer(content)
        ),
        supersedes=tuple(
            _clean_ref(m.group(1)) for m in SUPERSEDES_RE.finditer(content)
        ),
    )


def _index_artifacts(artifacts: Iterable[Artifact]) -> dict[str, Artifact]:
    """Index artifacts by every key a ref may name them with (pure).

    Keys: full path, basename, stem. Later artifacts win on a key collision
    (deterministic given a sorted input), which is acceptable — slugs are
    expected unique within a repo's decision corpus.
    """
    index: dict[str, Artifact] = {}
    for art in artifacts:
        for key in _ref_keys(art.path):
            index[key] = art
    return index


def _resolve_ref_sessions(ref: str, index: dict[str, Artifact]) -> tuple[str, ...]:
    """Resolve a relation ref to the session ids it points at (pure).

    Tries, in order: (1) the ref IS a session uuid → use it directly;
    (2) the ref names an indexed artifact → use that artifact's cited
    sessions. Returns ``()`` when neither resolves — the caller drops the
    relation rather than inventing a link.
    """
    cleaned = _clean_ref(ref)
    if UUID_RE.fullmatch(cleaned):
        return (cleaned.lower(),)
    for key in _ref_keys(ref):
        art = index.get(key)
        if art is not None:
            return art.session_ids
    return ()


def resolve_session_links(artifacts: Sequence[Artifact]) -> list[SessionLink]:
    """Resolve artifact relations to concrete SESSION-level links (pure).

    For each declared relation, resolves BOTH ends to their cited session ids
    and emits the cross-product of (superseding sessions × superseded
    sessions) — normally 1×1 when each artifact cites a single session of
    record. A relation whose either end has no resolvable session is DROPPED
    (we never fabricate a half-known link). Self-links (a session superseding
    itself) are dropped. Output is de-duped and sorted for determinism.
    """
    index = _index_artifacts(artifacts)
    links: set[SessionLink] = set()
    for art in artifacts:
        for ref in art.superseded_by:
            # ref supersedes art.
            _emit(links, _resolve_ref_sessions(ref, index), art.session_ids, art.source)
        for ref in art.supersedes:
            # art supersedes ref.
            _emit(links, art.session_ids, _resolve_ref_sessions(ref, index), art.source)
    return sorted(
        links, key=lambda link: (link.superseding_id, link.superseded_id, link.source)
    )


def _emit(
    sink: set[SessionLink],
    superseding: Sequence[str],
    superseded: Sequence[str],
    source: str,
) -> None:
    """Add the cross-product of (superseding × superseded) links to *sink*."""
    for sup in superseding:
        for sub in superseded:
            if sup != sub:
                sink.add(
                    SessionLink(superseding_id=sup, superseded_id=sub, source=source)
                )


_REVERT_SUBJECT_RE = re.compile(r"^Revert ", re.MULTILINE)
_REVERTS_COMMIT_RE = re.compile(r"(?i)This reverts commit ([0-9a-f]{7,40})")
# Per-commit record delimiter we ask git for (see :func:`read_git_log`).
_GIT_RECORD_SEP = "\x1e"
_GIT_FIELD_SEP = "\x1f"


def parse_git_reverts(git_log: str) -> list[RevertLink]:
    """Parse ``git log`` output into git-revert links (pure).

    Expects records separated by :data:`_GIT_RECORD_SEP`, each
    ``<sha>\\x1f<full message>``. A record whose message body names the
    reverted commit (``This reverts commit <sha>``) yields a link
    ``(reverting_sha supersedes reverted_sha)``. A ``Revert "..."`` subject
    with no parseable ``This reverts commit`` line is NOT emitted (we cannot
    determine WHAT it reverted — fail honest, not a guess). Sorted + de-duped.
    """
    links: set[RevertLink] = set()
    for record in git_log.split(_GIT_RECORD_SEP):
        record = record.strip()
        if not record or _GIT_FIELD_SEP not in record:
            continue
        sha, message = record.split(_GIT_FIELD_SEP, 1)
        sha = sha.strip()
        if not _REVERT_SUBJECT_RE.search(message):
            continue
        for reverted in _REVERTS_COMMIT_RE.findall(message):
            if sha and reverted and sha != reverted:
                links.add(RevertLink(superseding_id=sha, superseded_id=reverted))
    return sorted(links, key=lambda link: (link.superseding_id, link.superseded_id))


# --------------------------------------------------------------------------- #
# Imperative shell (IO boundary)
# --------------------------------------------------------------------------- #


def scan_artifacts(repo_path: Path) -> list[Artifact]:
    """Read + parse every decision/labbook markdown under *repo_path* (IO).

    Walks each configured ARTIFACT_DIR, parsing each ``*.md`` with the pure
    :func:`parse_artifact`. Returns artifacts sorted by path for deterministic
    downstream ordering. A missing dir is skipped (a consuming repo need not
    have both).
    """
    artifacts: list[Artifact] = []
    for rel_dir, source in ARTIFACT_DIRS:
        base = repo_path / rel_dir
        if not base.is_dir():
            continue
        for md in sorted(base.rglob("*.md")):
            if md.name.lower() == _TEMPLATE_NAME:
                continue  # a template is documentation, not a decision record
            rel = md.relative_to(repo_path).as_posix()
            artifacts.append(
                parse_artifact(rel, source, md.read_text(encoding="utf-8"))
            )
    return sorted(artifacts, key=lambda art: art.path)


def read_git_log(repo_path: Path) -> str:
    """Return ``git log`` of *repo_path* in the parse-friendly format (IO).

    Emits ``<sha>\\x1f<body>`` records separated by ``\\x1e`` so
    :func:`parse_git_reverts` can split unambiguously even with multi-line
    commit bodies. Fail-fast: a non-zero git exit raises.
    """
    fmt = f"%H{_GIT_FIELD_SEP}%B{_GIT_RECORD_SEP}"
    result = subprocess.run(
        ["git", "-C", str(repo_path), "log", f"--format={fmt}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def post_links(base_url: str, links: Sequence[dict[str, str]]) -> int:
    """POST a batch of links to ``/api/supersessions`` (IO). Returns inserted.

    Fail-fast: a non-2xx response or a malformed body raises. Returns the
    server-reported ``inserted`` count.
    """
    if not links:
        return 0
    url = base_url.rstrip("/") + "/api/supersessions"
    payload = json.dumps({"links": list(links)}).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req) as resp:  # noqa: S310 - operator-supplied URL
        body = json.loads(resp.read().decode("utf-8"))
    return int(body["inserted"])


def collect_links(repo_path: Path) -> tuple[list[SessionLink], list[RevertLink]]:
    """Scan *repo_path* and return (session-level links, git-revert links) (IO)."""
    artifacts = scan_artifacts(repo_path)
    session_links = resolve_session_links(artifacts)
    revert_links = parse_git_reverts(read_git_log(repo_path))
    return session_links, revert_links


def format_dry_run(
    session_links: Sequence[SessionLink], revert_links: Sequence[RevertLink]
) -> str:
    """Render the links a run WOULD create as deterministic text (pure)."""
    lines: list[str] = [f"SESSION-level supersession links ({len(session_links)}):"]
    lines.extend(
        f"  {link.superseding_id} supersedes {link.superseded_id}  [{link.source}]"
        for link in session_links
    )
    lines.append(
        f"git-revert links ({len(revert_links)}) "
        f"(inert: sha not session-mapped, will NOT affect ranking):"
    )
    lines.extend(
        f"  {link.superseding_id} supersedes {link.superseded_id}  [{link.source}]"
        for link in revert_links
    )
    return "\n".join(lines)


def run(
    repo_path: Path,
    base_url: str,
    *,
    dry_run: bool,
    include_reverts: bool,
) -> int:
    """Top-level orchestration; returns process exit code (IO at boundary)."""
    session_links, revert_links = collect_links(repo_path)
    if not include_reverts:
        revert_links = []
    print(format_dry_run(session_links, revert_links))
    if dry_run:
        return 0
    payload = [link.as_dict() for link in session_links]
    payload += [link.as_dict() for link in revert_links]
    inserted = post_links(base_url, payload)
    print(f"\nPOSTed {len(payload)} links -> {inserted} newly inserted.")
    return 0


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "repo_path", type=Path, help="Path to the consuming repo checkout."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8765",
        help="ingest-sessions server base URL (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the links that would be created; do not POST.",
    )
    parser.add_argument(
        "--include-reverts",
        action="store_true",
        help="Also POST git-revert links (inert for ranking; recorded for audit).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return run(
        args.repo_path,
        args.base_url,
        dry_run=args.dry_run,
        include_reverts=args.include_reverts,
    )


if __name__ == "__main__":
    raise SystemExit(main())
