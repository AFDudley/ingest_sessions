"""Unit tests for the consumer-side supersession adapter (pebble is-565.6).

The adapter (``scripts/supersession_adapter.py``) is a project-AGNOSTIC
markdown/git scanner run against a CONSUMING repo. These tests exercise its
PURE parse pipeline — ``parse_artifact`` → ``resolve_session_links`` and
``parse_git_reverts`` — directly, with no HTTP and (for the artifact path) a
tmp dir of fake ADR/labbook markdown. The POST is a thin shell and is NOT
exercised here (no live server in unit tests).
"""

from __future__ import annotations

from pathlib import Path

import supersession_adapter as adapter

# Two real-shaped session ids (uuids), each the "reference of record" cited by
# one ADR.
SESS_OLD = "11111111-1111-4111-8111-111111111111"
SESS_NEW = "22222222-2222-4222-8222-222222222222"
SESS_LAB = "33333333-3333-4333-8333-333333333333"


# --------------------------------------------------------------------------- #
# pure parse: a single artifact
# --------------------------------------------------------------------------- #


def test_parse_artifact_extracts_session_and_superseded_by() -> None:
    content = (
        "# ADR: use inotify\n\n"
        f"Session: {SESS_NEW}\n"
        "Superseded by: 2026-02-02-something-else.md\n"
    )
    art = adapter.parse_artifact("docs/decisions/adr.md", "adr-supersedes", content)
    assert art.session_ids == (SESS_NEW,)
    assert art.superseded_by == ("2026-02-02-something-else.md",)
    assert art.supersedes == ()


def test_extract_session_ids_ignores_non_session_uuids() -> None:
    # A uuid NOT in a session-citation context is not treated as a session id.
    content = (
        f"Random tree hash deadbeef-0000-4000-8000-000000000000\nSession: {SESS_OLD}"
    )
    assert adapter.extract_session_ids(content) == (SESS_OLD,)


# --------------------------------------------------------------------------- #
# pure parse: resolve a relation between two artifacts to a SESSION-level link
# --------------------------------------------------------------------------- #


def _write(base: Path, rel: str, body: str) -> None:
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_adapter_resolves_superseded_by_to_session_link(tmp_path: Path) -> None:
    # Old ADR cites SESS_OLD and says it is superseded by the new ADR (by slug).
    _write(
        tmp_path,
        "docs/decisions/2026-01-01-old.md",
        f"# Old decision\n\nSession: {SESS_OLD}\nSuperseded by: 2026-02-02-new.md\n",
    )
    # New ADR cites SESS_NEW.
    _write(
        tmp_path,
        "docs/decisions/2026-02-02-new.md",
        f"# New decision\n\nSession: {SESS_NEW}\n",
    )

    artifacts = adapter.scan_artifacts(tmp_path)
    links = adapter.resolve_session_links(artifacts)

    assert len(links) == 1
    link = links[0]
    assert link.superseding_id == SESS_NEW
    assert link.superseded_id == SESS_OLD
    assert link.source == "adr-supersedes"


def test_adapter_resolves_supersedes_direction_and_labbook_source(
    tmp_path: Path,
) -> None:
    # A labbook entry citing SESS_LAB declares it SUPERSEDES the old ADR.
    _write(
        tmp_path,
        "docs/labbook/2026-03-03-entry.md",
        f"# Labbook\n\nSession: {SESS_LAB}\nSupersedes: 2026-01-01-old.md\n",
    )
    _write(
        tmp_path,
        "docs/decisions/2026-01-01-old.md",
        f"# Old\n\nSession: {SESS_OLD}\n",
    )

    links = adapter.resolve_session_links(adapter.scan_artifacts(tmp_path))
    assert len(links) == 1
    assert links[0].superseding_id == SESS_LAB
    assert links[0].superseded_id == SESS_OLD
    assert links[0].source == "labbook-supersedes"


def test_adapter_drops_relation_with_unresolvable_target(tmp_path: Path) -> None:
    # Superseded-by points at an artifact that does not exist / cites no session
    # → no link fabricated.
    _write(
        tmp_path,
        "docs/decisions/2026-01-01-old.md",
        f"# Old\n\nSession: {SESS_OLD}\nSuperseded by: nonexistent.md\n",
    )
    links = adapter.resolve_session_links(adapter.scan_artifacts(tmp_path))
    assert links == []


def test_adapter_resolves_direct_uuid_ref(tmp_path: Path) -> None:
    # A relation ref that IS a session uuid resolves directly (no artifact
    # lookup needed for that end).
    _write(
        tmp_path,
        "docs/decisions/2026-02-02-new.md",
        f"# New\n\nSession: {SESS_NEW}\nSupersedes: {SESS_OLD}\n",
    )
    links = adapter.resolve_session_links(adapter.scan_artifacts(tmp_path))
    assert len(links) == 1
    assert links[0].superseding_id == SESS_NEW
    assert links[0].superseded_id == SESS_OLD


# --------------------------------------------------------------------------- #
# pure parse: git reverts
# --------------------------------------------------------------------------- #


def _git_record(sha: str, message: str) -> str:
    return f"{sha}{adapter._GIT_FIELD_SEP}{message}{adapter._GIT_RECORD_SEP}"


def test_parse_git_reverts_links_reverting_to_reverted() -> None:
    log = _git_record(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        'Revert "add polling"\n\nThis reverts commit '
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.\n",
    ) + _git_record("cccccccccccccccccccccccccccccccccccccccc", "normal commit\n")
    links = adapter.parse_git_reverts(log)
    assert len(links) == 1
    assert links[0].superseding_id == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert links[0].superseded_id == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert links[0].source == "git-revert"


def test_parse_git_reverts_skips_revert_without_reverts_line() -> None:
    # A "Revert" subject with no parseable "This reverts commit" body → not
    # emitted (we do not guess what it reverted).
    log = _git_record(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        'Revert "something" with no machine-readable reverts line\n',
    )
    assert adapter.parse_git_reverts(log) == []


# --------------------------------------------------------------------------- #
# dry-run rendering is deterministic and honest about git-revert inertness
# --------------------------------------------------------------------------- #


def test_format_dry_run_labels_reverts_inert() -> None:
    session_links = [
        adapter.SessionLink(SESS_NEW, SESS_OLD, "adr-supersedes"),
    ]
    revert_links = [
        adapter.RevertLink(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
    ]
    out = adapter.format_dry_run(session_links, revert_links)
    assert "SESSION-level supersession links (1)" in out
    assert f"{SESS_NEW} supersedes {SESS_OLD}" in out
    assert "inert: sha not session-mapped" in out


# --------------------------------------------------------------------------- #
# is-565.6b: documentation (code) must NOT be parsed as data
# --------------------------------------------------------------------------- #


def test_fenced_example_is_not_parsed_as_relation_or_citation() -> None:
    """A convention ADR that SHOWS the format in a code fence is not data.

    Regression for the bug where the session-of-record ADR's worked example
    (real slugs in a ``` block) emitted false supersession links.
    """
    content = (
        "# How to cite sessions\n\n"
        f"Session: {SESS_NEW}\n\n"  # the ADR's OWN real citation (plain) — kept
        "Show the format:\n\n"
        "```\n"
        "Supersedes: 2026-01-01-some-other-adr\n"
        f"Session: {SESS_OLD}\n"
        "```\n\n"
        "Inline `Superseded by: 2026-01-01-yet-another` is also an example.\n"
    )
    art = adapter.parse_artifact("docs/decisions/howto.md", "adr-supersedes", content)
    # Only the plain, out-of-code citation survives; the fenced/inline ones don't.
    assert art.session_ids == (SESS_NEW,)
    assert art.supersedes == ()
    assert art.superseded_by == ()


def test_strip_code_removes_fenced_and_inline() -> None:
    text = "a `inline` b\n```\nfenced Session: x\n```\nc"
    out = adapter.strip_code(text)
    assert "inline" not in out
    assert "fenced" not in out
    assert "a " in out and " b" in out and "c" in out


def test_scan_skips_template_files(tmp_path: Path) -> None:
    dec = tmp_path / "docs" / "decisions"
    dec.mkdir(parents=True)
    (dec / "TEMPLATE.md").write_text(
        f"Session: {SESS_OLD}\nSupersedes: 2026-01-01-x\n", encoding="utf-8"
    )
    (dec / "2026-02-02-real.md").write_text(f"Session: {SESS_NEW}\n", encoding="utf-8")
    arts = adapter.scan_artifacts(tmp_path)
    paths = {a.path for a in arts}
    assert "docs/decisions/2026-02-02-real.md" in paths
    assert "docs/decisions/TEMPLATE.md" not in paths
