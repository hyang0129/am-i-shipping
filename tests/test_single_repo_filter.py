"""Tests for the single-repo filter threaded through the LLM pipeline stages
(issue #88).

Covers the acceptance scenarios from the refined spec:

* Entry Point 1 — ``run_synthesis`` with ``repo`` writes a repo-scoped
  retrospective and leaves only targeted-repo units in the assembled prompt.
* Entry Point 2 — ``run_summarization`` with ``repo`` touches only the
  targeted repo's units.
* Entry Point 3 — ``run_extraction`` with ``repo`` writes only the targeted
  repo's expectations rows.
* Entry Point 5 — the output path is keyed on ``(week, repo)`` so single-repo
  and full-weekly runs coexist and neither blocks the other on the refuse-to-
  overwrite guard.
* Entry Point 6 — bare invocations (no ``repo``) produce the same rows / path
  as the pre-#88 baseline.

Session-rooted units are exercised separately via the
:func:`synthesis.weekly._load_units` helper — today's ``unit_identifier``
drops session-only components so end-to-end coverage via the CLIs is not
reachable, but the resolver SQL must still hold.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    init_expectations_db,
    init_github_db,
    init_sessions_db,
)
from synthesis.expectations import run_extraction
from synthesis.output_writer import write_retrospective
from synthesis.summarize import run_summarization
from synthesis.weekly import _load_units, _repo_filter_sql, run_synthesis


WEEK = "2026-04-21"
REPO_A = "hyang0129/am-i-shipping"
REPO_B = "hyang0129/other"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path) -> SynthesisConfig:
    return SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-6",
        summary_model="claude-haiku-4-5",
        output_dir=str(tmp_path / "retrospectives"),
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )


def _seed_unit(
    gh_db: Path,
    *,
    unit_id: str,
    root_node_type: str,
    root_node_id: str,
    week_start: str = WEEK,
) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, "
            " review_cycles, status, outlier_flags, abandonment_flag) "
            "VALUES (?, ?, ?, ?, 1.0, 0.0, 0, 0, 'closed', '[]', 0)",
            (week_start, unit_id, root_node_type, root_node_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_unit_summary(gh_db: Path, unit_id: str, *, week_start: str = WEEK) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO unit_summaries "
            "(week_start, unit_id, summary_text, model, input_bytes) "
            "VALUES (?, ?, ?, 'test', 0)",
            (week_start, unit_id, f"summary for {unit_id}"),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_session_attribution(
    gh_db: Path,
    *,
    session_uuid: str,
    repo: str,
    issue_number: int,
    week_start: str = WEEK,
) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO session_issue_attribution "
            "(week_start, session_uuid, repo, issue_number, fraction, phase) "
            "VALUES (?, ?, ?, ?, 1.0, 'execution')",
            (week_start, session_uuid, repo, issue_number),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def two_repo_fixture(tmp_path: Path) -> dict:
    """Seed two repos with one issue-rooted and one PR-rooted unit each."""
    gh_db = tmp_path / "github.db"
    sess_db = tmp_path / "sessions.db"
    exp_db = tmp_path / "expectations.db"
    init_github_db(gh_db)
    init_sessions_db(sess_db)
    init_expectations_db(exp_db)

    # Repo A — two units
    _seed_unit(
        gh_db,
        unit_id="u-a-issue",
        root_node_type="issue",
        root_node_id=f"issue:{REPO_A}#1",
    )
    _seed_unit(
        gh_db,
        unit_id="u-a-pr",
        root_node_type="pr",
        root_node_id=f"pr:{REPO_A}#2",
    )
    # Repo B — one unit, one PR-rooted
    _seed_unit(
        gh_db,
        unit_id="u-b-issue",
        root_node_type="issue",
        root_node_id=f"issue:{REPO_B}#3",
    )
    _seed_unit(
        gh_db,
        unit_id="u-b-pr",
        root_node_type="pr",
        root_node_id=f"pr:{REPO_B}#4",
    )

    # Look-alike slug to guard against prefix-bleed via the trailing ``#%``.
    _seed_unit(
        gh_db,
        unit_id="u-lookalike",
        root_node_type="issue",
        root_node_id=f"issue:{REPO_A}-sibling#99",
    )

    return {
        "gh_db": gh_db,
        "sess_db": sess_db,
        "exp_db": exp_db,
        "tmp_path": tmp_path,
    }


# ---------------------------------------------------------------------------
# Helper unit tests — the SQL builder
# ---------------------------------------------------------------------------


def test_repo_filter_sql_is_noop_without_repo() -> None:
    fragment, params = _repo_filter_sql(None)
    assert fragment == ""
    assert params == []


def test_repo_filter_sql_pins_slug_boundary() -> None:
    _, params = _repo_filter_sql(REPO_A, WEEK)
    # Param patterns must end in ``#%`` so a sibling repo slug cannot match.
    like_params = [p for p in params if p.startswith(("issue:", "pr:"))]
    assert len(like_params) == 2
    assert all(p.endswith("#%") for p in like_params)


def test_repo_filter_sql_requires_week_start_when_repo_set() -> None:
    """F-1 cycle 1: helper must fail loudly if caller forgets week_start.

    The old two-function API used a sentinel string that silently
    surfaced as a bad bind if the caller skipped ``_repo_filter_bind``.
    The collapsed API makes week_start a required positional whenever
    repo is truthy.
    """
    with pytest.raises(ValueError, match="week_start is required"):
        _repo_filter_sql(REPO_A)


def test_repo_filter_sql_escapes_like_metachars() -> None:
    """F-3 cycle 1: underscore / percent in a repo name must not leak as
    LIKE wildcards. A repo called ``owner/my_repo`` must not match a
    unit id ``issue:owner/myXrepo#1``."""
    from synthesis.weekly import _repo_filter_sql as _filter

    _, params = _filter("owner/my_repo", WEEK)
    like_params = [p for p in params if p.startswith(("issue:", "pr:"))]
    # The ``_`` was escaped to ``\_`` (so the LIKE treats it literally).
    assert all("\\_" in p for p in like_params)

    # A literal ``%`` in the slug is also escaped.
    _, pct_params = _filter("owner/50%repo", WEEK)
    pct_like = [p for p in pct_params if p.startswith(("issue:", "pr:"))]
    assert all("\\%" in p for p in pct_like)


def test_load_units_filters_by_repo(two_repo_fixture: dict) -> None:
    gh_conn = sqlite3.connect(str(two_repo_fixture["gh_db"]))
    try:
        all_units = _load_units(gh_conn, WEEK)
        filtered = _load_units(gh_conn, WEEK, repo=REPO_A)
    finally:
        gh_conn.close()

    all_ids = {u["unit_id"] for u in all_units}
    filtered_ids = {u["unit_id"] for u in filtered}
    # Every seed unit is visible without the flag.
    assert {"u-a-issue", "u-a-pr", "u-b-issue", "u-b-pr", "u-lookalike"} <= all_ids
    # The filter keeps only REPO_A's issue + PR and drops the look-alike.
    assert filtered_ids == {"u-a-issue", "u-a-pr"}


def test_load_units_filter_uses_session_attribution(two_repo_fixture: dict) -> None:
    """Session-rooted unit reachable only via session_issue_attribution."""
    gh_db = two_repo_fixture["gh_db"]
    # Seed a session-rooted unit and an attribution row pointing at REPO_A.
    _seed_unit(
        gh_db,
        unit_id="u-a-session",
        root_node_type="session",
        root_node_id="session:abc-123",
    )
    _seed_session_attribution(gh_db, session_uuid="abc-123", repo=REPO_A, issue_number=1)

    gh_conn = sqlite3.connect(str(gh_db))
    try:
        filtered = _load_units(gh_conn, WEEK, repo=REPO_A)
    finally:
        gh_conn.close()

    assert "u-a-session" in {u["unit_id"] for u in filtered}


# ---------------------------------------------------------------------------
# Entry Point 2 — am-summarize-units (via run_summarization)
# ---------------------------------------------------------------------------


def test_summarize_respects_repo_filter(two_repo_fixture: dict) -> None:
    cfg = _make_config(two_repo_fixture["tmp_path"])

    rc = run_summarization(
        cfg,
        github_db=str(two_repo_fixture["gh_db"]),
        sessions_db=str(two_repo_fixture["sess_db"]),
        week_start=WEEK,
        repo=REPO_A,
    )
    assert rc == 0

    # Only REPO_A's units have summaries.
    conn = sqlite3.connect(str(two_repo_fixture["gh_db"]))
    try:
        rows = conn.execute(
            "SELECT unit_id FROM unit_summaries WHERE week_start = ?",
            (WEEK,),
        ).fetchall()
    finally:
        conn.close()
    ids = {r[0] for r in rows}
    assert ids == {"u-a-issue", "u-a-pr"}


# ---------------------------------------------------------------------------
# Entry Point 3 — am-extract-expectations (via run_extraction)
# ---------------------------------------------------------------------------


def test_extract_expectations_respects_repo_filter(two_repo_fixture: dict) -> None:
    cfg = _make_config(two_repo_fixture["tmp_path"])
    rc = run_extraction(
        cfg,
        github_db=str(two_repo_fixture["gh_db"]),
        sessions_db=str(two_repo_fixture["sess_db"]),
        expectations_db=str(two_repo_fixture["exp_db"]),
        week_start=WEEK,
        repo=REPO_A,
    )
    # Even on skip rows the run should return 0 (no fatal failures).
    assert rc == 0

    conn = sqlite3.connect(str(two_repo_fixture["exp_db"]))
    try:
        rows = conn.execute(
            "SELECT DISTINCT unit_id FROM expectations WHERE week_start = ?",
            (WEEK,),
        ).fetchall()
    finally:
        conn.close()
    ids = {r[0] for r in rows}
    # Only REPO_A units have expectation rows written.
    assert ids == {"u-a-issue", "u-a-pr"}


# ---------------------------------------------------------------------------
# Entry Point 5 — write_retrospective repo-scoped path
# ---------------------------------------------------------------------------


def test_write_retrospective_repo_scoped_path(tmp_path: Path) -> None:
    out = tmp_path / "retrospectives"
    # Seed a pre-existing bare-week file.
    out.mkdir()
    (out / f"{WEEK}.md").write_text("pre-existing full-week", encoding="utf-8")

    repo_path = write_retrospective("single repo", out, WEEK, repo=REPO_A)
    assert repo_path is not None
    expected = out / WEEK / "hyang0129__am-i-shipping.md"
    assert repo_path == expected
    assert expected.read_text(encoding="utf-8") == "single repo"
    # Full-week file untouched.
    assert (out / f"{WEEK}.md").read_text(encoding="utf-8") == "pre-existing full-week"


def test_write_retrospective_bare_week_still_writes_after_repo(tmp_path: Path) -> None:
    """Write the repo-scoped file first, then a bare-week run must not be blocked."""
    out = tmp_path / "retrospectives"
    repo_path = write_retrospective("single repo", out, WEEK, repo=REPO_A)
    assert repo_path is not None

    bare_path = write_retrospective("full week", out, WEEK)
    assert bare_path is not None
    assert bare_path == out / f"{WEEK}.md"
    assert bare_path.read_text(encoding="utf-8") == "full week"


def test_write_retrospective_refuse_repeats_repo_path(tmp_path: Path) -> None:
    """Two single-repo runs for the same (week, repo) — second is a no-op."""
    out = tmp_path / "retrospectives"
    first = write_retrospective("first", out, WEEK, repo=REPO_A)
    assert first is not None
    second = write_retrospective("second — should not overwrite", out, WEEK, repo=REPO_A)
    assert second is None
    assert first.read_text(encoding="utf-8") == "first"


# ---------------------------------------------------------------------------
# Entry Point 1 — run_synthesis writes to the repo-scoped path
# ---------------------------------------------------------------------------


def test_run_synthesis_writes_repo_scoped_retrospective(
    two_repo_fixture: dict,
) -> None:
    cfg = _make_config(two_repo_fixture["tmp_path"])
    # Seed unit_summaries for every REPO_A unit so run_synthesis does not
    # raise the missing-summary guard. Non-targeted units can remain
    # unsummarised; the filter means run_synthesis never touches them.
    _seed_unit_summary(two_repo_fixture["gh_db"], "u-a-issue")
    _seed_unit_summary(two_repo_fixture["gh_db"], "u-a-pr")

    result = run_synthesis(
        cfg,
        two_repo_fixture["gh_db"],
        two_repo_fixture["sess_db"],
        WEEK,
        expectations_db=two_repo_fixture["exp_db"],
        repo=REPO_A,
    )
    assert result is not None
    # Path is the repo-scoped form.
    assert result.parent.name == WEEK
    assert result.name == "hyang0129__am-i-shipping.md"


# ---------------------------------------------------------------------------
# Entry Point 6 — no-flag byte-identity (path shape)
# ---------------------------------------------------------------------------


def test_run_synthesis_no_flag_writes_bare_week_path(
    two_repo_fixture: dict,
) -> None:
    cfg = _make_config(two_repo_fixture["tmp_path"])
    # Seed every unit's summary so the bare-week path succeeds without a
    # missing-summary raise.
    for uid in ("u-a-issue", "u-a-pr", "u-b-issue", "u-b-pr", "u-lookalike"):
        _seed_unit_summary(two_repo_fixture["gh_db"], uid)

    result = run_synthesis(
        cfg,
        two_repo_fixture["gh_db"],
        two_repo_fixture["sess_db"],
        WEEK,
        expectations_db=two_repo_fixture["exp_db"],
    )
    assert result is not None
    # Flat path (no week directory) — unchanged from pre-#88 behavior.
    assert result.name == f"{WEEK}.md"
    assert result.parent.name == "retrospectives"
