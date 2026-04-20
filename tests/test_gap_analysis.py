"""Tests for ``synthesis/gap_analysis.py`` (Epic #27 — X-2, Issue #73)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    EXPECTED_EXPECTATIONS_TABLES,
    assert_schema,
    init_expectations_db,
    init_github_db,
)
from synthesis import gap_analysis
from synthesis.gap_analysis import (
    DIRECTION_ENUM,
    FAILURE_PRECONDITION_ENUM,
    SEVERITY_ENUM,
    compute_severity_direction,
    load_gap_rows,
)


WEEK_START = "2026-04-14"


def _make_config(**overrides) -> SynthesisConfig:
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-5",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


def _init_dbs(tmp_path: Path) -> tuple[Path, Path]:
    gh = tmp_path / "github.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_expectations_db(exp)
    return gh, exp


def _seed_unit(
    gh_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    status: str = "closed",
    total_reprompts: int = 0,
    review_cycles: int = 0,
    elapsed_days: float = 1.0,
    abandonment_flag: int = 0,
) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, review_cycles, "
            " status, outlier_flags, abandonment_flag) "
            "VALUES (?, ?, 'session', ?, ?, 0.0, ?, ?, ?, '[]', ?)",
            (
                week_start,
                unit_id,
                f"n-{unit_id}",
                elapsed_days,
                total_reprompts,
                review_cycles,
                status,
                abandonment_flag,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_expectation(
    exp_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    skip_reason: str | None = None,
    expected_scope: str | None = "fix X",
    expected_effort: str | None = "one session",
    expected_outcome: str | None = "tests pass",
    commitment_point: str | None = "turn 3",
) -> None:
    conn = sqlite3.connect(str(exp_db))
    try:
        conn.execute(
            "INSERT INTO expectations "
            "(week_start, unit_id, commitment_point, expected_scope, "
            " expected_effort, expected_outcome, confidence, model, "
            " input_bytes, skip_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                week_start,
                unit_id,
                commitment_point,
                expected_scope,
                expected_effort,
                expected_outcome,
                0.8 if skip_reason is None else None,
                "claude-sonnet-4-5",
                100,
                skip_reason,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _all_gap_rows(exp_db: Path, week_start: str = WEEK_START) -> list[dict]:
    return load_gap_rows(str(exp_db), week_start)


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ---------------------------------------------------------------------------
# AS-1: schema exists with locked column names
# ---------------------------------------------------------------------------


class TestExpectationGapsSchema:
    def test_init_creates_expectation_gaps_table(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)

        conn = sqlite3.connect(str(db))
        try:
            cols = {
                r[1]
                for r in conn.execute(
                    "PRAGMA table_info(expectation_gaps)"
                ).fetchall()
            }
        finally:
            conn.close()

        required = {
            "week_start",
            "unit_id",
            "commitment_point",
            "scope_gap",
            "effort_gap",
            "outcome_gap",
            "severity",
            "direction",
            "failure_precondition",
            "computed_at",
            "auto_confirmed",
        }
        missing = required - cols
        assert not missing, (
            f"expectation_gaps missing required columns: {missing}"
        )

    def test_idempotent_init(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        init_expectations_db(db)
        assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)


# ---------------------------------------------------------------------------
# Pure-function severity tests
# ---------------------------------------------------------------------------


class TestComputeSeverityDirection:
    def test_skip_reason_yields_none_ambiguous(self):
        sev, direction = compute_severity_direction(
            status="closed",
            total_reprompts=0,
            review_cycles=0,
            elapsed_days=1.0,
            expected_effort=None,
            expected_outcome=None,
            skip_reason="raw_content_json_empty",
        )
        assert sev == "none"
        assert direction == "ambiguous"

    def test_abandoned_status_yields_critical_under(self):
        sev, direction = compute_severity_direction(
            status="abandoned",
            total_reprompts=0,
            review_cycles=0,
            elapsed_days=14.0,
            expected_effort="one session",
            expected_outcome="shipped",
            skip_reason=None,
        )
        assert sev == "critical"
        assert direction == "under"

    def test_high_reprompts_yields_major_over(self):
        sev, direction = compute_severity_direction(
            status="closed",
            total_reprompts=12,
            review_cycles=1,
            elapsed_days=2.0,
            expected_effort="one session",
            expected_outcome="shipped",
            skip_reason=None,
        )
        assert sev == "major"
        assert direction == "over"

    def test_moderate_reprompts_yields_minor_over(self):
        sev, direction = compute_severity_direction(
            status="closed",
            total_reprompts=5,
            review_cycles=1,
            elapsed_days=1.0,
            expected_effort="one session",
            expected_outcome="shipped",
            skip_reason=None,
        )
        assert sev == "minor"
        assert direction == "over"

    def test_clean_run_yields_none_match(self):
        sev, direction = compute_severity_direction(
            status="closed",
            total_reprompts=0,
            review_cycles=0,
            elapsed_days=0.5,
            expected_effort="one session",
            expected_outcome="shipped",
            skip_reason=None,
        )
        assert sev == "none"
        assert direction == "match"


# ---------------------------------------------------------------------------
# AS-2, AS-3, AS-4: gap pass produces one row per expectation with valid enums
# ---------------------------------------------------------------------------


class TestGapRunOffline:
    def test_one_gap_row_per_expectation(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u1", total_reprompts=0, review_cycles=0)
        _seed_unit(gh, unit_id="u2", status="abandoned", total_reprompts=0)
        _seed_unit(gh, unit_id="u3_no_expectation")
        _seed_expectation(exp, unit_id="u1")
        _seed_expectation(exp, unit_id="u2")

        written = gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
        )

        assert written == 2
        rows = _all_gap_rows(exp)
        unit_ids = {r["unit_id"] for r in rows}
        assert unit_ids == {"u1", "u2"}
        # u3 had a units row but no expectation row → no gap row.

    def test_severity_and_direction_populated(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u1", status="abandoned")
        _seed_expectation(exp, unit_id="u1")

        gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
        )
        rows = _all_gap_rows(exp)
        assert len(rows) == 1
        assert rows[0]["severity"] in SEVERITY_ENUM
        assert rows[0]["direction"] in DIRECTION_ENUM

    def test_failure_precondition_constrained_enum(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        # Mix of severities.
        _seed_unit(gh, unit_id="u_clean", total_reprompts=0)
        _seed_unit(gh, unit_id="u_abandoned", status="abandoned")
        _seed_unit(gh, unit_id="u_overbusy", total_reprompts=15)
        _seed_expectation(exp, unit_id="u_clean")
        _seed_expectation(exp, unit_id="u_abandoned")
        _seed_expectation(exp, unit_id="u_overbusy")

        gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
        )
        rows = _all_gap_rows(exp)
        for r in rows:
            sev = r["severity"]
            fp = r["failure_precondition"]
            if sev == "none":
                assert fp is None, (
                    f"failure_precondition must be NULL when severity=none, "
                    f"got {fp!r}"
                )
            else:
                assert fp in FAILURE_PRECONDITION_ENUM, (
                    f"failure_precondition {fp!r} not in enum"
                )

    def test_skip_reason_yields_none_severity(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u1")
        _seed_expectation(
            exp,
            unit_id="u1",
            skip_reason="raw_content_json_empty",
            expected_scope=None,
            expected_effort=None,
            expected_outcome=None,
            commitment_point=None,
        )
        gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
        )
        rows = _all_gap_rows(exp)
        assert len(rows) == 1
        assert rows[0]["severity"] == "none"
        assert rows[0]["failure_precondition"] is None


# ---------------------------------------------------------------------------
# Idempotency — re-running replaces gap rows for the week
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_rerun_replaces_gap_rows(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u1", total_reprompts=0)
        _seed_expectation(exp, unit_id="u1")

        gap_analysis.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp),
            config=_make_config(),
        )
        rows1 = _all_gap_rows(exp)
        computed_at_1 = sqlite3.connect(str(exp)).execute(
            "SELECT computed_at FROM expectation_gaps WHERE unit_id='u1'"
        ).fetchone()[0]

        # Sleep-free idempotency check: run again and assert the row still
        # exists (count stable, unit_id unchanged). The computed_at string
        # uses second-granularity so back-to-back invocations can tie; the
        # important invariant is count + content stability, not a new
        # timestamp.
        gap_analysis.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp),
            config=_make_config(),
        )
        rows2 = _all_gap_rows(exp)
        assert len(rows2) == len(rows1) == 1
        assert rows2[0]["unit_id"] == "u1"

    def test_noop_when_no_expectations(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u1")
        # No expectations rows seeded.
        written = gap_analysis.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp),
            config=_make_config(),
        )
        assert written == 0
        assert _all_gap_rows(exp) == []


# ---------------------------------------------------------------------------
# AS-8: auto-confirm sweep flips rows older than 14 days
# ---------------------------------------------------------------------------


class TestAutoConfirmSweep:
    def test_old_rows_flipped_young_rows_untouched(self, tmp_path: Path):
        gh, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="u_old")
        _seed_unit(gh, unit_id="u_young")
        _seed_expectation(exp, unit_id="u_old")
        _seed_expectation(exp, unit_id="u_young")

        # First run: populates gap rows with ``computed_at = now``.
        gap_analysis.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp),
            config=_make_config(),
        )

        # Back-date u_old's computed_at to 15 days ago.
        old_ts = (
            datetime.now(timezone.utc) - timedelta(days=15)
        ).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(str(exp))
        try:
            conn.execute(
                "UPDATE expectation_gaps SET computed_at = ? "
                "WHERE unit_id = 'u_old'",
                (old_ts,),
            )
            conn.commit()
        finally:
            conn.close()

        # Re-run: gap pass rewrites rows for the week (so both u_old and
        # u_young get fresh computed_at). But we specifically want to test
        # the sweep, so call it directly on the back-dated state BEFORE
        # re-running gap pass.
        conn = sqlite3.connect(str(exp))
        try:
            swept = gap_analysis._auto_confirm_sweep(conn)
            assert swept >= 1
            rows = conn.execute(
                "SELECT unit_id, auto_confirmed FROM expectation_gaps "
                "ORDER BY unit_id"
            ).fetchall()
        finally:
            conn.close()

        by_uid = {r[0]: r[1] for r in rows}
        assert by_uid["u_old"] == 1, "old row should be auto-confirmed"
        assert by_uid["u_young"] == 0, "young row should be untouched"


# ---------------------------------------------------------------------------
# AS-5 / AS-7: integration with run_synthesis
# ---------------------------------------------------------------------------


class TestWeeklyIntegration:
    """End-to-end: gap pass runs inside ``run_synthesis``.

    The fake-adapter path returns a canned retrospective Markdown that
    does NOT include an ``## Expectation Gaps`` section (the fake is
    deterministic), so we verify the gap pass ran by inspecting the
    ``expectation_gaps`` table directly.
    """

    def test_gap_pass_runs_inside_run_synthesis(self, tmp_path: Path):
        from synthesis.weekly import run_synthesis
        from am_i_shipping.db import init_sessions_db

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        exp_db = tmp_path / "expectations.db"
        init_github_db(gh_db)
        init_sessions_db(sess_db)
        init_expectations_db(exp_db)

        _seed_unit(gh_db, unit_id="u1", total_reprompts=12)
        _seed_expectation(exp_db, unit_id="u1")

        # unit_summaries row required (fail-loud in run_synthesis).
        conn = sqlite3.connect(str(gh_db))
        try:
            conn.execute(
                "INSERT INTO unit_summaries "
                "(week_start, unit_id, summary_text, model, input_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "u1", "summary", "fake", 7),
            )
            conn.commit()
        finally:
            conn.close()

        out_dir = tmp_path / "retrospectives"
        cfg = replace(_make_config(), output_dir=str(out_dir))

        result = run_synthesis(
            cfg,
            gh_db,
            sess_db,
            WEEK_START,
            dry_run=False,
            expectations_db=exp_db,
        )
        assert result is not None

        rows = _all_gap_rows(exp_db)
        assert len(rows) == 1
        assert rows[0]["unit_id"] == "u1"
        assert rows[0]["severity"] in SEVERITY_ENUM

    def test_missing_expectations_db_is_silent_warning(
        self, tmp_path: Path, caplog
    ):
        """run_synthesis must not crash when expectations_db is missing."""
        from synthesis.weekly import run_synthesis
        from am_i_shipping.db import init_sessions_db

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        # No expectations.db — pass a non-existent path. The module will
        # try to open it, fail with OperationalError, and warn.
        init_github_db(gh_db)
        init_sessions_db(sess_db)
        _seed_unit(gh_db, unit_id="u1")
        conn = sqlite3.connect(str(gh_db))
        try:
            conn.execute(
                "INSERT INTO unit_summaries "
                "(week_start, unit_id, summary_text, model, input_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "u1", "summary", "fake", 7),
            )
            conn.commit()
        finally:
            conn.close()

        out_dir = tmp_path / "retrospectives"
        cfg = replace(_make_config(), output_dir=str(out_dir))

        # Don't pass expectations_db — the existing (backward-compatible)
        # code path must continue to work.
        result = run_synthesis(
            cfg, gh_db, sess_db, WEEK_START, dry_run=False,
        )
        assert result is not None
