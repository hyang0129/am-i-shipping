"""Tests for ``synthesis/calibration.py`` (Epic #27 — X-5, Issue #76)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import pytest

from am_i_shipping.db import (
    EXPECTED_EXPECTATIONS_TABLES,
    assert_schema,
    init_expectations_db,
    init_github_db,
)
from synthesis import calibration
from synthesis.calibration import (
    CALIBRATION_MIN_CORRECTIONS,
    FEW_SHOT_BLOCK_MARKER,
    FEW_SHOT_SAMPLE_SIZE,
    build_few_shot_block,
    load_trends,
    render_calibration_block,
)


WEEK_START = "2026-04-14"


def _init_dbs(tmp_path: Path) -> tuple[Path, Path]:
    gh = tmp_path / "github.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_expectations_db(exp)
    return gh, exp


def _seed_issue(
    gh_db: Path, *, repo: str, issue_number: int, type_label: Optional[str]
) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO issues "
            "(repo, issue_number, title, type_label, state) "
            "VALUES (?, ?, ?, ?, ?)",
            (repo, issue_number, f"issue #{issue_number}", type_label, "open"),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_unit(
    gh_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    root_node_type: str = "issue",
    root_node_id: Optional[str] = None,
) -> None:
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT OR REPLACE INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, review_cycles, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                week_start,
                unit_id,
                root_node_type,
                root_node_id or unit_id,
                1.0,
                0.0,
                0,
                0,
                "complete",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_correction(
    exp_db: Path,
    *,
    unit_id: str,
    facet: str,
    original_value: Optional[str],
    corrected_value: Optional[str],
    corrected_by: str = "user",
    corrected_at: Optional[str] = None,
    week_start: str = WEEK_START,
) -> None:
    conn = sqlite3.connect(str(exp_db))
    try:
        if corrected_at is None:
            conn.execute(
                "INSERT OR REPLACE INTO expectation_corrections "
                "(week_start, unit_id, facet, original_value, corrected_value, "
                " correction_note, corrected_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    week_start,
                    unit_id,
                    facet,
                    original_value,
                    corrected_value,
                    None,
                    corrected_by,
                ),
            )
        else:
            conn.execute(
                "INSERT OR REPLACE INTO expectation_corrections "
                "(week_start, unit_id, facet, original_value, corrected_value, "
                " correction_note, corrected_by, corrected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    week_start,
                    unit_id,
                    facet,
                    original_value,
                    corrected_value,
                    None,
                    corrected_by,
                    corrected_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_n_user_corrections(
    exp_db: Path,
    gh_db: Path,
    *,
    n: int,
    type_label: str = "bug",
    facet_cycle: Iterable[str] = ("scope", "effort", "outcome"),
    starting_at: Optional[datetime] = None,
) -> None:
    """Helper: seed *n* user corrections backed by issue-rooted units."""
    facets = list(facet_cycle)
    clock = starting_at or datetime(2026, 4, 10, 10, 0, 0, tzinfo=timezone.utc)
    for i in range(n):
        issue_number = 1000 + i
        repo = "acme/svc"
        root = f"issue:{repo}#{issue_number}"
        _seed_issue(gh_db, repo=repo, issue_number=issue_number, type_label=type_label)
        _seed_unit(
            gh_db,
            unit_id=root,
            root_node_type="issue",
            root_node_id=root,
        )
        facet = facets[i % len(facets)]
        ts = (clock + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        _seed_correction(
            exp_db,
            unit_id=root,
            facet=facet,
            original_value=f"old-{i}",
            corrected_value=f"new-{i}" if i % 2 == 0 else f"old-{i}",
            corrected_by="user",
            corrected_at=ts,
        )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestCalibrationTrendsSchema:
    """AS-7: expectation_calibration_trends is registered and created."""

    def test_table_created(self, tmp_path):
        exp = tmp_path / "expectations.db"
        init_expectations_db(exp)
        conn = sqlite3.connect(str(exp))
        try:
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        finally:
            conn.close()
        assert "expectation_calibration_trends" in tables

    def test_table_has_expected_columns(self, tmp_path):
        exp = tmp_path / "expectations.db"
        init_expectations_db(exp)
        conn = sqlite3.connect(str(exp))
        try:
            columns = {
                r[1]
                for r in conn.execute(
                    "PRAGMA table_info(expectation_calibration_trends)"
                ).fetchall()
            }
        finally:
            conn.close()
        expected = {
            "work_type",
            "week_start",
            "avg_scope_delta",
            "avg_effort_delta",
            "avg_outcome_delta",
            "sample_count",
            "computed_at",
        }
        assert expected.issubset(columns)

    def test_registered_in_expected_tables(self, tmp_path):
        exp = tmp_path / "expectations.db"
        init_expectations_db(exp)
        # assert_schema verifies the table is in the global registry
        assert_schema(exp, EXPECTED_EXPECTATIONS_TABLES)
        assert "expectation_calibration_trends" in EXPECTED_EXPECTATIONS_TABLES


# ---------------------------------------------------------------------------
# Few-shot injection — gating
# ---------------------------------------------------------------------------


class TestFewShotGating:
    """AS-1 / AS-2: few-shot block is gated on ≥20 user corrections."""

    def test_below_threshold_returns_empty(self, tmp_path):
        _, exp = _init_dbs(tmp_path)
        gh = tmp_path / "github.db"
        _seed_n_user_corrections(exp, gh, n=CALIBRATION_MIN_CORRECTIONS - 1)
        result = build_few_shot_block(str(exp))
        assert result == ""
        assert FEW_SHOT_BLOCK_MARKER not in result

    def test_at_threshold_injects_block(self, tmp_path):
        _, exp = _init_dbs(tmp_path)
        gh = tmp_path / "github.db"
        _seed_n_user_corrections(exp, gh, n=CALIBRATION_MIN_CORRECTIONS)
        result = build_few_shot_block(str(exp))
        assert result != ""
        assert FEW_SHOT_BLOCK_MARKER in result

    def test_block_contains_five_entries_at_threshold(self, tmp_path):
        _, exp = _init_dbs(tmp_path)
        gh = tmp_path / "github.db"
        _seed_n_user_corrections(exp, gh, n=CALIBRATION_MIN_CORRECTIONS)
        result = build_few_shot_block(str(exp))
        # Count unit bullets — one per example.
        unit_bullets = [
            ln for ln in result.splitlines() if ln.startswith("- unit ")
        ]
        assert len(unit_bullets) == FEW_SHOT_SAMPLE_SIZE

    def test_auto_confirm_rows_excluded(self, tmp_path):
        _, exp = _init_dbs(tmp_path)
        gh = tmp_path / "github.db"
        _seed_n_user_corrections(exp, gh, n=CALIBRATION_MIN_CORRECTIONS)
        # Add some auto_confirm rows with distinctive values.
        _seed_issue(gh, repo="acme/svc", issue_number=9999, type_label="bug")
        _seed_unit(
            gh,
            unit_id="issue:acme/svc#9999",
            root_node_id="issue:acme/svc#9999",
        )
        _seed_correction(
            exp,
            unit_id="issue:acme/svc#9999",
            facet="scope",
            original_value="AUTO-CONFIRM-SENTINEL",
            corrected_value="AUTO-CONFIRM-SENTINEL",
            corrected_by="auto_confirm",
            corrected_at="2029-01-01 00:00:00",  # Most recent — would win if not filtered
        )
        result = build_few_shot_block(str(exp))
        assert "AUTO-CONFIRM-SENTINEL" not in result

    def test_selection_deterministic(self, tmp_path):
        """AS-5: identical inputs produce identical few-shot selection."""
        _, exp = _init_dbs(tmp_path)
        gh = tmp_path / "github.db"
        # Seed rows with identical timestamps to force the tie-break.
        same_ts = "2026-04-10 10:00:00"
        for i in range(CALIBRATION_MIN_CORRECTIONS):
            issue_number = 2000 + i
            repo = "acme/svc"
            root = f"issue:{repo}#{issue_number}"
            _seed_issue(
                gh, repo=repo, issue_number=issue_number, type_label="bug"
            )
            _seed_unit(gh, unit_id=root, root_node_id=root)
            _seed_correction(
                exp,
                unit_id=root,
                facet="scope",
                original_value=f"v-{i}",
                corrected_value=f"w-{i}",
                corrected_by="user",
                corrected_at=same_ts,
            )
        first = build_few_shot_block(str(exp))
        second = build_few_shot_block(str(exp))
        assert first == second


# ---------------------------------------------------------------------------
# Trend computation
# ---------------------------------------------------------------------------


class TestTrendComputation:
    """AS-3 / AS-7: per-work-type deltas land in calibration trends table."""

    def test_below_threshold_noop(self, tmp_path):
        gh, exp = _init_dbs(tmp_path)
        _seed_n_user_corrections(exp, gh, n=CALIBRATION_MIN_CORRECTIONS - 1)
        result = calibration.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
        )
        assert result == {}
        # Table must be empty.
        conn = sqlite3.connect(str(exp))
        try:
            rows = conn.execute(
                "SELECT * FROM expectation_calibration_trends"
            ).fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_at_threshold_writes_rows(self, tmp_path):
        gh, exp = _init_dbs(tmp_path)
        _seed_n_user_corrections(
            exp, gh, n=CALIBRATION_MIN_CORRECTIONS, type_label="bug"
        )
        result = calibration.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
        )
        assert "bug" in result
        entry = result["bug"]
        assert entry["sample_count"] == CALIBRATION_MIN_CORRECTIONS
        # At least one facet has a numeric delta.
        numeric = [
            entry.get(f"avg_{f}_delta") for f in ("scope", "effort", "outcome")
        ]
        assert any(isinstance(v, float) for v in numeric)

    def test_work_type_grouping(self, tmp_path):
        """AS-3: multiple type_labels produce multiple trend rows."""
        gh, exp = _init_dbs(tmp_path)
        # 15 bug corrections + 10 refactor = 25 total, above threshold.
        _seed_n_user_corrections(exp, gh, n=15, type_label="bug")
        # Continue numbering to avoid PK collisions.
        clock = datetime(2026, 4, 11, 10, 0, 0, tzinfo=timezone.utc)
        for i in range(10):
            issue_number = 3000 + i
            repo = "acme/svc"
            root = f"issue:{repo}#{issue_number}"
            _seed_issue(
                gh, repo=repo, issue_number=issue_number, type_label="refactor"
            )
            _seed_unit(gh, unit_id=root, root_node_id=root)
            ts = (clock + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
            _seed_correction(
                exp,
                unit_id=root,
                facet="effort",
                original_value="small",
                corrected_value="large",
                corrected_by="user",
                corrected_at=ts,
            )
        result = calibration.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
        )
        assert set(result.keys()) >= {"bug", "refactor"}
        assert result["refactor"]["sample_count"] == 10

    def test_unknown_work_type_fallback(self, tmp_path):
        """Units with NULL type_label fall under 'unknown'."""
        gh, exp = _init_dbs(tmp_path)
        _seed_n_user_corrections(
            exp, gh, n=CALIBRATION_MIN_CORRECTIONS, type_label=None
        )
        result = calibration.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
        )
        assert "unknown" in result

    def test_upsert_idempotent_on_rerun(self, tmp_path):
        """Re-running updates the trend row rather than duplicating."""
        gh, exp = _init_dbs(tmp_path)
        _seed_n_user_corrections(
            exp, gh, n=CALIBRATION_MIN_CORRECTIONS, type_label="bug"
        )
        calibration.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp)
        )
        calibration.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp)
        )
        conn = sqlite3.connect(str(exp))
        try:
            rows = conn.execute(
                "SELECT work_type, week_start FROM expectation_calibration_trends"
            ).fetchall()
        finally:
            conn.close()
        # No duplicates per (work_type, week_start).
        assert len(rows) == len(set(rows))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRendering:
    """AS-4: render_calibration_block produces the retrospective section."""

    def test_empty_trends_returns_empty_list(self):
        assert render_calibration_block({}) == []

    def test_section_header_present(self):
        trends = {
            "bug": {
                "avg_scope_delta": 0.5,
                "avg_effort_delta": 0.75,
                "avg_outcome_delta": None,
                "sample_count": 20,
            }
        }
        lines = render_calibration_block(trends)
        assert any("## Calibration Trends" in ln for ln in lines)
        assert any("bug" in ln for ln in lines)

    def test_top_n_limits_rendered_entries(self):
        trends = {
            f"type{i}": {
                "avg_scope_delta": i * 0.1,
                "avg_effort_delta": None,
                "avg_outcome_delta": None,
                "sample_count": 5,
            }
            for i in range(10)
        }
        lines = render_calibration_block(trends, top_n=3)
        bullets = [ln for ln in lines if ln.startswith("- ")]
        assert len(bullets) == 3


# ---------------------------------------------------------------------------
# load_trends
# ---------------------------------------------------------------------------


class TestLoadTrends:
    def test_load_trends_empty_when_missing(self, tmp_path):
        _, exp = _init_dbs(tmp_path)
        assert load_trends(str(exp), WEEK_START) == {}

    def test_load_trends_round_trip(self, tmp_path):
        gh, exp = _init_dbs(tmp_path)
        _seed_n_user_corrections(
            exp, gh, n=CALIBRATION_MIN_CORRECTIONS, type_label="bug"
        )
        calibration.run(
            WEEK_START, github_db=str(gh), expectations_db=str(exp)
        )
        loaded = load_trends(str(exp), WEEK_START)
        assert "bug" in loaded
        assert loaded["bug"]["sample_count"] == CALIBRATION_MIN_CORRECTIONS


# ---------------------------------------------------------------------------
# Weekly pipeline integration (idempotency of the .md)
# ---------------------------------------------------------------------------


class TestWeeklyIntegration:
    """AS-8: retrospective .md is not overwritten on re-run."""

    def test_md_idempotency(self, tmp_path):
        """Pre-existing retrospective .md is preserved on re-run."""
        from synthesis.output_writer import write_retrospective

        output_dir = tmp_path / "retrospectives"
        output_dir.mkdir()
        md_path = output_dir / f"{WEEK_START}.md"
        md_path.write_text("ORIGINAL CONTENT\n", encoding="utf-8")
        mtime_before = md_path.stat().st_mtime

        # Attempt to overwrite — writer must refuse.
        result = write_retrospective(
            "NEW CONTENT\n", output_dir, WEEK_START
        )
        assert result is None
        assert md_path.read_text(encoding="utf-8") == "ORIGINAL CONTENT\n"
        assert md_path.stat().st_mtime == mtime_before
