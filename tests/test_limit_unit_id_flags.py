"""Tests for --limit / --unit-id feature (Issue #90) — Finding F-1-3.

Covers:
 1. summarize._load_unsummarized_units with unit_ids / limit
 2. expectations._load_week_units with unit_ids / limit
 3. run_extraction with rebuild=True, repo=X, unit_ids from repo Y (F-1-4 regression)
 4. gap_analysis.run scoped to unit_ids (F-1-1 regression)
 5. revision_detector.run scoped to unit_ids with rebuild=True (F-1-2 regression)
 6. run_synthesis with len(unit_ids) > MAX_UNITS_PER_PROMPT (F-1-5 regression)
 7. CLI mutually-exclusive group: --unit-id and --limit raise SystemExit
 8. _positive_int rejects 0 and negative values
"""

from __future__ import annotations

import json
import sqlite3
import unittest.mock as mock
from dataclasses import replace
from pathlib import Path
from typing import List, Optional

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    init_expectations_db,
    init_github_db,
    init_sessions_db,
)

WEEK_START = "2026-04-14"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> SynthesisConfig:
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-6",
        summary_model="claude-haiku-4-5",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


def _init_dbs(tmp_path: Path):
    """Return (gh, sess, exp) paths with fully-initialised schemas."""
    gh = tmp_path / "github.db"
    sess = tmp_path / "sessions.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_sessions_db(sess)
    init_expectations_db(exp)
    return gh, sess, exp


def _seed_unit(
    gh_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    elapsed_days: float = 1.0,
    abandonment_flag: int = 0,
    outlier_flags: str = "[]",
    status: str = "closed",
    repo: Optional[str] = None,
) -> None:
    """Insert a minimal units row (and optionally a repo-linked graph node)."""
    if repo:
        root_node_id = f"issue:{repo}#1"
        root_node_type = "issue"
    else:
        root_node_id = f"n-{unit_id}"
        root_node_type = "session"

    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, "
            " review_cycles, status, outlier_flags, abandonment_flag) "
            "VALUES (?, ?, ?, ?, ?, 0.0, 0, 0, ?, ?, ?)",
            (
                week_start, unit_id, root_node_type, root_node_id,
                elapsed_days, status, outlier_flags, abandonment_flag,
            ),
        )
        if repo:
            conn.execute(
                "INSERT OR IGNORE INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref) "
                "VALUES (?, ?, 'issue', ?)",
                (week_start, root_node_id, root_node_id),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_expectation(
    exp_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    skip_reason: Optional[str] = None,
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
                week_start, unit_id, "turn 0", "fix X",
                "one session", "tests pass",
                0.8 if skip_reason is None else None,
                "claude-sonnet-4-6", 100, skip_reason,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_revision_row(
    exp_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
) -> None:
    """Insert a minimal expectation_revisions row."""
    conn = sqlite3.connect(str(exp_db))
    try:
        conn.execute(
            "INSERT INTO expectation_revisions "
            "(week_start, unit_id, revision_index, revision_turn, "
            " revision_trigger, facet, before_text, after_text, "
            " confidence, detected_at) "
            "VALUES (?, ?, 0, 1, 'session_break', 'scope', 'old', 'new', "
            "        0.8, '2026-04-14')",
            (week_start, unit_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_gap_row(
    exp_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
) -> None:
    """Insert a minimal expectation_gaps row."""
    conn = sqlite3.connect(str(exp_db))
    try:
        conn.execute(
            "INSERT INTO expectation_gaps "
            "(week_start, unit_id, commitment_point, scope_gap, effort_gap, "
            " effort_gap_ratio, outcome_gap, severity, direction, "
            " failure_precondition, computed_at, auto_confirmed) "
            "VALUES (?, ?, 'turn 0', '', '', 1.0, '', 'none', 'match', "
            "        NULL, '2026-04-14 00:00:00', 0)",
            (week_start, unit_id),
        )
        conn.commit()
    finally:
        conn.close()


def _unit_summary_rows(gh_db: Path, week_start: str = WEEK_START) -> list[str]:
    """Return unit_ids present in unit_summaries."""
    conn = sqlite3.connect(str(gh_db))
    try:
        return [r[0] for r in conn.execute(
            "SELECT unit_id FROM unit_summaries WHERE week_start = ?",
            (week_start,),
        ).fetchall()]
    finally:
        conn.close()


def _gap_rows(exp_db: Path, week_start: str = WEEK_START) -> list[str]:
    conn = sqlite3.connect(str(exp_db))
    try:
        return [r[0] for r in conn.execute(
            "SELECT unit_id FROM expectation_gaps WHERE week_start = ?",
            (week_start,),
        ).fetchall()]
    finally:
        conn.close()


def _revision_rows(exp_db: Path, week_start: str = WEEK_START) -> list[str]:
    conn = sqlite3.connect(str(exp_db))
    try:
        return [r[0] for r in conn.execute(
            "SELECT unit_id FROM expectation_revisions WHERE week_start = ?",
            (week_start,),
        ).fetchall()]
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ===========================================================================
# 1. summarize._load_unsummarized_units
# ===========================================================================


class TestLoadUnsummarizedUnits:
    """Unit tests for summarize._load_unsummarized_units."""

    def _conn(self, gh_db: Path):
        return sqlite3.connect(str(gh_db))

    def test_unknown_unit_ids_raises_value_error(self, tmp_path: Path):
        from synthesis.summarize import _load_unsummarized_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="real-unit")
        conn = self._conn(gh)
        try:
            with pytest.raises(ValueError, match="Unknown unit_ids"):
                _load_unsummarized_units(
                    conn, WEEK_START, unit_ids=["does-not-exist"]
                )
        finally:
            conn.close()

    def test_already_summarized_raises_value_error_with_rebuild_hint(
        self, tmp_path: Path
    ):
        from synthesis.summarize import _load_unsummarized_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="unit-done")
        # Seed a summary row so the unit is considered already-summarized.
        conn = self._conn(gh)
        try:
            conn.execute(
                "INSERT INTO unit_summaries "
                "(week_start, unit_id, summary_text, model, input_bytes) "
                "VALUES (?, ?, 'done', 'fake', 4)",
                (WEEK_START, "unit-done"),
            )
            conn.commit()
            with pytest.raises(ValueError) as exc_info:
                _load_unsummarized_units(
                    conn, WEEK_START, unit_ids=["unit-done"]
                )
            msg = str(exc_info.value)
            assert "Already summarized" in msg
            assert "--rebuild-summaries" in msg
        finally:
            conn.close()

    def test_limit_returns_at_most_n_units(self, tmp_path: Path):
        from synthesis.summarize import _load_unsummarized_units

        gh, _, _ = _init_dbs(tmp_path)
        for i in range(5):
            _seed_unit(gh, unit_id=f"u-{i}", elapsed_days=float(i + 1))
        conn = self._conn(gh)
        try:
            result = _load_unsummarized_units(conn, WEEK_START, limit=3)
        finally:
            conn.close()
        assert len(result) <= 3

    def test_limit_priority_order_abandoned_first(self, tmp_path: Path):
        """abandonment_flag=1 units sort before outlier and elapsed units."""
        from synthesis.summarize import _load_unsummarized_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="normal", elapsed_days=5.0, abandonment_flag=0)
        _seed_unit(gh, unit_id="outlier", elapsed_days=3.0, abandonment_flag=0,
                   outlier_flags='["elapsed_days"]')
        _seed_unit(gh, unit_id="abandoned", elapsed_days=1.0, abandonment_flag=1)
        conn = self._conn(gh)
        try:
            result = _load_unsummarized_units(conn, WEEK_START, limit=1)
        finally:
            conn.close()
        assert len(result) == 1
        assert result[0]["unit_id"] == "abandoned"

    def test_limit_priority_outlier_before_plain_elapsed(self, tmp_path: Path):
        """outlier-flagged units rank before non-flagged elapsed units."""
        from synthesis.summarize import _load_unsummarized_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="plain", elapsed_days=10.0, abandonment_flag=0)
        _seed_unit(gh, unit_id="outlier", elapsed_days=1.0, abandonment_flag=0,
                   outlier_flags='["reprompts"]')
        conn = self._conn(gh)
        try:
            result = _load_unsummarized_units(conn, WEEK_START, limit=1)
        finally:
            conn.close()
        assert result[0]["unit_id"] == "outlier"


# ===========================================================================
# 2. expectations._load_week_units
# ===========================================================================


class TestLoadWeekUnits:
    """Unit tests for expectations._load_week_units."""

    def test_unknown_unit_ids_raises_value_error(self, tmp_path: Path):
        from synthesis.expectations import _load_week_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="existing")
        conn = sqlite3.connect(str(gh))
        try:
            with pytest.raises(ValueError, match="Unknown unit_ids"):
                _load_week_units(conn, WEEK_START, unit_ids=["ghost"])
        finally:
            conn.close()

    def test_limit_returns_top_n_by_priority(self, tmp_path: Path):
        from synthesis.expectations import _load_week_units

        gh, _, _ = _init_dbs(tmp_path)
        for i in range(4):
            _seed_unit(gh, unit_id=f"u-{i}", elapsed_days=float(i + 1))
        _seed_unit(gh, unit_id="top", abandonment_flag=1, elapsed_days=0.5)
        conn = sqlite3.connect(str(gh))
        try:
            result = _load_week_units(conn, WEEK_START, limit=1)
        finally:
            conn.close()
        assert result == ["top"]

    def test_unit_ids_filter_returns_only_requested(self, tmp_path: Path):
        from synthesis.expectations import _load_week_units

        gh, _, _ = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="a")
        _seed_unit(gh, unit_id="b")
        _seed_unit(gh, unit_id="c")
        conn = sqlite3.connect(str(gh))
        try:
            result = _load_week_units(conn, WEEK_START, unit_ids=["a", "c"])
        finally:
            conn.close()
        assert sorted(result) == ["a", "c"]


# ===========================================================================
# 3. CLI: --unit-id and --limit are mutually exclusive
# ===========================================================================


class TestCLIExclusivity:
    """argparse mutually-exclusive-group tests for summarize and expectations CLIs."""

    def test_summarize_unit_id_and_limit_mutual_exclusion(self):
        from synthesis.summarize import _build_parser

        parser = _build_parser()
        # argparse calls sys.exit(2) on mutex violations.
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(
                ["--week", WEEK_START, "--unit-id", "x", "--limit", "5"]
            )
        assert exc_info.value.code != 0

    def test_expectations_unit_id_and_limit_mutual_exclusion(self):
        from synthesis.expectations import _build_parser

        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(
                ["--week", WEEK_START, "--unit-id", "x", "--limit", "5"]
            )
        assert exc_info.value.code != 0

    def test_summarize_limit_zero_raises(self):
        """_positive_int must reject 0."""
        from synthesis.summarize import _positive_int
        import argparse

        with pytest.raises(argparse.ArgumentTypeError):
            _positive_int("0")

    def test_summarize_limit_negative_raises(self):
        """_positive_int must reject -1."""
        from synthesis.summarize import _positive_int
        import argparse

        with pytest.raises(argparse.ArgumentTypeError):
            _positive_int("-1")

    def test_expectations_limit_zero_raises(self):
        from synthesis.expectations import _positive_int
        import argparse

        with pytest.raises(argparse.ArgumentTypeError):
            _positive_int("0")


# ===========================================================================
# 4. F-1-4 regression: run_extraction rebuild=True with cross-repo unit_ids
# ===========================================================================


class TestRunExtractionCrossRepoRegression:
    """F-1-4: run_extraction(rebuild=True, repo=X, unit_ids=<id from repo Y>)
    must raise ValueError and must NOT delete the repo-Y row."""

    def test_cross_repo_unit_ids_raises_and_does_not_delete(
        self, tmp_path: Path
    ):
        gh, sess, exp = _init_dbs(tmp_path)

        # Seed unit A in repo-X.
        _seed_unit(gh, unit_id="unit-repo-x", repo="org/repo-x")
        _seed_expectation(exp, unit_id="unit-repo-x")

        # Seed unit B in repo-Y (different repo).
        _seed_unit(gh, unit_id="unit-repo-y", repo="org/repo-y")
        _seed_expectation(exp, unit_id="unit-repo-y")

        from synthesis.expectations import run_extraction

        adapter_stub = _stub_adapter_json()
        with mock.patch("synthesis.expectations._get_adapter", return_value=adapter_stub):
            with pytest.raises(ValueError):
                # unit-repo-y does not belong to repo-x → ValueError.
                run_extraction(
                    _make_config(),
                    github_db=str(gh),
                    sessions_db=str(sess),
                    expectations_db=str(exp),
                    week_start=WEEK_START,
                    rebuild=True,
                    repo="org/repo-x",
                    unit_ids=["unit-repo-y"],
                )

        # Critically: the repo-Y expectation row must still exist.
        conn = sqlite3.connect(str(exp))
        try:
            row = conn.execute(
                "SELECT unit_id FROM expectations WHERE unit_id = 'unit-repo-y'"
            ).fetchone()
        finally:
            conn.close()
        assert row is not None, (
            "F-1-4 regression: unit-repo-y row was deleted even though it "
            "belongs to a different repo"
        )


# ===========================================================================
# 5. F-1-1 regression: gap_analysis.run scoped to unit_ids
# ===========================================================================


class TestGapAnalysisScopedUnitIds:
    """F-1-1: gap_analysis.run(..., unit_ids=['A']) must leave rows for B, C intact."""

    def test_scoped_run_preserves_sibling_gap_rows(self, tmp_path: Path):
        from synthesis import gap_analysis

        gh, _, exp = _init_dbs(tmp_path)

        # Seed units A, B, C with expectations and existing gap rows.
        for uid in ["unit-a", "unit-b", "unit-c"]:
            _seed_unit(gh, unit_id=uid)
            _seed_expectation(exp, unit_id=uid)
            _seed_gap_row(exp, unit_id=uid)

        # Verify all three gap rows exist before the scoped run.
        before = set(_gap_rows(exp))
        assert before == {"unit-a", "unit-b", "unit-c"}

        # Run gap_analysis scoped to only unit-a.
        gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
            unit_ids=["unit-a"],
        )

        # unit-b and unit-c rows must still be present.
        after = set(_gap_rows(exp))
        assert "unit-b" in after, (
            "F-1-1 regression: unit-b gap row was deleted by scoped run"
        )
        assert "unit-c" in after, (
            "F-1-1 regression: unit-c gap row was deleted by scoped run"
        )

    def test_scoped_run_writes_row_for_targeted_unit(self, tmp_path: Path):
        """The targeted unit gets a fresh gap row after a scoped run."""
        from synthesis import gap_analysis

        gh, _, exp = _init_dbs(tmp_path)
        _seed_unit(gh, unit_id="unit-target")
        _seed_unit(gh, unit_id="unit-other")
        _seed_expectation(exp, unit_id="unit-target")
        _seed_expectation(exp, unit_id="unit-other")

        written = gap_analysis.run(
            WEEK_START,
            github_db=str(gh),
            expectations_db=str(exp),
            config=_make_config(),
            unit_ids=["unit-target"],
        )
        assert written >= 1
        assert "unit-target" in set(_gap_rows(exp))


# ===========================================================================
# 6. F-1-2 regression: revision_detector.run scoped to unit_ids
# ===========================================================================


class TestRevisionDetectorScopedUnitIds:
    """F-1-2: revision_detector.run(rebuild=True, unit_ids=['A']) must leave
    revision rows for B and C intact."""

    def _seed_unit_with_graph(self, gh_db: Path, unit_id: str) -> None:
        """Seed a unit + graph node + graph edge so the detector can walk it."""
        root = f"unit:{unit_id}"
        sess_id = f"sess-{unit_id}"
        conn = sqlite3.connect(str(gh_db))
        try:
            conn.execute(
                "INSERT INTO units "
                "(week_start, unit_id, root_node_type, root_node_id, "
                " elapsed_days, dark_time_pct, total_reprompts, review_cycles, "
                " status, outlier_flags, abandonment_flag) "
                "VALUES (?, ?, 'session', ?, 1.0, 0.0, 0, 0, 'closed', '[]', 0)",
                (WEEK_START, unit_id, root),
            )
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref) "
                "VALUES (?, ?, 'unit', ?)",
                (WEEK_START, root, unit_id),
            )
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref) "
                "VALUES (?, ?, 'session', ?)",
                (WEEK_START, sess_id, sess_id),
            )
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type) "
                "VALUES (?, ?, ?, 'unit_contains_session')",
                (WEEK_START, root, sess_id),
            )
            conn.commit()
        finally:
            conn.close()

    def test_scoped_rebuild_preserves_sibling_revision_rows(
        self, tmp_path: Path
    ):
        from synthesis import revision_detector

        gh, sess, exp = _init_dbs(tmp_path)

        # Seed units A, B, C with existing revision rows.
        for uid in ["rev-a", "rev-b", "rev-c"]:
            self._seed_unit_with_graph(gh, uid)
            _seed_expectation(exp, unit_id=uid)
            _seed_revision_row(exp, unit_id=uid)

        before = set(_revision_rows(exp))
        assert before == {"rev-a", "rev-b", "rev-c"}

        # Scoped rebuild for rev-a only.
        revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
            rebuild=True,
            unit_ids=["rev-a"],
        )

        # rev-b and rev-c rows must be untouched.
        after = set(_revision_rows(exp))
        assert "rev-b" in after, (
            "F-1-2 regression: rev-b revision row destroyed by scoped rebuild"
        )
        assert "rev-c" in after, (
            "F-1-2 regression: rev-c revision row destroyed by scoped rebuild"
        )


# ===========================================================================
# 7. F-1-5 regression: run_synthesis truncates unit_ids > MAX_UNITS_PER_PROMPT
# ===========================================================================


class TestRunSynthesisTruncation:
    """F-1-5: when len(unit_ids) > MAX_UNITS_PER_PROMPT, scoped_unit_ids
    passed to gap_analysis.run must be the truncated set, not the full list."""

    def test_scoped_unit_ids_matches_truncated_set(self, tmp_path: Path):
        from synthesis.weekly import run_synthesis, MAX_UNITS_PER_PROMPT

        gh, sess, exp = _init_dbs(tmp_path)

        # Seed MAX+5 units.
        target_count = MAX_UNITS_PER_PROMPT + 5
        unit_ids_list = [f"u-{i:04d}" for i in range(target_count)]
        for uid in unit_ids_list:
            _seed_unit(gh, unit_id=uid)
            # Each needs a unit_summaries row or run_synthesis raises.
            conn = sqlite3.connect(str(gh))
            try:
                conn.execute(
                    "INSERT INTO unit_summaries "
                    "(week_start, unit_id, summary_text, model, input_bytes) "
                    "VALUES (?, ?, 'summary', 'fake', 7)",
                    (WEEK_START, uid),
                )
                conn.commit()
            finally:
                conn.close()

        # Capture the unit_ids kwarg passed to gap_analysis.run.
        captured_unit_ids: list = []

        import synthesis.gap_analysis as gap_mod

        real_gap_run = gap_mod.run

        def _spy_gap_run(week_start, *, github_db, expectations_db, **kwargs):
            captured_unit_ids.extend(kwargs.get("unit_ids") or [])
            # Return 0 — no rows written (we don't need real gap rows here).
            return 0

        out_dir = tmp_path / "retrospectives"
        cfg = _make_config(output_dir=str(out_dir))

        with mock.patch.object(gap_mod, "run", side_effect=_spy_gap_run):
            # Also stub load_gap_rows and revision passes to avoid DB errors.
            with mock.patch("synthesis.gap_analysis.load_gap_rows", return_value=[]):
                with mock.patch("synthesis.revision_detector.run", return_value=0):
                    with mock.patch(
                        "synthesis.revision_detector.load_revision_rows",
                        return_value=[],
                    ):
                        try:
                            run_synthesis(
                                cfg,
                                gh,
                                sess,
                                WEEK_START,
                                dry_run=False,
                                expectations_db=exp,
                                unit_ids=unit_ids_list,
                            )
                        except Exception:
                            # run_synthesis may raise for other reasons
                            # (LLM, output_dir). We only care about the
                            # captured kwargs, so swallow.
                            pass

        # gap_analysis.run must have been called.
        assert captured_unit_ids, (
            "gap_analysis.run was not called — spy never fired"
        )
        # The scoped set must be <= MAX_UNITS_PER_PROMPT.
        assert len(captured_unit_ids) <= MAX_UNITS_PER_PROMPT, (
            f"F-1-5 regression: gap_analysis.run received "
            f"{len(captured_unit_ids)} unit_ids (> MAX={MAX_UNITS_PER_PROMPT})"
        )
        # And it must be strictly fewer than the full list.
        assert len(captured_unit_ids) < target_count, (
            "scoped_unit_ids was not truncated before being passed downstream"
        )


# ===========================================================================
# Stub adapter shared by tests that need a fake LLM
# ===========================================================================


def _stub_adapter_json(expected_scope: str = "foo.py"):
    class _Result:
        def __init__(self, text: str):
            self.text = text
            self.cost_usd = 0.0
            self.input_chars = 0
            self.output_chars = len(text)

    class _Adapter:
        def __init__(self):
            self.calls: list = []

        def call(self, system, user, model, max_tokens):
            self.calls.append((system, user, model, max_tokens))
            body = {
                "commitment_point": "turn 0",
                "expected_scope": expected_scope,
                "expected_effort": "one session",
                "expected_outcome": "tests pass",
                "confidence": 0.75,
            }
            return _Result(json.dumps(body))

    return _Adapter()
