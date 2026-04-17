"""Integration gate: smoke test for run_collectors end-to-end pipeline.

Gated by the AMIS_INTEGRATION environment variable. Tests are skipped
unless AMIS_INTEGRATION=1 is set:

    AMIS_INTEGRATION=1 pytest tests/test_integration_gate.py -v

These tests run the actual collectors against real (or configured) data
sources. They require:
  - config.yaml with valid settings
  - gh CLI authenticated (for GitHub poller)
  - ActivityWatch running (for appswitch — absence is tolerated)
"""

from __future__ import annotations

import datetime
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_REASON = "Set AMIS_INTEGRATION=1 to run integration tests"
pytestmark = pytest.mark.skipif(
    os.environ.get("AMIS_INTEGRATION") != "1",
    reason=SKIP_REASON,
)


def _run_collectors(
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run run_collectors.sh and return the result.

    ``env_overrides`` lets a test inject extra environment variables
    (e.g. ``AMIS_FORCE_SYNTHESIS=1`` to exercise the weekly-cadence
    branch on any day of the week).
    """
    script = REPO_ROOT / "run_collectors.sh"
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        ["bash", str(script)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )


class TestIntegrationGate:
    """Smoke tests for the full collector pipeline."""

    def test_run_collectors_produces_log(self) -> None:
        """run_collectors.sh creates a dated log file under logs/."""
        result = _run_collectors()
        assert result.returncode == 0, (
            f"run_collectors.sh exited {result.returncode}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        log_dir = REPO_ROOT / "logs"
        assert log_dir.exists(), "logs/ directory was not created"
        log_files = sorted(log_dir.glob("run_*.log"))
        assert len(log_files) > 0, "No log file created by run_collectors.sh"

    def test_health_check_passes(self) -> None:
        """health_check.py exits 0 after a clean collector run."""
        run_result = _run_collectors()
        assert run_result.returncode == 0, (
            f"run_collectors.sh exited {run_result.returncode}:\n"
            f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
        )
        result = subprocess.run(
            [sys.executable, "-m", "am_i_shipping.health_check"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"health_check exited {result.returncode}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

    # Known tables for each database — avoids dynamic SQL construction.
    # Epic #17 Sub-Issue 2 (#35) adds ``commits`` and ``timeline_events``;
    # both are populated by the new E-1 / E-2 collectors enabled by default.
    # Epic #50 replaced the Phase 1 ``issues`` / ``pull_requests`` /
    # ``pushes`` / ``issue_pr_links`` tables with the unified Phase 2
    # schema (``graph_nodes`` / ``graph_edges`` / ``units``). The old
    # names are intentionally omitted — they no longer exist in
    # ``github.db`` and referencing them would leave the "has any data"
    # probe testing nothing.
    GITHUB_TABLES = (
        "graph_nodes",
        "graph_edges",
        "units",
        "commits",
        "timeline_events",
    )

    def test_databases_have_data(self) -> None:
        """After a run, implemented collector DBs contain rows."""
        run_result = _run_collectors()
        assert run_result.returncode == 0, (
            f"run_collectors.sh exited {run_result.returncode}:\n"
            f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
        )
        data_dir = REPO_ROOT / "data"

        # sessions.db — session parser
        sessions_db = data_dir / "sessions.db"
        if sessions_db.exists():
            conn = sqlite3.connect(str(sessions_db))
            count = conn.execute(
                "SELECT COUNT(*) FROM sessions"
            ).fetchone()[0]
            conn.close()
            assert count > 0, "sessions.db has no rows after collector run"

        # github.db — GitHub poller
        github_db = data_dir / "github.db"
        if github_db.exists():
            conn = sqlite3.connect(str(github_db))
            has_data = False
            for table_name in self.GITHUB_TABLES:
                try:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM [{table_name}]"
                    ).fetchone()[0]
                    if count > 0:
                        has_data = True
                        break
                except sqlite3.OperationalError:
                    # Table may not exist yet
                    continue
            conn.close()
            assert has_data, "github.db has no data in any table after collector run"

    def test_no_duplicate_rows_on_rerun(self) -> None:
        """Re-running collectors over the same period produces no duplicates."""
        # First run
        run_result = _run_collectors()
        assert run_result.returncode == 0, (
            f"First run_collectors.sh exited {run_result.returncode}:\n"
            f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
        )
        data_dir = REPO_ROOT / "data"

        # Capture row counts before second run
        counts_before: dict[str, int] = {}

        sessions_db = data_dir / "sessions.db"
        if sessions_db.exists():
            conn = sqlite3.connect(str(sessions_db))
            counts_before["sessions"] = conn.execute(
                "SELECT COUNT(*) FROM sessions"
            ).fetchone()[0]
            conn.close()

        github_db = data_dir / "github.db"
        if github_db.exists():
            conn = sqlite3.connect(str(github_db))
            for table_name in self.GITHUB_TABLES:
                try:
                    counts_before[f"github.{table_name}"] = conn.execute(
                        f"SELECT COUNT(*) FROM [{table_name}]"
                    ).fetchone()[0]
                except sqlite3.OperationalError:
                    continue
            conn.close()

        # Second run
        run_result2 = _run_collectors()
        assert run_result2.returncode == 0, (
            f"Second run_collectors.sh exited {run_result2.returncode}:\n"
            f"stdout: {run_result2.stdout}\nstderr: {run_result2.stderr}"
        )

        # Verify counts are unchanged (idempotency)
        if sessions_db.exists():
            conn = sqlite3.connect(str(sessions_db))
            count_after = conn.execute(
                "SELECT COUNT(*) FROM sessions"
            ).fetchone()[0]
            conn.close()
            assert count_after == counts_before.get("sessions", 0), (
                f"sessions.db row count changed: "
                f"{counts_before.get('sessions', 0)} -> {count_after}"
            )

        if github_db.exists():
            conn = sqlite3.connect(str(github_db))
            for table_name in self.GITHUB_TABLES:
                try:
                    count_after = conn.execute(
                        f"SELECT COUNT(*) FROM [{table_name}]"
                    ).fetchone()[0]
                except sqlite3.OperationalError:
                    continue
                expected = counts_before.get(f"github.{table_name}", 0)
                assert count_after == expected, (
                    f"github.db.{table_name} row count changed: "
                    f"{expected} -> {count_after}"
                )
            conn.close()

    def test_health_json_updated(self) -> None:
        """data/health.json is updated after every collector run."""
        import json
        from datetime import datetime, timezone, timedelta

        run_result = _run_collectors()
        assert run_result.returncode == 0, (
            f"run_collectors.sh exited {run_result.returncode}:\n"
            f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
        )
        health_path = REPO_ROOT / "data" / "health.json"
        assert health_path.exists(), "health.json not found after collector run"

        with open(health_path) as f:
            data = json.load(f)

        now = datetime.now(timezone.utc)
        threshold = timedelta(minutes=5)

        # At least one collector should have a recent last_success
        found_recent = False
        for collector_name, entry in data.items():
            last_success_str = entry.get("last_success")
            if last_success_str:
                last_success = datetime.fromisoformat(last_success_str)
                if last_success.tzinfo is None:
                    last_success = last_success.replace(tzinfo=timezone.utc)
                if now - last_success < threshold:
                    found_recent = True
                    break

        assert found_recent, (
            "No collector has a last_success within the last 5 minutes "
            f"in health.json: {data}"
        )

    # ------------------------------------------------------------------
    # Session timestamp coverage gate (issue #53)
    # ------------------------------------------------------------------
    # Weekly synthesis computes dark_time_pct and elapsed_days from
    # session_started_at / session_ended_at. When those columns are NULL
    # (pre-Epic-#17 rows that have never been backfilled), the unit
    # summary table collapses to zeros and the retrospective is
    # meaningless. This gate asserts that at least 90% of rows in
    # sessions.db have a populated session_started_at before the
    # integration suite is considered green — if the threshold is not
    # met, the operator must run:
    #
    #     python -m am_i_shipping.scripts.backfill_session_timestamps
    #
    # See setup.md Step 4 for context.

    SESSION_TIMESTAMP_COVERAGE_MIN = 0.90

    def test_session_timestamp_coverage_gate(self) -> None:
        """>=90% of rows in sessions.db have a populated session_started_at.

        If sessions.db does not exist yet (fresh install before first
        collector run), the test is skipped — there is nothing to gate.
        Once the DB has rows, any drop below 90% fails the gate and
        directs the operator to the backfill script.
        """
        sessions_db = REPO_ROOT / "data" / "sessions.db"
        if not sessions_db.exists():
            pytest.skip("data/sessions.db does not exist yet; nothing to gate")

        conn = sqlite3.connect(str(sessions_db))
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM sessions"
            ).fetchone()[0]
            with_ts = conn.execute(
                "SELECT COUNT(*) FROM sessions "
                "WHERE session_started_at IS NOT NULL"
            ).fetchone()[0]
        finally:
            conn.close()

        if total == 0:
            pytest.skip("sessions.db has no rows yet; nothing to gate")

        coverage = with_ts / total
        assert coverage >= self.SESSION_TIMESTAMP_COVERAGE_MIN, (
            f"session_started_at coverage is {coverage:.1%} "
            f"({with_ts}/{total}); required minimum is "
            f"{self.SESSION_TIMESTAMP_COVERAGE_MIN:.0%}. "
            "Run: python -m am_i_shipping.scripts.backfill_session_timestamps "
            "(see setup.md Step 4)."
        )

    # ------------------------------------------------------------------
    # Weekly synthesis block (issue #40 / F-4 in PR-48 review-fix cycle)
    # ------------------------------------------------------------------
    # The scheduler invokes ``am-synthesize`` on Sundays, or on any day
    # when ``AMIS_FORCE_SYNTHESIS=1``. The existing tests above skip
    # this branch on non-Sundays, so without these two cases the entire
    # cadence block has no positive coverage.

    def test_synthesis_block_skipped_on_non_sunday_without_force(self) -> None:
        """Without FORCE and not on Sunday, the synthesis block is SKIPPED.

        Negative assertion: the log must NOT contain the
        ``--- Starting: Weekly Synthesis`` marker. Skipped with reason
        when today happens to be Sunday — the positive case is covered
        separately.
        """
        import datetime as _dt

        if _dt.date.today().weekday() == 6:  # Sunday in Python weekday
            pytest.skip("today is Sunday; skip-branch not exercised")

        result = _run_collectors()
        assert result.returncode == 0, (
            f"run_collectors.sh exited {result.returncode}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        log_files = sorted((REPO_ROOT / "logs").glob("run_*.log"))
        assert log_files, "no log file produced"
        log_text = log_files[-1].read_text(encoding="utf-8", errors="replace")
        assert "Starting: Weekly Synthesis" not in log_text, (
            "synthesis block ran on a non-Sunday without FORCE; "
            "cadence guard is broken:\n" + log_text
        )

    def test_synthesis_block_runs_with_force_env(self) -> None:
        """With ``AMIS_FORCE_SYNTHESIS=1`` the synthesis block always runs.

        Positive assertion: log contains the "Starting: Weekly Synthesis"
        marker and the WEEK_START is a valid YYYY-MM-DD token. The
        am-synthesize invocation may legitimately exit non-zero (missing
        API key, empty DB, etc.); per F-5 that must NOT flip the
        scheduler to a non-zero exit code, so we also assert exit 0
        regardless of whether synthesis itself succeeded.
        """
        import re

        result = _run_collectors({"AMIS_FORCE_SYNTHESIS": "1"})
        assert result.returncode == 0, (
            f"run_collectors.sh exited {result.returncode} with "
            f"AMIS_FORCE_SYNTHESIS=1 — synthesis failure must NOT flip "
            f"the scheduler to non-zero.\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
        log_files = sorted((REPO_ROOT / "logs").glob("run_*.log"))
        assert log_files, "no log file produced"
        log_text = log_files[-1].read_text(encoding="utf-8", errors="replace")
        assert "Starting: Weekly Synthesis" in log_text, (
            "synthesis block did not start under AMIS_FORCE_SYNTHESIS=1:\n"
            + log_text
        )
        # WEEK_START must appear as YYYY-MM-DD in the "(week=...)" parens.
        match = re.search(
            r"Starting: Weekly Synthesis \(week=(\d{4}-\d{2}-\d{2})\)",
            log_text,
        )
        assert match, (
            "synthesis block started but WEEK_START not a valid "
            "YYYY-MM-DD in the log:\n" + log_text
        )


# ----------------------------------------------------------------------
# Phase 2 pipeline integration (issue #55 / P-6)
# ----------------------------------------------------------------------
# The tests above exercise the Phase 1 collector pipeline
# (``run_collectors.sh`` → sessions / github pollers). They never call
# the Phase 2 synthesis chain — ``am-prepare-week`` followed by
# ``am-synthesize`` — on a real ``week_start``. Without that coverage
# breakage such as the ``session:session:`` doubling regression
# (#51) or the stale ``week_start='all'`` pollution (see
# ``_clean_stale_phase2_rows``) only surfaces in production.
#
# Preconditions (enforced by earlier merged issues on this epic):
#   * P-1 #52  — ``am-prepare-week`` CLI exists and populates
#                ``graph_nodes`` / ``units``.
#   * P-3 #51  — session node IDs no longer double-prefix ``session:``.
#   * P-4 #53  — ``sessions.session_started_at`` is backfilled on
#                existing rows (gated by the timestamp coverage test).
#   * P-5 #56  — ``config.yaml`` exposes a ``synthesis:`` section.
#
# All tests gated on ``AMIS_INTEGRATION=1`` via the module-level
# ``pytestmark`` above.


def _last_sunday(today: datetime.date | None = None) -> str:
    """Return the most recent Sunday as ``YYYY-MM-DD``.

    Python's ``weekday()`` returns 0 for Monday and 6 for Sunday. Any
    offset computed from ``weekday()`` treats the previous Sunday as
    the week boundary — matching the convention used by
    ``am-prepare-week`` and ``am-synthesize``. When ``today`` itself is
    a Sunday, that Sunday is returned (not the Sunday seven days
    prior) so reruns on Sunday target the current week.
    """
    if today is None:
        today = datetime.date.today()
    # weekday(): Mon=0 ... Sun=6. Days since last Sunday:
    #   Sun -> 0, Mon -> 1, Tue -> 2, ..., Sat -> 6
    days_since_sunday = (today.weekday() + 1) % 7
    sunday = today - datetime.timedelta(days=days_since_sunday)
    return sunday.isoformat()


def _clean_stale_phase2_rows(github_db: Path) -> None:
    """Delete any ``week_start = 'all'`` pollution from Phase 2 tables.

    The 2026-04-17 smoke test for this issue wrote 15,845 / 63 / 15,785
    rows into ``graph_nodes`` / ``graph_edges`` / ``units`` with
    ``week_start = 'all'`` before ``am-prepare-week`` required a real
    ISO week. Those rows are not keyed on a real week and silently
    distort count-based idempotency assertions. Delete them before the
    Phase 2 tests run so T-3 / T-5 compare real-week counts only.

    Safe no-op when the tables are absent (fresh install) or when no
    ``'all'`` rows exist.
    """
    if not github_db.exists():
        return
    conn = sqlite3.connect(str(github_db))
    try:
        for table in ("graph_nodes", "graph_edges", "units"):
            try:
                conn.execute(
                    f"DELETE FROM [{table}] WHERE week_start = 'all'"
                )
            except sqlite3.OperationalError:
                # Table not created yet — nothing to clean.
                continue
        conn.commit()
    finally:
        conn.close()


class TestPhase2Pipeline:
    """Phase 2 smoke tests — run prepare/synthesize on the real DB.

    Every test here has side effects on ``data/github.db`` and on the
    ``retrospectives/.dry-run/`` directory. They must only run when
    ``AMIS_INTEGRATION=1`` is set (enforced by the module-level
    ``pytestmark``). Each test computes its own ``week_start`` via
    :func:`_last_sunday` so the suite stays deterministic when run on
    any day of the week.
    """

    # Dry-run prompt size ceiling — matches the 512 KB water-fill
    # transcript budget in ``synthesis.weekly``. A file larger than this
    # signals the budgeter broke.
    DRY_RUN_SIZE_LIMIT_BYTES = 512 * 1024

    # Regex for the doubled type-prefix regression fixed by P-3 (#51).
    # The original bug lived in the unit-graph ID formatter and could
    # double any namespace prefix — not just the ones that happened to
    # exist when P-3 landed. Match ANY lowercase-identifier prefix
    # doubled with a colon separator (e.g. ``session:session:``,
    # ``issue:issue:``, ``pr:pr:``, and any future node families such
    # as ``commit:commit:`` or ``unit:unit:``). If a new node family
    # is added to the graph, no update to this regex is required.
    DOUBLE_PREFIX_PATTERN = re.compile(r"\b([a-z][a-z_]*):\1:")

    # >=90% of sessions must have ``session_started_at`` set. This
    # gates the P-4 (#53) backfill — without it, Phase 2 unit summaries
    # collapse to zero dark-time / elapsed-days and the retrospective
    # is meaningless.
    TIMESTAMP_COVERAGE_MIN = 0.90

    # ------------------------------------------------------------------
    # Setup: purge the stale ``week_start='all'`` pollution exactly once
    # per test (not once per session) — cheap, and makes each test
    # independent of sibling-test ordering.
    # ------------------------------------------------------------------
    def setup_method(self, _method) -> None:
        _clean_stale_phase2_rows(REPO_ROOT / "data" / "github.db")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _run_prepare_week(week: str) -> subprocess.CompletedProcess:
        """Invoke ``am-prepare-week --week <week>``.

        Uses the current Python interpreter's console script so the
        test does not depend on ``PATH`` resolution — ``sys.executable``
        points at the venv that installed the package in editable mode.
        """
        return subprocess.run(
            [sys.executable, "-m", "synthesis.prepare", "--week", week],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )

    @staticmethod
    def _run_synthesize_dry_run(week: str) -> subprocess.CompletedProcess:
        """Invoke ``am-synthesize --week <week> --dry-run``."""
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "synthesis.cli",
                "--week",
                week,
                "--dry-run",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )

    @staticmethod
    def _count_rows(db: Path, table: str, week: str) -> int:
        conn = sqlite3.connect(str(db))
        try:
            return conn.execute(
                f"SELECT COUNT(*) FROM [{table}] WHERE week_start = ?",
                (week,),
            ).fetchone()[0]
        finally:
            conn.close()

    @staticmethod
    def _dry_run_path(week: str) -> Path:
        return REPO_ROOT / "retrospectives" / ".dry-run" / f"{week}.prompt.txt"

    # ------------------------------------------------------------------
    # T-2
    # ------------------------------------------------------------------
    def test_prepare_week_populates_graph(self) -> None:
        """``am-prepare-week`` exits 0 and writes rows for the target week."""
        week = _last_sunday()
        result = self._run_prepare_week(week)
        assert result.returncode == 0, (
            f"am-prepare-week exited {result.returncode}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

        github_db = REPO_ROOT / "data" / "github.db"
        assert github_db.exists(), "data/github.db does not exist"

        graph_count = self._count_rows(github_db, "graph_nodes", week)
        units_count = self._count_rows(github_db, "units", week)

        # Both tables must have at least one row keyed to the real week.
        # An empty DB (zero rows) is a legitimate "nothing happened this
        # week" state only if the collector has never run — in an
        # integration gate that always follows a real collector run we
        # expect at least some activity.
        assert graph_count > 0, (
            f"graph_nodes has no rows for week_start={week} after "
            f"am-prepare-week; stdout:\n{result.stdout}"
        )
        assert units_count > 0, (
            f"units has no rows for week_start={week} after "
            f"am-prepare-week; stdout:\n{result.stdout}"
        )

    # ------------------------------------------------------------------
    # T-3
    # ------------------------------------------------------------------
    def test_prepare_week_is_idempotent(self) -> None:
        """Running ``am-prepare-week`` twice is a no-op on the second run."""
        week = _last_sunday()

        first = self._run_prepare_week(week)
        assert first.returncode == 0, (
            f"first am-prepare-week exited {first.returncode}:\n"
            f"stderr: {first.stderr}"
        )

        github_db = REPO_ROOT / "data" / "github.db"
        graph_before = self._count_rows(github_db, "graph_nodes", week)
        units_before = self._count_rows(github_db, "units", week)

        second = self._run_prepare_week(week)
        assert second.returncode == 0, (
            f"second am-prepare-week exited {second.returncode}:\n"
            f"stderr: {second.stderr}"
        )

        graph_after = self._count_rows(github_db, "graph_nodes", week)
        units_after = self._count_rows(github_db, "units", week)

        assert graph_after == graph_before, (
            f"graph_nodes row count changed on rerun "
            f"({graph_before} -> {graph_after}) — idempotency broken"
        )
        assert units_after == units_before, (
            f"units row count changed on rerun "
            f"({units_before} -> {units_after}) — idempotency broken"
        )

    # ------------------------------------------------------------------
    # T-4
    # ------------------------------------------------------------------
    def test_synthesize_dry_run_completes(self) -> None:
        """``am-synthesize --dry-run`` writes a bounded, clean prompt file."""
        week = _last_sunday()

        # Prepare must run first — synthesize reads ``units``.
        prep = self._run_prepare_week(week)
        assert prep.returncode == 0, (
            f"am-prepare-week exited {prep.returncode}:\n"
            f"stderr: {prep.stderr}"
        )

        result = self._run_synthesize_dry_run(week)
        assert result.returncode == 0, (
            f"am-synthesize --dry-run exited {result.returncode}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

        prompt_path = self._dry_run_path(week)
        assert prompt_path.exists(), (
            f"dry-run prompt not written to {prompt_path}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )

        size = prompt_path.stat().st_size
        assert size <= self.DRY_RUN_SIZE_LIMIT_BYTES, (
            f"dry-run prompt is {size} bytes, exceeds "
            f"{self.DRY_RUN_SIZE_LIMIT_BYTES} byte ceiling — water-fill "
            "transcript budgeter likely regressed"
        )

        content = prompt_path.read_text(encoding="utf-8", errors="replace")
        double_prefix = self.DOUBLE_PREFIX_PATTERN.search(content)
        assert double_prefix is None, (
            f"dry-run prompt contains doubled type prefix "
            f"{double_prefix.group(0)!r} at offset {double_prefix.start()} "
            "— P-3 (#51) regression"
        )

    # ------------------------------------------------------------------
    # T-5
    # ------------------------------------------------------------------
    def test_full_pipeline_idempotent(self) -> None:
        """Running prepare + synthesize --dry-run twice is stable."""
        week = _last_sunday()

        # First full pass.
        prep1 = self._run_prepare_week(week)
        assert prep1.returncode == 0, (
            f"first am-prepare-week exited {prep1.returncode}:\n"
            f"stderr: {prep1.stderr}"
        )
        syn1 = self._run_synthesize_dry_run(week)
        assert syn1.returncode == 0, (
            f"first am-synthesize --dry-run exited {syn1.returncode}:\n"
            f"stderr: {syn1.stderr}"
        )

        github_db = REPO_ROOT / "data" / "github.db"
        units_before = self._count_rows(github_db, "units", week)
        prompt_path = self._dry_run_path(week)
        assert prompt_path.exists(), "dry-run prompt not written on first pass"
        content_before = prompt_path.read_text(encoding="utf-8", errors="replace")

        # Second full pass.
        prep2 = self._run_prepare_week(week)
        assert prep2.returncode == 0, (
            f"second am-prepare-week exited {prep2.returncode}:\n"
            f"stderr: {prep2.stderr}"
        )
        syn2 = self._run_synthesize_dry_run(week)
        assert syn2.returncode == 0, (
            f"second am-synthesize --dry-run exited {syn2.returncode}:\n"
            f"stderr: {syn2.stderr}"
        )

        units_after = self._count_rows(github_db, "units", week)
        content_after = prompt_path.read_text(encoding="utf-8", errors="replace")

        assert units_after == units_before, (
            f"units row count changed between passes "
            f"({units_before} -> {units_after})"
        )

        # Allow the prompt to differ on at most one line that looks
        # like a timestamp. The dry-run prompt assembler in
        # ``synthesis.weekly`` does not (yet) emit a single, stable,
        # named field for the generation timestamp — so this check
        # uses a heuristic (keyword or ISO-date fragment) rather than
        # an exact field-prefix match. Once the assembler commits to
        # a canonical field name, tighten this to that prefix. Until
        # then: single-line timestamp divergence is acceptable;
        # anything else is a real non-determinism bug.
        lines_before = content_before.splitlines()
        lines_after = content_after.splitlines()
        if lines_before == lines_after:
            return

        assert len(lines_before) == len(lines_after), (
            f"dry-run prompt line count changed "
            f"({len(lines_before)} -> {len(lines_after)}) — content "
            "is non-deterministic beyond timestamps"
        )
        diffs = [
            (i, a, b)
            for i, (a, b) in enumerate(zip(lines_before, lines_after))
            if a != b
        ]
        # At most one differing line, and that line must look timestampy.
        assert len(diffs) <= 1, (
            f"dry-run prompt has {len(diffs)} differing lines between "
            f"passes; first: line {diffs[0][0]} {diffs[0][1]!r} vs "
            f"{diffs[0][2]!r}"
        )
        if diffs:
            _, a, b = diffs[0]
            # A line qualifies as "just a timestamp" if either (a) it
            # contains one of the explicit timestamp-field keywords or
            # (b) either side embeds an ISO-8601 date fragment
            # (``YYYY-MM-DD``) — the latter is month-agnostic, so this
            # check does not rot when the calendar rolls into a month
            # not explicitly listed here.
            iso_date = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")
            lower_a = a.lower()
            lower_b = b.lower()
            keyword_hit = any(
                token in lower_a or token in lower_b
                for token in ("time", "date", "generated", "timestamp")
            )
            iso_hit = bool(iso_date.search(a) or iso_date.search(b))
            timestampy = keyword_hit or iso_hit
            assert timestampy, (
                f"dry-run prompt differs on a non-timestamp line: "
                f"{a!r} vs {b!r}"
            )

    # ------------------------------------------------------------------
    # Timestamp coverage — gates the P-4 (#53) backfill.
    # ------------------------------------------------------------------
    def test_timestamp_coverage(self) -> None:
        """>=90% of ``sessions`` rows have a populated ``session_started_at``.

        Duplicates the intent of ``TestIntegrationGate
        .test_session_timestamp_coverage_gate`` but asserts it inside
        the Phase 2 block so a Phase 2-only pytest selection still
        catches regressions in the backfill.
        """
        sessions_db = REPO_ROOT / "data" / "sessions.db"
        if not sessions_db.exists():
            pytest.skip("data/sessions.db does not exist yet")

        conn = sqlite3.connect(str(sessions_db))
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM sessions"
            ).fetchone()[0]
            with_ts = conn.execute(
                "SELECT COUNT(*) FROM sessions "
                "WHERE session_started_at IS NOT NULL"
            ).fetchone()[0]
        finally:
            conn.close()

        if total == 0:
            pytest.skip("sessions.db has no rows yet")

        coverage = with_ts / total
        assert coverage >= self.TIMESTAMP_COVERAGE_MIN, (
            f"session_started_at coverage is {coverage:.1%} "
            f"({with_ts}/{total}); required minimum is "
            f"{self.TIMESTAMP_COVERAGE_MIN:.0%}. Run: "
            "python -m am_i_shipping.scripts.backfill_session_timestamps"
        )
