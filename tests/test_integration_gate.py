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

import os
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


def _run_collectors() -> subprocess.CompletedProcess:
    """Run run_collectors.sh and return the result."""
    script = REPO_ROOT / "run_collectors.sh"
    return subprocess.run(
        ["bash", str(script)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
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

    # Known tables for each database — avoids dynamic SQL construction
    GITHUB_TABLES = ("issues", "pull_requests", "pushes", "issue_pr_links")

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
