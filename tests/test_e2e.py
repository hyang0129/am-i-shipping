"""End-to-end smoke test against real sessions (C1-5).

Skipped in CI via the AM_I_SHIPPING_E2E env var guard.
Runs --mode batch against real ~/.claude/projects/ if available.
"""

import os
import sqlite3
from pathlib import Path

import pytest

from collector.session_parser import (
    _discover_session_files,
    parse_session,
    run_batch,
)


# Skip unless AM_I_SHIPPING_E2E=1 is set
pytestmark = pytest.mark.skipif(
    os.environ.get("AM_I_SHIPPING_E2E", "0") != "1",
    reason="E2E tests require AM_I_SHIPPING_E2E=1 and real session data",
)

# Default projects path — override with AM_I_SHIPPING_PROJECTS_PATH
PROJECTS_PATH = Path(
    os.environ.get(
        "AM_I_SHIPPING_PROJECTS_PATH",
        str(Path.home() / ".claude" / "projects"),
    )
)


def _create_e2e_config(tmp_path: Path) -> Path:
    """Create config pointing to real projects dir but temp data dir."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
session:
  projects_path: "{PROJECTS_PATH}"
  reprompt_threshold: 3
github:
  repos: ["test/repo"]
data:
  data_dir: "{tmp_path / 'data'}"
"""
    )
    return config_path


class TestE2ESmoke:
    """Smoke tests against real session data."""

    def test_real_sessions_exist(self):
        """Verify we have at least one real session to test against."""
        files = _discover_session_files(PROJECTS_PATH)
        assert len(files) > 0, f"No .jsonl files found under {PROJECTS_PATH}"

    def test_parse_first_real_session(self):
        """Parse at least one real session without crashing."""
        files = _discover_session_files(PROJECTS_PATH)
        assert len(files) > 0

        record = parse_session(files[0])
        assert record.session_uuid is not None
        assert record.turn_count >= 0
        assert record.tool_call_count >= 0
        assert record.tool_failure_count >= 0

    def test_batch_mode_processes_sessions(self, tmp_path):
        """Run batch mode and verify rows are inserted."""
        config_path = _create_e2e_config(tmp_path)
        run_batch(config_path=str(config_path))

        db_path = tmp_path / "data" / "sessions.db"
        assert db_path.exists()

        conn = sqlite3.connect(str(db_path))
        count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()
        assert count > 0, "Batch mode should have processed at least one session"

    def test_rerun_batch_no_duplicates(self, tmp_path):
        """Re-running batch mode produces no duplicate rows.

        New sessions may appear on disk between runs (this is a live system),
        so we assert idempotency — no session_uuid appears more than once —
        rather than asserting the row count is frozen.
        """
        config_path = _create_e2e_config(tmp_path)
        db_path = tmp_path / "data" / "sessions.db"

        # First run
        run_batch(config_path=str(config_path))

        # Second run — may pick up sessions created during the first run
        run_batch(config_path=str(config_path))

        conn = sqlite3.connect(str(db_path))
        duplicates = conn.execute(
            "SELECT session_uuid, COUNT(*) as n FROM sessions "
            "GROUP BY session_uuid HAVING n > 1"
        ).fetchall()
        conn.close()

        assert duplicates == [], (
            f"Duplicate session_uuids after second batch run: {duplicates}"
        )

    def test_known_session_fields(self):
        """Verify turn_count, tool_failure_count, and reprompt_count
        are integers for a real session."""
        files = _discover_session_files(PROJECTS_PATH)
        assert len(files) > 0

        record = parse_session(files[0])
        assert isinstance(record.turn_count, int)
        assert isinstance(record.tool_failure_count, int)
        assert isinstance(record.reprompt_count, int)
