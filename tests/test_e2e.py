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

    def test_rerun_batch_zero_new_rows(self, tmp_path):
        """Re-running batch mode produces zero new rows."""
        config_path = _create_e2e_config(tmp_path)

        # First run
        run_batch(config_path=str(config_path))
        db_path = tmp_path / "data" / "sessions.db"
        conn = sqlite3.connect(str(db_path))
        count1 = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()

        # Second run
        run_batch(config_path=str(config_path))
        conn = sqlite3.connect(str(db_path))
        count2 = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()

        assert count1 == count2, (
            f"Second batch run added rows: {count1} -> {count2}"
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
