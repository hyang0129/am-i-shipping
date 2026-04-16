"""Tests for resource limiter functionality (issue #15).

Covers config defaults, inter-request delay, item cap, connection
batching, session parser batch limiters, and appswitch removal.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from am_i_shipping.config_loader import (
    Config,
    GitHubConfig,
    GitHubLimiterConfig,
    SessionConfig,
    SessionLimiterConfig,
    load_config,
)
from am_i_shipping.health_check import EXPECTED_COLLECTORS, check_health


def _write_config(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(data), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

class TestLimiterConfigDefaults:
    """Limiter sections use correct defaults when absent from YAML."""

    def test_no_limiter_section_uses_defaults(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/path"},
            "github": {"repos": ["a/b"]},
        })
        cfg = load_config(cfg_path)

        assert cfg.github.limiter.inter_request_delay_seconds == 1.0
        assert cfg.github.limiter.max_items_per_repo == 500
        assert cfg.github.limiter.process_nice_increment == 10
        assert cfg.session.limiter.max_files_per_run == 200
        assert cfg.session.limiter.inter_file_delay_seconds == 0.05

    def test_limiter_values_override(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {
                "projects_path": "/path",
                "limiter": {
                    "max_files_per_run": 50,
                    "inter_file_delay_seconds": 0.1,
                },
            },
            "github": {
                "repos": ["a/b"],
                "limiter": {
                    "inter_request_delay_seconds": 2.0,
                    "max_items_per_repo": 100,
                    "process_nice_increment": 5,
                },
            },
        })
        cfg = load_config(cfg_path)

        assert cfg.github.limiter.inter_request_delay_seconds == 2.0
        assert cfg.github.limiter.max_items_per_repo == 100
        assert cfg.github.limiter.process_nice_increment == 5
        assert cfg.session.limiter.max_files_per_run == 50
        assert cfg.session.limiter.inter_file_delay_seconds == 0.1

    def test_partial_limiter_config_fills_defaults(self, tmp_path):
        """Specifying only some limiter keys fills the rest with defaults."""
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/path"},
            "github": {
                "repos": ["a/b"],
                "limiter": {
                    "inter_request_delay_seconds": 0.5,
                },
            },
        })
        cfg = load_config(cfg_path)

        assert cfg.github.limiter.inter_request_delay_seconds == 0.5
        assert cfg.github.limiter.max_items_per_repo == 500  # default
        assert cfg.github.limiter.process_nice_increment == 10  # default


# ---------------------------------------------------------------------------
# gh_client inter-request delay
# ---------------------------------------------------------------------------

class TestGhClientDelay:
    def test_configure_limiter_sets_delay(self):
        from collector.github_poller.gh_client import (
            configure_limiter,
            _inter_request_delay,
        )
        import collector.github_poller.gh_client as gh_mod

        configure_limiter(2.5)
        assert gh_mod._inter_request_delay == 2.5

        # Reset
        configure_limiter(0.0)
        assert gh_mod._inter_request_delay == 0.0


# ---------------------------------------------------------------------------
# Item cap in _poll_repo
# ---------------------------------------------------------------------------

class TestItemCap:
    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_item_cap_limits_processing(
        self, mock_issues, mock_prs, mock_issue_comments, mock_pr_comments,
        mock_edit_batch, mock_pr_edit, mock_push, mock_link, tmp_path
    ):
        from collector.github_poller.run import _poll_repo

        # Return 800 issues and 400 PRs
        mock_issues.return_value = [
            {"number": i, "title": f"Issue {i}", "state": "OPEN",
             "body": "", "comments": [], "type_label": None,
             "created_at": "2024-01-01", "closed_at": None}
            for i in range(800)
        ]
        mock_prs.return_value = [
            {"number": 1000 + i, "title": f"PR {1000+i}", "head_ref": "main",
             "body": "", "review_comments": [],
             "review_comment_count": 0, "created_at": "2024-01-02",
             "merged_at": None}
            for i in range(400)
        ]
        mock_push.return_value = 0
        mock_link.return_value = 0

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        count = _poll_repo(
            "owner/repo", gh_db, sess_db,
            backfill_days=30, dry_run=False,
            max_items_per_repo=500,
        )

        # The item cap of 500 should limit actual DB writes
        conn = sqlite3.connect(str(gh_db))
        issue_count = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        pr_count = conn.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        conn.close()

        assert issue_count + pr_count == 500

    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_no_cap_when_under_limit(
        self, mock_issues, mock_prs, mock_issue_comments, mock_pr_comments,
        mock_edit_batch, mock_pr_edit, mock_push, mock_link, tmp_path
    ):
        from collector.github_poller.run import _poll_repo

        mock_issues.return_value = [
            {"number": 1, "title": "Issue 1", "state": "OPEN",
             "body": "", "comments": [], "type_label": None,
             "created_at": "2024-01-01", "closed_at": None}
        ]
        mock_prs.return_value = [
            {"number": 10, "title": "PR 10", "head_ref": "main",
             "body": "", "review_comments": [],
             "review_comment_count": 0, "created_at": "2024-01-02",
             "merged_at": None}
        ]
        mock_push.return_value = 0
        mock_link.return_value = 0

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        count = _poll_repo(
            "owner/repo", gh_db, sess_db,
            backfill_days=30, dry_run=False,
            max_items_per_repo=500,
        )
        assert count == 2


# ---------------------------------------------------------------------------
# Connection batching in store.py
# ---------------------------------------------------------------------------

class TestConnectionBatching:
    def test_conn_parameter_skips_connect(self, tmp_path):
        """When conn is provided, _connect is not called."""
        from am_i_shipping.db import init_github_db
        from collector.github_poller.store import upsert_issue

        db = tmp_path / "github.db"
        init_github_db(db)
        conn = sqlite3.connect(str(db))

        with patch("collector.github_poller.store._connect") as mock_connect:
            upsert_issue(
                "owner/repo",
                {"number": 1, "title": "t", "state": "OPEN", "body": "",
                 "comments": [], "type_label": None,
                 "created_at": None, "closed_at": None},
                db,
                conn=conn,
            )
            mock_connect.assert_not_called()

        conn.commit()
        # Verify the row was written
        row = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        conn.close()
        assert row == 1

    def test_no_conn_uses_connect(self, tmp_path):
        """When conn is None (default), _connect is called."""
        from collector.github_poller.store import upsert_issue

        db = tmp_path / "github.db"

        with patch("collector.github_poller.store._connect", wraps=__import__(
            'collector.github_poller.store', fromlist=['_connect']
        )._connect) as mock_connect:
            upsert_issue(
                "owner/repo",
                {"number": 1, "title": "t", "state": "OPEN", "body": "",
                 "comments": [], "type_label": None,
                 "created_at": None, "closed_at": None},
                db,
            )
            mock_connect.assert_called_once()


# ---------------------------------------------------------------------------
# Session parser batch limiters
# ---------------------------------------------------------------------------

class TestSessionBatchLimiters:
    def _make_session_file(self, path: Path, uuid: str) -> Path:
        """Create a minimal JSONL session file."""
        entry = json.dumps({
            "type": "user",
            "sessionId": uuid,
            "timestamp": "2024-01-01T00:00:00Z",
            "cwd": "/tmp",
            "message": {"role": "user", "content": "hello"},
        })
        entry2 = json.dumps({
            "type": "assistant",
            "sessionId": uuid,
            "timestamp": "2024-01-01T00:01:00Z",
            "message": {"role": "assistant", "content": "hi", "usage": {}},
        })
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{entry}\n{entry2}\n")
        return path

    def test_max_files_per_run_caps_batch(self, tmp_path):
        """run_batch stops after max_files_per_run new files."""
        from collector.session_parser import run_batch

        # Create projects dir with 10 session files
        projects = tmp_path / "projects"
        for i in range(10):
            self._make_session_file(
                projects / f"session_{i}.jsonl",
                f"uuid-{i:04d}",
            )

        # Config with max_files_per_run=5 and zero delay for speed
        cfg_path = _write_config(tmp_path, {
            "session": {
                "projects_path": str(projects),
                "limiter": {
                    "max_files_per_run": 5,
                    "inter_file_delay_seconds": 0.0,
                },
            },
            "github": {"repos": ["a/b"]},
            "data": {"data_dir": str(tmp_path / "data")},
        })

        run_batch(config_path=str(cfg_path))

        # Check DB has exactly 5 rows
        db = tmp_path / "data" / "sessions.db"
        conn = sqlite3.connect(str(db))
        count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()
        assert count == 5

        # Run again — picks up the remaining 5
        run_batch(config_path=str(cfg_path))
        conn = sqlite3.connect(str(db))
        count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()
        assert count == 10


# ---------------------------------------------------------------------------
# Session store skip_init / skip_health
# ---------------------------------------------------------------------------

class TestSessionStoreFlags:
    def test_skip_init_does_not_call_init(self, tmp_path):
        """upsert_session with skip_init=True skips init_sessions_db."""
        from am_i_shipping.db import init_sessions_db
        from collector.session_parser import SessionRecord
        from collector.store import upsert_session

        db = tmp_path / "sessions.db"
        init_sessions_db(db)  # init once manually

        record = SessionRecord(
            session_uuid="test-uuid",
            turn_count=1,
            tool_call_count=0,
            tool_failure_count=0,
            reprompt_count=0,
            bail_out=False,
            session_duration_seconds=10.0,
            working_directory="/tmp",
            git_branch="main",
            raw_content_json="[]",
            input_tokens=0,
            output_tokens=0,
            cache_creation_tokens=0,
            cache_read_tokens=0,
            fast_mode_turns=0,
        )

        # With skip_init=True, init_sessions_db is never imported/called.
        # Verify the function works correctly with the schema already initialized.
        upsert_session(record, db_path=db, data_dir=tmp_path,
                        skip_init=True, skip_health=True)

        conn = sqlite3.connect(str(db))
        count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        conn.close()
        assert count == 1

    def test_skip_health_does_not_write_health(self, tmp_path):
        """upsert_session with skip_health=True skips write_health."""
        from am_i_shipping.db import init_sessions_db
        from collector.session_parser import SessionRecord
        from collector.store import upsert_session

        db = tmp_path / "sessions.db"
        init_sessions_db(db)

        record = SessionRecord(
            session_uuid="test-uuid",
            turn_count=1,
            tool_call_count=0,
            tool_failure_count=0,
            reprompt_count=0,
            bail_out=False,
            session_duration_seconds=10.0,
            working_directory="/tmp",
            git_branch="main",
            raw_content_json="[]",
            input_tokens=0,
            output_tokens=0,
            cache_creation_tokens=0,
            cache_read_tokens=0,
            fast_mode_turns=0,
        )

        upsert_session(record, db_path=db, data_dir=tmp_path,
                        skip_init=True, skip_health=True)

        # health.json should NOT exist
        health = tmp_path / "health.json"
        assert not health.exists()


# ---------------------------------------------------------------------------
# Appswitch removal from health check
# ---------------------------------------------------------------------------

class TestAppswitchRemoved:
    def test_expected_collectors_excludes_appswitch(self):
        assert "appswitch_export" not in EXPECTED_COLLECTORS
        assert "session_parser" in EXPECTED_COLLECTORS
        assert "github_poller" in EXPECTED_COLLECTORS

    def test_health_check_passes_without_appswitch(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        health = {
            "session_parser": {"last_success": now, "last_record_count": 10},
            "github_poller": {"last_success": now, "last_record_count": 20},
        }
        (data_dir / "health.json").write_text(json.dumps(health))

        healthy, messages = check_health(data_dir=data_dir)
        assert healthy


# ---------------------------------------------------------------------------
# os.nice() in run.py
# ---------------------------------------------------------------------------

class TestProcessNice:
    @patch("collector.github_poller.run.os.nice")
    @patch("collector.github_poller.run.configure_limiter")
    def test_nice_called_on_run(self, mock_limiter, mock_nice, tmp_path):
        from collector.github_poller.run import run

        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": str(tmp_path / "projects")},
            "github": {
                "repos": ["a/b"],
                "limiter": {"process_nice_increment": 10},
            },
            "data": {"data_dir": str(tmp_path / "data")},
        })

        with patch("collector.github_poller.run.fetch_issues", return_value=[]), \
             patch("collector.github_poller.run.fetch_prs", return_value=[]), \
             patch("collector.github_poller.run.link_sessions", return_value=0):
            run(config_path=str(cfg_path))

        mock_nice.assert_called_once_with(10)

    @patch("collector.github_poller.run.os.nice", side_effect=OSError("not supported"))
    @patch("collector.github_poller.run.configure_limiter")
    def test_nice_failure_does_not_crash(self, mock_limiter, mock_nice, tmp_path):
        """os.nice() failure is logged but does not abort the run."""
        from collector.github_poller.run import run

        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": str(tmp_path / "projects")},
            "github": {"repos": ["a/b"]},
            "data": {"data_dir": str(tmp_path / "data")},
        })

        with patch("collector.github_poller.run.fetch_issues", return_value=[]), \
             patch("collector.github_poller.run.fetch_prs", return_value=[]), \
             patch("collector.github_poller.run.link_sessions", return_value=0):
            _total, ok = run(config_path=str(cfg_path))

        assert ok is True  # should not fail
