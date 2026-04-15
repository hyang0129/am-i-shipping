"""Tests for init_db.py."""

import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.config_loader import Config, SessionConfig, GitHubConfig, AppSwitchConfig, DataConfig
from am_i_shipping.db import init_all


def _make_config(tmp_path: Path) -> Config:
    data_dir = tmp_path / "data"
    return Config(
        session=SessionConfig(projects_path="/fake"),
        github=GitHubConfig(repos=["a/b"]),
        appswitch=AppSwitchConfig(),
        data=DataConfig(data_dir=str(data_dir)),
    )


class TestInitDb:
    """init_db.py is safe to run twice; no errors, no duplicate tables."""

    def test_creates_all_databases(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        data_dir = tmp_path / "data"
        assert (data_dir / "sessions.db").exists()
        assert (data_dir / "github.db").exists()
        assert (data_dir / "appswitch.db").exists()

    def test_idempotent_double_run(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)
        # Second run should not error
        init_all(config)

        data_dir = tmp_path / "data"
        assert (data_dir / "sessions.db").exists()

    def test_recreates_after_deletion(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        data_dir = tmp_path / "data"
        # Delete and recreate
        for db in ["sessions.db", "github.db", "appswitch.db"]:
            (data_dir / db).unlink()

        init_all(config)

        assert (data_dir / "sessions.db").exists()
        assert (data_dir / "github.db").exists()
        assert (data_dir / "appswitch.db").exists()

    def test_sessions_db_schema(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "sessions.db"))
        cursor = conn.execute("PRAGMA table_info(sessions)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()

        expected = {
            "session_uuid", "turn_count", "tool_call_count",
            "tool_failure_count", "reprompt_count", "bail_out",
            "session_duration_seconds", "working_directory",
            "git_branch", "raw_content_json", "created_at",
        }
        assert expected.issubset(columns)

    def test_github_db_schema(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "github.db"))
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()

        expected = {"issues", "pull_requests", "pr_issues", "pr_sessions", "poll_cursor"}
        assert expected.issubset(tables)

    def test_appswitch_db_schema(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "appswitch.db"))
        cursor = conn.execute("PRAGMA table_info(app_events)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()

        expected = {
            "timestamp_bucket", "window_hash", "app_name",
            "window_title", "duration_seconds",
        }
        assert expected.issubset(columns)
