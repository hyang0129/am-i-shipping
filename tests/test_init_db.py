"""Tests for init_db.py."""

import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.config_loader import Config, SessionConfig, GitHubConfig, AppSwitchConfig, DataConfig
from am_i_shipping.db import (
    EXPECTED_GITHUB_TABLES,
    EXPECTED_SESSIONS_COLUMNS,
    assert_schema,
    init_all,
)


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

        expected = {
            "issues", "pull_requests", "pr_issues", "pr_sessions", "poll_cursor",
            "issue_body_edits", "issue_comment_edits",
            "pr_body_edits", "pr_review_comment_edits",
        }
        assert expected.issubset(tables)

    def test_issues_has_updated_at_column(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "github.db"))
        cursor = conn.execute("PRAGMA table_info(issues)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()
        assert "updated_at" in columns

    def test_pull_requests_has_updated_at_column(self, tmp_path):
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "github.db"))
        cursor = conn.execute("PRAGMA table_info(pull_requests)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()
        assert "updated_at" in columns

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

    # -----------------------------------------------------------------
    # Epic #17 — Sub-Issue 1 additions
    # -----------------------------------------------------------------

    def test_sessions_has_timestamp_columns(self, tmp_path):
        """session_started_at / session_ended_at are present after migration."""
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "sessions.db"))
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        conn.close()
        assert "session_started_at" in columns
        assert "session_ended_at" in columns

    def test_github_db_has_synthesis_tables(self, tmp_path):
        """All five new synthesis tables live in github.db."""
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

        expected = {
            "commits",
            "timeline_events",
            "graph_nodes",
            "graph_edges",
            "units",
        }
        assert expected.issubset(tables), (
            f"Missing synthesis tables: {expected - tables}"
        )

    def test_units_has_expected_columns(self, tmp_path):
        """units is the append-only week-indexed table (ADR Decision 5)."""
        config = _make_config(tmp_path)
        init_all(config)

        conn = sqlite3.connect(str(tmp_path / "data" / "github.db"))
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(units)").fetchall()
        }
        conn.close()

        expected = {
            "week_start", "unit_id", "root_node_type", "root_node_id",
            "elapsed_days", "dark_time_pct", "total_reprompts",
            "review_cycles", "status",
        }
        assert expected.issubset(columns)

    def test_assert_schema_passes_on_fresh_db(self, tmp_path):
        """init_*_db() already call assert_schema(); a direct call must also pass."""
        config = _make_config(tmp_path)
        init_all(config)

        # Both should raise nothing.
        assert_schema(
            tmp_path / "data" / "sessions.db",
            {"sessions": EXPECTED_SESSIONS_COLUMNS},
        )
        assert_schema(
            tmp_path / "data" / "github.db", EXPECTED_GITHUB_TABLES
        )

    def test_assert_schema_raises_on_missing_column(self, tmp_path):
        """assert_schema raises RuntimeError naming the missing column."""
        bad_db = tmp_path / "bad.db"
        conn = sqlite3.connect(str(bad_db))
        # Legacy sessions schema — missing the new timestamp columns.
        conn.execute(
            """
            CREATE TABLE sessions (
                session_uuid TEXT PRIMARY KEY,
                turn_count INTEGER
            )
            """
        )
        conn.commit()
        conn.close()

        with pytest.raises(RuntimeError, match="session_started_at"):
            assert_schema(bad_db, {"sessions": EXPECTED_SESSIONS_COLUMNS})

    def test_assert_schema_raises_on_missing_table(self, tmp_path):
        """assert_schema raises when a whole table is absent."""
        empty_db = tmp_path / "empty.db"
        conn = sqlite3.connect(str(empty_db))
        conn.commit()
        conn.close()

        with pytest.raises(RuntimeError, match="units"):
            assert_schema(empty_db, {"units": {"week_start", "unit_id"}})

    def test_migration_replay_on_legacy_sessions_db(self, tmp_path):
        """Running init_all over an old sessions.db adds the new columns
        without dropping the pre-existing row (NULL in new columns)."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        legacy = data_dir / "sessions.db"

        # Hand-craft a legacy sessions schema (pre Sub-Issue 1) with one row.
        conn = sqlite3.connect(str(legacy))
        conn.execute(
            """
            CREATE TABLE sessions (
                session_uuid    TEXT PRIMARY KEY,
                turn_count      INTEGER,
                tool_call_count INTEGER,
                tool_failure_count INTEGER,
                reprompt_count  INTEGER,
                bail_out        INTEGER DEFAULT 0,
                session_duration_seconds REAL,
                working_directory TEXT,
                git_branch      TEXT,
                raw_content_json TEXT,
                input_tokens    INTEGER DEFAULT 0,
                output_tokens   INTEGER DEFAULT 0,
                cache_creation_tokens INTEGER DEFAULT 0,
                cache_read_tokens INTEGER DEFAULT 0,
                fast_mode_turns INTEGER DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            "INSERT INTO sessions (session_uuid, turn_count) VALUES (?, ?)",
            ("legacy-uuid-1", 7),
        )
        conn.commit()
        conn.close()

        # Run init_all; the migration path should add the new columns.
        init_all(_make_config(tmp_path))

        conn = sqlite3.connect(str(legacy))
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        assert "session_started_at" in columns
        assert "session_ended_at" in columns

        row = conn.execute(
            "SELECT turn_count, session_started_at, session_ended_at "
            "FROM sessions WHERE session_uuid = ?",
            ("legacy-uuid-1",),
        ).fetchone()
        conn.close()
        assert row is not None, "legacy row disappeared after migration"
        assert row[0] == 7
        # New columns must be NULL for historical rows.
        assert row[1] is None
        assert row[2] is None


class TestGoldenFixture:
    """The committed synthesis fixture is loadable and has the expected units."""

    FIXTURE = Path(__file__).resolve().parent / "fixtures" / "synthesis" / "golden.sqlite"

    def test_golden_fixture_loadable(self):
        assert self.FIXTURE.exists(), (
            f"Golden fixture not committed at {self.FIXTURE}"
        )
        conn = sqlite3.connect(str(self.FIXTURE))
        try:
            unit_ids = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT unit_id FROM units"
                ).fetchall()
            }
        finally:
            conn.close()
        # Three distinct unit topologies: multi, abandoned, singleton.
        assert len(unit_ids) == 3, f"expected 3 distinct unit_ids, got {unit_ids}"

    def test_golden_fixture_schema_matches_live_db(self, tmp_path):
        """Every expected github.db table is present in the fixture."""
        assert_schema(self.FIXTURE, EXPECTED_GITHUB_TABLES)
