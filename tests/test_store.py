"""Tests for collector/store.py (C1-3).

Uses temporary SQLite databases; asserts no duplicate on second upsert
of same session_uuid.
"""

import json
import sqlite3
from pathlib import Path

import pytest

from collector.session_parser import SessionRecord, parse_session, process_session
from collector.store import upsert_session


FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample_session.jsonl"


def _make_record(**overrides) -> SessionRecord:
    """Create a minimal SessionRecord with optional overrides."""
    defaults = {
        "session_uuid": "test-uuid-1234",
        "turn_count": 5,
        "tool_call_count": 10,
        "tool_failure_count": 1,
        "reprompt_count": 0,
        "bail_out": False,
        "session_duration_seconds": 120.0,
        "working_directory": "/tmp/test",
        "git_branch": "main",
        "raw_content_json": "[]",
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_tokens": 0,
        "cache_read_tokens": 0,
        "fast_mode_turns": 0,
    }
    defaults.update(overrides)
    return SessionRecord(**defaults)


class TestUpsertSession:
    """Idempotent upsert into sessions.db."""

    def test_insert_creates_row(self, tmp_path):
        db_path = tmp_path / "sessions.db"
        record = _make_record()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_no_duplicate_on_second_upsert(self, tmp_path):
        """Running upsert twice with the same session_uuid produces exactly one row."""
        db_path = tmp_path / "sessions.db"
        record = _make_record()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_upsert_updates_values(self, tmp_path):
        """Second upsert with different values updates the row."""
        db_path = tmp_path / "sessions.db"
        record1 = _make_record(turn_count=5)
        upsert_session(record1, db_path=db_path, data_dir=tmp_path)

        record2 = _make_record(turn_count=10)
        upsert_session(record2, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT turn_count FROM sessions WHERE session_uuid = ?",
            ("test-uuid-1234",),
        ).fetchone()
        conn.close()
        assert row[0] == 10

    def test_health_json_written(self, tmp_path):
        """health.json should be updated after upsert."""
        db_path = tmp_path / "sessions.db"
        record = _make_record()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        health_path = tmp_path / "health.json"
        assert health_path.exists()

        data = json.loads(health_path.read_text())
        assert "session_parser" in data
        assert "last_success" in data["session_parser"]


class TestProcessSession:
    """End-to-end: process_session wires parse -> reprompt -> store."""

    def test_process_returns_uuid(self, tmp_path):
        db_path = tmp_path / "sessions.db"
        uuid = process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)
        assert uuid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_process_twice_one_row(self, tmp_path):
        """Running process_session twice produces exactly one row."""
        db_path = tmp_path / "sessions.db"
        process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)
        process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_health_json_valid_iso(self, tmp_path):
        """health.json contains valid ISO timestamp under session_parser.last_success."""
        db_path = tmp_path / "sessions.db"
        process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)

        health_path = tmp_path / "health.json"
        data = json.loads(health_path.read_text())
        ts = data["session_parser"]["last_success"]
        # Should parse without error
        from datetime import datetime

        datetime.fromisoformat(ts)


class TestSessionTimestampsColumns:
    """Epic #17 Sub-Issue 2 (#35): the two session timestamp columns roundtrip."""

    def test_stored_and_read_back(self, tmp_path):
        db_path = tmp_path / "sessions.db"
        record = _make_record(
            session_uuid="ts-uuid",
            session_started_at="2024-05-01T00:00:00+00:00",
            session_ended_at="2024-05-01T01:00:00+00:00",
        )
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT session_started_at, session_ended_at "
            "FROM sessions WHERE session_uuid = 'ts-uuid'"
        ).fetchone()
        conn.close()
        assert row == (
            "2024-05-01T00:00:00+00:00",
            "2024-05-01T01:00:00+00:00",
        )

    def test_second_upsert_overwrites_timestamps(self, tmp_path):
        db_path = tmp_path / "sessions.db"
        r1 = _make_record(
            session_uuid="ts-uuid",
            session_started_at="2024-05-01T00:00:00+00:00",
            session_ended_at="2024-05-01T01:00:00+00:00",
        )
        upsert_session(r1, db_path=db_path, data_dir=tmp_path)

        r2 = _make_record(
            session_uuid="ts-uuid",
            session_started_at="2024-06-01T00:00:00+00:00",
            session_ended_at="2024-06-01T02:00:00+00:00",
        )
        upsert_session(r2, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute(
            "SELECT session_started_at, session_ended_at "
            "FROM sessions WHERE session_uuid = 'ts-uuid'"
        ).fetchone()
        conn.close()
        assert row == (
            "2024-06-01T00:00:00+00:00",
            "2024-06-01T02:00:00+00:00",
        )


# ---------------------------------------------------------------------------
# Issue #66 — gh_events written to session_gh_events in github.db
# ---------------------------------------------------------------------------


def _make_record_with_events(**overrides) -> SessionRecord:
    """Build a SessionRecord carrying gh_events."""
    defaults = {
        "session_uuid": "gh-events-uuid",
        "turn_count": 2,
        "tool_call_count": 1,
        "tool_failure_count": 0,
        "reprompt_count": 0,
        "bail_out": False,
        "session_duration_seconds": 60.0,
        "working_directory": "/tmp/test",
        "git_branch": "main",
        "raw_content_json": "[]",
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_creation_tokens": 0,
        "cache_read_tokens": 0,
        "fast_mode_turns": 0,
        "gh_events": [
            {
                "event_type": "issue_comment",
                "repo": "a/b",
                "ref": "1",
                "url": "",
                "confidence": "high",
                "created_at": "2025-01-10T10:00:00Z",
            }
        ],
    }
    defaults.update(overrides)
    return SessionRecord(**defaults)


class TestUpsertSessionGhEvents:
    """upsert_session writes gh_events to session_gh_events in github.db."""

    def test_upsert_session_writes_gh_events(self, tmp_path):
        """upsert_session persists gh_events rows in session_gh_events."""
        from am_i_shipping.db import init_github_db

        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_events()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            rows = conn.execute(
                "SELECT session_uuid, event_type, repo, ref "
                "FROM session_gh_events"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1
        assert rows[0] == ("gh-events-uuid", "issue_comment", "a/b", "1")

    def test_upsert_session_gh_events_idempotent(self, tmp_path):
        """Calling upsert_session twice produces exactly one row (INSERT OR IGNORE)."""
        from am_i_shipping.db import init_github_db

        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_events()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM session_gh_events"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 1, f"Expected 1 row after double-upsert, got {count}"

    def test_upsert_session_empty_gh_events_writes_nothing(self, tmp_path):
        """upsert_session with empty gh_events does not write rows to session_gh_events."""
        from am_i_shipping.db import init_github_db

        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_events(gh_events=[])
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM session_gh_events"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 0
