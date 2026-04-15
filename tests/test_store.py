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
