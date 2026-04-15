"""Tests for health_writer.py and health_check.py."""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from am_i_shipping.health_writer import write_health
from am_i_shipping.health_check import check_health


# ---------------------------------------------------------------------------
# health_writer tests
# ---------------------------------------------------------------------------

class TestWriteHealth:
    """write_health creates or updates health.json without corrupting other entries."""

    def test_creates_health_json(self, tmp_path):
        data_dir = tmp_path / "data"
        write_health("session_parser", 42, data_dir=data_dir)

        health_path = data_dir / "health.json"
        assert health_path.exists()

        data = json.loads(health_path.read_text())
        assert "session_parser" in data
        assert data["session_parser"]["last_record_count"] == 42
        assert "last_success" in data["session_parser"]

    def test_preserves_other_entries(self, tmp_path):
        data_dir = tmp_path / "data"
        write_health("session_parser", 10, data_dir=data_dir)
        write_health("github_poller", 20, data_dir=data_dir)

        data = json.loads((data_dir / "health.json").read_text())
        assert data["session_parser"]["last_record_count"] == 10
        assert data["github_poller"]["last_record_count"] == 20

    def test_updates_not_duplicates(self, tmp_path):
        data_dir = tmp_path / "data"
        write_health("session_parser", 10, data_dir=data_dir)
        write_health("session_parser", 42, data_dir=data_dir)

        data = json.loads((data_dir / "health.json").read_text())
        # Only one key for session_parser, updated to 42
        assert data["session_parser"]["last_record_count"] == 42
        assert len([k for k in data if k == "session_parser"]) == 1

    def test_creates_data_dir_if_missing(self, tmp_path):
        data_dir = tmp_path / "nonexistent" / "nested" / "data"
        write_health("session_parser", 1, data_dir=data_dir)

        assert (data_dir / "health.json").exists()


# ---------------------------------------------------------------------------
# health_check tests
# ---------------------------------------------------------------------------

class TestCheckHealth:
    """health_check.py exits 1 when stale or missing, 0 when all healthy."""

    def test_missing_health_json(self, tmp_path):
        healthy, messages = check_health(data_dir=tmp_path)
        assert not healthy
        assert any("not found" in m for m in messages)

    def test_all_healthy(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        now = datetime.now(timezone.utc).isoformat()
        health = {
            "session_parser": {"last_success": now, "last_record_count": 10},
            "github_poller": {"last_success": now, "last_record_count": 20},
        }
        (data_dir / "health.json").write_text(json.dumps(health))

        healthy, messages = check_health(data_dir=data_dir)
        assert healthy
        assert all(m.startswith("OK") for m in messages)

    def test_stale_collector(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        now = datetime.now(timezone.utc)
        stale = (now - timedelta(hours=49)).isoformat()
        fresh = now.isoformat()
        health = {
            "session_parser": {"last_success": stale, "last_record_count": 10},
            "github_poller": {"last_success": fresh, "last_record_count": 20},
        }
        (data_dir / "health.json").write_text(json.dumps(health))

        healthy, messages = check_health(data_dir=data_dir)
        assert not healthy
        assert any("stale" in m for m in messages)

    def test_missing_collector_entry(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        now = datetime.now(timezone.utc).isoformat()
        # Only one of two expected collectors present
        health = {
            "session_parser": {"last_success": now, "last_record_count": 10},
        }
        (data_dir / "health.json").write_text(json.dumps(health))

        healthy, messages = check_health(data_dir=data_dir)
        assert not healthy
        assert any("never reported" in m for m in messages)

    def test_importable_without_side_effects(self):
        """from health_check import check_health works without side effects."""
        # This test passing proves importability. check_health with a
        # non-existent dir should not create files or print to stdout.
        from am_i_shipping.health_check import check_health as imported_fn
        healthy, msgs = imported_fn(data_dir="/tmp/nonexistent_dir_test_12345")
        assert not healthy

    def test_corrupted_health_json(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "health.json").write_text("NOT JSON")

        healthy, messages = check_health(data_dir=data_dir)
        assert not healthy
        assert any("Failed to read" in m for m in messages)
