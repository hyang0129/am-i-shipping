"""Tests for hook and batch modes (C1-4).

Uses temp directories to test batch file discovery and skip logic.
"""

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from collector.session_parser import (
    SessionParseError,
    _discover_session_files,
    _extract_uuid_from_file,
    _get_existing_uuids,
    process_session,
    run_batch,
)


FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample_session.jsonl"
REPROMPT = FIXTURES / "reprompt_session.jsonl"


def _create_config(tmp_path: Path, projects_path: Path) -> Path:
    """Create a minimal config.yaml for testing."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
session:
  projects_path: "{projects_path}"
  reprompt_threshold: 3
github:
  repos: ["test/repo"]
data:
  data_dir: "{tmp_path / 'data'}"
"""
    )
    return config_path


def _make_session_file(
    dest: Path, session_uuid: str, template: Path = SAMPLE
) -> Path:
    """Copy a fixture and rewrite its sessionId."""
    lines = []
    with open(template, "r", encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line)
            if "sessionId" in entry:
                entry["sessionId"] = session_uuid
            lines.append(json.dumps(entry))

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines) + "\n")
    return dest


class TestDiscoverSessionFiles:
    """Batch mode file discovery."""

    def test_finds_jsonl_files(self, tmp_path):
        projects = tmp_path / "projects"
        projects.mkdir()
        (projects / "sess1.jsonl").write_text("{}")
        (projects / "sess2.jsonl").write_text("{}")

        files = _discover_session_files(projects)
        assert len(files) == 2

    def test_excludes_subagents(self, tmp_path):
        projects = tmp_path / "projects"
        sub = projects / "proj" / "subagents"
        sub.mkdir(parents=True)
        (projects / "proj" / "main.jsonl").write_text("{}")
        (sub / "agent.jsonl").write_text("{}")

        files = _discover_session_files(projects)
        assert len(files) == 1

    def test_empty_dir(self, tmp_path):
        projects = tmp_path / "projects"
        projects.mkdir()
        files = _discover_session_files(projects)
        assert len(files) == 0

    def test_nonexistent_dir(self, tmp_path):
        files = _discover_session_files(tmp_path / "nope")
        assert len(files) == 0


class TestHookMode:
    """Hook mode: process single file, idempotent."""

    def test_hook_second_call_no_duplicate(self, tmp_path):
        """Second hook call on same fixture exits without duplicate row."""
        db_path = tmp_path / "sessions.db"
        uuid1 = process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)
        uuid2 = process_session(SAMPLE, db_path=db_path, data_dir=tmp_path)

        assert uuid1 == uuid2

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM sessions").fetchall()
        conn.close()
        assert len(rows) == 1


class TestBatchMode:
    """Batch mode: enumerate, skip known, process new."""

    def test_batch_processes_new_skips_known(self, tmp_path):
        """3 files in dir, 1 already in DB -> processes 2, DB has 3 rows."""
        projects = tmp_path / "projects"
        projects.mkdir()

        # Create 3 session files with different UUIDs
        _make_session_file(projects / "s1.jsonl", "uuid-aaa")
        _make_session_file(projects / "s2.jsonl", "uuid-bbb")
        _make_session_file(projects / "s3.jsonl", "uuid-ccc")

        config_path = _create_config(tmp_path, projects)
        data_dir = tmp_path / "data"
        db_path = data_dir / "sessions.db"

        # Pre-process one file to simulate "already in DB"
        process_session(
            projects / "s1.jsonl", db_path=db_path, data_dir=data_dir
        )

        # Verify 1 row exists
        conn = sqlite3.connect(str(db_path))
        assert len(conn.execute("SELECT * FROM sessions").fetchall()) == 1
        conn.close()

        # Run batch
        run_batch(config_path=str(config_path))

        # Should now have 3 rows total
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute("SELECT * FROM sessions").fetchall()
        conn.close()
        assert len(rows) == 3

    def test_batch_empty_dir(self, tmp_path):
        """Batch mode on empty directory exits 0 without error."""
        projects = tmp_path / "projects"
        projects.mkdir()

        config_path = _create_config(tmp_path, projects)

        # Should not raise
        run_batch(config_path=str(config_path))

    def test_health_json_updated_after_batch(self, tmp_path):
        """health.json is updated after batch mode."""
        projects = tmp_path / "projects"
        projects.mkdir()
        _make_session_file(projects / "s1.jsonl", "uuid-batch-health")

        config_path = _create_config(tmp_path, projects)
        run_batch(config_path=str(config_path))

        health_path = tmp_path / "data" / "health.json"
        assert health_path.exists()
        data = json.loads(health_path.read_text())
        assert "session_parser" in data

    def test_health_json_updated_after_empty_batch(self, tmp_path):
        """health.json is updated even when batch finds no files."""
        projects = tmp_path / "projects"
        projects.mkdir()

        config_path = _create_config(tmp_path, projects)
        run_batch(config_path=str(config_path))

        health_path = tmp_path / "data" / "health.json"
        assert health_path.exists()


class TestExtractUuid:
    """Quick UUID extraction for skip logic."""

    def test_extracts_uuid(self):
        uuid = _extract_uuid_from_file(SAMPLE)
        assert uuid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_returns_none_for_bad_file(self, tmp_path):
        bad = tmp_path / "bad.jsonl"
        bad.write_text("not json\n")
        uuid = _extract_uuid_from_file(bad)
        assert uuid is None
