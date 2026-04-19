"""Tests for session_gh_events extraction and persistence (Issue #66).

Tests for:
  - _extract_gh_events: extracts events from tool_use Bash blocks
  - parse_session: gh_events populated; raw_content_json still stripped
  - upsert_session: writes gh_events rows to session_gh_events in github.db
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from collector.session_parser import SessionRecord, _extract_gh_events, parse_session
from collector.store import upsert_session
from am_i_shipping.db import init_github_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entry(
    session_id: str,
    entry_type: str,
    content: list,
    timestamp: str = "2025-01-10T10:00:00Z",
) -> dict:
    """Build a JSONL entry dict in the Claude Code session format."""
    return {
        "type": entry_type,
        "sessionId": session_id,
        "timestamp": timestamp,
        "message": {
            "role": entry_type,
            "content": content,
        },
    }


def _make_tool_use_block(
    tool_use_id: str, command: str
) -> dict:
    """Build a tool_use Bash block."""
    return {
        "type": "tool_use",
        "id": tool_use_id,
        "name": "Bash",
        "input": {"command": command},
    }


def _make_tool_result_block(tool_use_id: str, text: str) -> dict:
    """Build a tool_result block responding to a tool_use_id."""
    return {
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": [{"type": "text", "text": text}],
    }


def _make_text_block(text: str) -> dict:
    return {"type": "text", "text": text}


# ---------------------------------------------------------------------------
# Tests for _extract_gh_events
# ---------------------------------------------------------------------------


class TestExtractGhEvents:
    def test_issue_create_upgraded_from_tool_result(self):
        """gh issue create with tool_result stdout upgrades ref from pending to number."""
        entry = {
            "type": "assistant",
            "timestamp": "2025-01-10T10:00:00Z",
            "message": {
                "role": "assistant",
                "content": [
                    _make_tool_use_block(
                        "tu-1",
                        "gh issue create --repo foo/bar --title x --body y",
                    ),
                    _make_tool_result_block(
                        "tu-1",
                        "https://github.com/foo/bar/issues/42\n",
                    ),
                ],
            },
        }
        events = _extract_gh_events(entry, working_directory=None)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "issue_create"
        assert ev["repo"] == "foo/bar"
        assert ev["ref"] == "42"
        assert ev["url"] == "https://github.com/foo/bar/issues/42"
        assert ev["confidence"] == "high"

    def test_issue_create_pending_when_no_result(self):
        """gh issue create without a tool_result leaves ref as 'pending'."""
        entry = {
            "type": "assistant",
            "timestamp": "2025-01-10T10:00:00Z",
            "message": {
                "role": "assistant",
                "content": [
                    _make_tool_use_block(
                        "tu-2",
                        "gh issue create --repo foo/bar --title pending-test",
                    ),
                ],
            },
        }
        events = _extract_gh_events(entry, working_directory=None)
        assert len(events) == 1
        assert events[0]["event_type"] == "issue_create"
        assert events[0]["ref"] == "pending"
        assert events[0]["confidence"] == "medium"

    def test_issue_comment_extracts_ref_and_repo(self):
        """gh issue comment N --repo owner/repo extracts number as ref."""
        entry = {
            "type": "assistant",
            "timestamp": "2025-01-10T10:00:00Z",
            "message": {
                "role": "assistant",
                "content": [
                    _make_tool_use_block(
                        "tu-3",
                        "gh issue comment 7 --repo foo/bar --body hi",
                    ),
                ],
            },
        }
        events = _extract_gh_events(entry, working_directory=None)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "issue_comment"
        assert ev["repo"] == "foo/bar"
        assert ev["ref"] == "7"

    def test_pr_create_upgraded_from_tool_result(self):
        """gh pr create with tool_result stdout containing PR URL upgrades ref."""
        entry = {
            "type": "assistant",
            "timestamp": "2025-01-10T10:00:00Z",
            "message": {
                "role": "assistant",
                "content": [
                    _make_tool_use_block(
                        "tu-4",
                        "gh pr create --repo foo/bar --title my-pr --body desc",
                    ),
                    _make_tool_result_block(
                        "tu-4",
                        "https://github.com/foo/bar/pull/99\n",
                    ),
                ],
            },
        }
        events = _extract_gh_events(entry, working_directory=None)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "pr_create"
        assert ev["repo"] == "foo/bar"
        assert ev["ref"] == "99"
        assert ev["confidence"] == "high"

    def test_git_push_captured(self):
        """git push origin feature-x emits a git_push event."""
        entry = {
            "type": "assistant",
            "timestamp": "2025-01-10T10:00:00Z",
            "message": {
                "role": "assistant",
                "content": [
                    _make_tool_use_block(
                        "tu-5",
                        "git push origin feature-x",
                    ),
                ],
            },
        }
        events = _extract_gh_events(entry, working_directory=None)
        assert len(events) == 1
        ev = events[0]
        assert ev["event_type"] == "git_push"
        # branch ref should be captured if possible
        assert ev["ref"] == "feature-x"

    def test_non_user_assistant_entry_returns_empty(self):
        """Entries with type other than user/assistant return empty list."""
        entry = {
            "type": "system",
            "message": {
                "content": [
                    _make_tool_use_block("tu-x", "gh issue create --repo a/b --title t"),
                ],
            },
        }
        assert _extract_gh_events(entry, working_directory=None) == []

    def test_string_content_returns_empty(self):
        """Entries with string content (not a list) return empty list."""
        entry = {
            "type": "assistant",
            "message": {"content": "just a string"},
        }
        assert _extract_gh_events(entry, working_directory=None) == []


# ---------------------------------------------------------------------------
# Tests for parse_session pipeline: gh_events populated, raw_content stripped
# ---------------------------------------------------------------------------


def _build_session_jsonl(tmp_path: Path, session_uuid: str) -> Path:
    """Write a minimal session JSONL with gh CLI tool_use turns."""
    # Entry 1: tool_use for gh issue create with corresponding tool_result (in same assistant entry)
    entry_issue_create = {
        "type": "assistant",
        "sessionId": session_uuid,
        "timestamp": "2025-01-10T10:00:00Z",
        "message": {
            "role": "assistant",
            "content": [
                _make_tool_use_block("tu-ic", "gh issue create --repo foo/bar --title x --body y"),
                _make_tool_result_block("tu-ic", "https://github.com/foo/bar/issues/42\n"),
            ],
        },
    }
    # Entry 2: user turn providing tool_result for pr comment (tool_use in user entry)
    entry_pr_comment = {
        "type": "user",
        "sessionId": session_uuid,
        "timestamp": "2025-01-10T10:01:00Z",
        "message": {
            "role": "user",
            "content": [
                _make_tool_use_block("tu-pc", "gh pr comment 7 --repo foo/bar --body reply"),
            ],
        },
    }
    # Entry 3: assistant turn with git push
    entry_git_push = {
        "type": "assistant",
        "sessionId": session_uuid,
        "timestamp": "2025-01-10T10:02:00Z",
        "message": {
            "role": "assistant",
            "content": [
                _make_tool_use_block("tu-gp", "git push origin feature-x"),
            ],
        },
    }
    # Entry 4: user text turn (needed so turn_count >= 1)
    entry_user_text = {
        "type": "user",
        "sessionId": session_uuid,
        "timestamp": "2025-01-10T10:03:00Z",
        "message": {
            "role": "user",
            "content": [_make_text_block("Please review the changes.")],
        },
    }

    filepath = tmp_path / f"{session_uuid}.jsonl"
    with filepath.open("w") as f:
        for entry in [entry_issue_create, entry_pr_comment, entry_git_push, entry_user_text]:
            f.write(json.dumps(entry) + "\n")
    return filepath


class TestParseSessionGhEvents:
    SESSION_UUID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"

    def test_gh_events_populated(self, tmp_path):
        """parse_session populates gh_events from tool_use Bash blocks."""
        path = _build_session_jsonl(tmp_path, self.SESSION_UUID)
        record = parse_session(path)
        assert isinstance(record.gh_events, list)
        assert len(record.gh_events) >= 1
        event_types = {ev["event_type"] for ev in record.gh_events}
        assert "issue_create" in event_types

    def test_git_push_event_present(self, tmp_path):
        """parse_session captures git_push events."""
        path = _build_session_jsonl(tmp_path, self.SESSION_UUID)
        record = parse_session(path)
        event_types = {ev["event_type"] for ev in record.gh_events}
        assert "git_push" in event_types

    def test_raw_content_json_strips_tool_use_blocks(self, tmp_path):
        """raw_content_json must NOT contain tool_use blocks."""
        path = _build_session_jsonl(tmp_path, self.SESSION_UUID)
        record = parse_session(path)
        turns = json.loads(record.raw_content_json)
        for turn in turns:
            content = turn.get("content")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        assert block.get("type") not in ("thinking", "tool_use", "tool_result"), (
                            f"thinking/tool_use/tool_result block leaked into raw_content_json: {block}"
                        )
            elif isinstance(content, str):
                # Pure text turns — allowed
                pass

    def test_raw_content_json_has_user_text_turn(self, tmp_path):
        """The user text turn should survive stripping and appear in raw_content_json."""
        path = _build_session_jsonl(tmp_path, self.SESSION_UUID)
        record = parse_session(path)
        turns = json.loads(record.raw_content_json)
        # At least one turn with text content from the user message
        has_text = any(
            "Please review the changes." in (
                json.dumps(turn.get("content", ""))
            )
            for turn in turns
        )
        assert has_text, "User text turn should be present in raw_content_json"


# ---------------------------------------------------------------------------
# Tests for upsert_session + github.db session_gh_events persistence
# ---------------------------------------------------------------------------


def _make_record_with_gh_events(**overrides) -> SessionRecord:
    """Build a SessionRecord with gh_events set."""
    defaults = {
        "session_uuid": "test-uuid-gh-events",
        "turn_count": 3,
        "tool_call_count": 2,
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


class TestUpsertSessionGhEventsPersistence:
    def test_upsert_session_writes_gh_events(self, tmp_path):
        """upsert_session writes gh_events rows to session_gh_events in github.db."""
        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_gh_events()
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            rows = conn.execute(
                "SELECT session_uuid, event_type, repo, ref, url, confidence "
                "FROM session_gh_events"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1
        assert rows[0] == ("test-uuid-gh-events", "issue_comment", "a/b", "1", "", "high")

    def test_upsert_session_gh_events_idempotent(self, tmp_path):
        """Calling upsert_session twice produces exactly one row (INSERT OR IGNORE)."""
        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_gh_events()
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

    def test_upsert_session_no_gh_events_is_noop(self, tmp_path):
        """upsert_session with empty gh_events does not write to session_gh_events."""
        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        record = _make_record_with_gh_events(gh_events=[])
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM session_gh_events"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 0

    def test_upsert_session_multiple_events_all_written(self, tmp_path):
        """upsert_session writes all gh_events rows, not just the first."""
        db_path = tmp_path / "sessions.db"
        gh_db_path = tmp_path / "github.db"
        init_github_db(gh_db_path)

        events = [
            {
                "event_type": "issue_comment",
                "repo": "a/b",
                "ref": "1",
                "url": "",
                "confidence": "high",
                "created_at": "2025-01-10T10:00:00Z",
            },
            {
                "event_type": "git_push",
                "repo": "",
                "ref": "feature-x",
                "url": "",
                "confidence": "medium",
                "created_at": "2025-01-10T10:01:00Z",
            },
        ]
        record = _make_record_with_gh_events(gh_events=events)
        upsert_session(record, db_path=db_path, data_dir=tmp_path)

        conn = sqlite3.connect(str(gh_db_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM session_gh_events"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 2
