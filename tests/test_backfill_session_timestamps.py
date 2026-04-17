"""Tests for am_i_shipping/scripts/backfill_session_timestamps.py.

Epic #17 Sub-Issue 2 (#35). Verifies the one-shot backfill populates the
two timestamp columns for rows that pre-date the schema addition, without
touching any other column.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.db import init_sessions_db
from am_i_shipping.scripts.backfill_session_timestamps import (
    _build_session_index,
    _extract_timestamps,
    backfill,
)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, session_uuid: str, timestamps: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for i, ts in enumerate(timestamps):
        lines.append(json.dumps({
            "sessionId": session_uuid,
            "type": "user" if i % 2 == 0 else "assistant",
            "timestamp": ts,
            "message": {"role": "user", "content": "x"},
        }))
    path.write_text("\n".join(lines) + "\n")


def _insert_bare_session(db: Path, session_uuid: str) -> None:
    """Insert a session row with NULL timestamp columns."""
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            """
            INSERT INTO sessions (
                session_uuid, turn_count, tool_call_count, tool_failure_count,
                reprompt_count, bail_out, session_duration_seconds,
                working_directory, git_branch, raw_content_json,
                input_tokens, output_tokens, cache_creation_tokens,
                cache_read_tokens, fast_mode_turns
            ) VALUES (?, 1, 0, 0, 0, 0, 0.0, NULL, NULL, '[]', 0, 0, 0, 0, 0)
            """,
            (session_uuid,),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestSessionIndex:
    def test_indexes_session_files(self, tmp_path):
        projects = tmp_path / "projects"
        _write_jsonl(
            projects / "proj-1" / "sess-a.jsonl",
            "aaaa-1111",
            ["2024-01-15T10:00:00Z", "2024-01-15T10:30:00Z"],
        )
        _write_jsonl(
            projects / "proj-2" / "sess-b.jsonl",
            "bbbb-2222",
            ["2024-02-15T10:00:00Z"],
        )
        # Subagent file — should be excluded.
        _write_jsonl(
            projects / "proj-1" / "subagents" / "sub.jsonl",
            "cccc-3333",
            ["2024-03-15T10:00:00Z"],
        )

        index = _build_session_index(projects)
        assert "aaaa-1111" in index
        assert "bbbb-2222" in index
        assert "cccc-3333" not in index
        assert index["aaaa-1111"].name == "sess-a.jsonl"

    def test_missing_projects_path_returns_empty(self, tmp_path):
        assert _build_session_index(tmp_path / "nope") == {}


class TestExtractTimestamps:
    def test_first_and_last(self, tmp_path):
        f = tmp_path / "sess.jsonl"
        _write_jsonl(
            f, "uid",
            ["2024-01-15T10:00:00Z", "2024-01-15T10:05:00Z", "2024-01-15T10:30:00Z"],
        )
        stamps = _extract_timestamps(f)
        assert stamps is not None
        first, last = stamps
        assert first.startswith("2024-01-15T10:00")
        assert last.startswith("2024-01-15T10:30")

    def test_no_timestamps_returns_none(self, tmp_path):
        f = tmp_path / "sess.jsonl"
        f.write_text(json.dumps({"sessionId": "x"}) + "\n")
        assert _extract_timestamps(f) is None


# ---------------------------------------------------------------------------
# End-to-end backfill
# ---------------------------------------------------------------------------


class TestBackfill:
    def test_populates_timestamps_without_touching_other_columns(self, tmp_path):
        projects = tmp_path / "projects"
        _write_jsonl(
            projects / "p" / "sess.jsonl",
            "uuid-aaaa",
            ["2024-05-01T00:00:00Z", "2024-05-01T01:00:00Z"],
        )

        db = tmp_path / "sessions.db"
        init_sessions_db(db)
        _insert_bare_session(db, "uuid-aaaa")

        # Capture the "other" column values before running the backfill.
        conn = sqlite3.connect(str(db))
        before = conn.execute(
            "SELECT turn_count, raw_content_json, input_tokens "
            "FROM sessions WHERE session_uuid = 'uuid-aaaa'"
        ).fetchone()
        conn.close()

        updated, skipped, errored = backfill(db, projects)
        assert updated == 1
        assert skipped == 0
        assert errored == 0

        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT session_started_at, session_ended_at, "
            "turn_count, raw_content_json, input_tokens "
            "FROM sessions WHERE session_uuid = 'uuid-aaaa'"
        ).fetchone()
        conn.close()

        assert row[0].startswith("2024-05-01T00:00")
        assert row[1].startswith("2024-05-01T01:00")
        # Every other column unchanged.
        assert (row[2], row[3], row[4]) == before

    def test_already_populated_rows_are_skipped(self, tmp_path):
        projects = tmp_path / "projects"
        _write_jsonl(
            projects / "p" / "sess.jsonl",
            "uuid-aaaa",
            ["2024-05-01T00:00:00Z", "2024-05-01T01:00:00Z"],
        )

        db = tmp_path / "sessions.db"
        init_sessions_db(db)

        # Insert a row that already has timestamps.
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            INSERT INTO sessions (
                session_uuid, turn_count, tool_call_count, tool_failure_count,
                reprompt_count, bail_out, session_duration_seconds,
                working_directory, git_branch, raw_content_json,
                input_tokens, output_tokens, cache_creation_tokens,
                cache_read_tokens, fast_mode_turns,
                session_started_at, session_ended_at
            ) VALUES (?, 1, 0, 0, 0, 0, 0.0, NULL, NULL, '[]', 0, 0, 0, 0, 0,
                      ?, ?)
            """,
            ("uuid-aaaa", "pre-existing", "pre-existing"),
        )
        conn.commit()
        conn.close()

        updated, skipped, errored = backfill(db, projects)
        assert updated == 0
        assert skipped == 0
        assert errored == 0

    def test_missing_jsonl_skipped_not_errored(self, tmp_path):
        projects = tmp_path / "projects"
        projects.mkdir(parents=True)

        db = tmp_path / "sessions.db"
        init_sessions_db(db)
        _insert_bare_session(db, "orphan-uuid")

        updated, skipped, errored = backfill(db, projects)
        assert updated == 0
        assert skipped == 1
        assert errored == 0

    def test_dry_run_does_not_write(self, tmp_path):
        projects = tmp_path / "projects"
        _write_jsonl(
            projects / "p" / "sess.jsonl",
            "uuid-aaaa",
            ["2024-05-01T00:00:00Z", "2024-05-01T01:00:00Z"],
        )

        db = tmp_path / "sessions.db"
        init_sessions_db(db)
        _insert_bare_session(db, "uuid-aaaa")

        updated, skipped, errored = backfill(db, projects, dry_run=True)
        assert updated == 1

        conn = sqlite3.connect(str(db))
        row = conn.execute(
            "SELECT session_started_at, session_ended_at "
            "FROM sessions WHERE session_uuid = 'uuid-aaaa'"
        ).fetchone()
        conn.close()
        # Still NULL — dry-run did not write.
        assert row == (None, None)

    def test_limit_caps_rows(self, tmp_path):
        projects = tmp_path / "projects"
        db = tmp_path / "sessions.db"
        init_sessions_db(db)

        for i in range(5):
            uid = f"uuid-{i}"
            _write_jsonl(
                projects / "p" / f"{uid}.jsonl",
                uid,
                ["2024-05-01T00:00:00Z", "2024-05-01T01:00:00Z"],
            )
            _insert_bare_session(db, uid)

        updated, skipped, errored = backfill(db, projects, limit=2)
        assert updated == 2
