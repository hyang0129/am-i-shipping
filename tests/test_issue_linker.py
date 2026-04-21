"""Tests for collector/github_poller/issue_linker.py.

Uses temporary SQLite databases; no live network calls.
"""

from __future__ import annotations

import sqlite3

import pytest

from am_i_shipping.db import init_github_db
from collector.github_poller.issue_linker import link_issues


def _insert_gh_event(conn, session_uuid, event_type, repo, ref):
    """Insert a row into session_gh_events."""
    conn.execute(
        """
        INSERT OR IGNORE INTO session_gh_events
            (session_uuid, event_type, repo, ref)
        VALUES (?, ?, ?, ?)
        """,
        (session_uuid, event_type, repo, ref),
    )
    conn.commit()


class TestIssueLinker:
    def test_no_sessions_db_returns_zero(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "nonexistent.db"
        init_github_db(gh_db)
        assert link_issues("owner/repo", gh_db, sess_db) == 0

    def test_no_gh_events_returns_zero(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()  # file exists but no events seeded

        init_github_db(gh_db)

        result = link_issues("owner/repo", gh_db, sess_db)
        assert result == 0

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 0

    def test_issue_create_event_creates_link(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-1", "issue_create", "owner/repo", "42")
        conn.close()

        count = link_issues("owner/repo", gh_db, sess_db)
        assert count >= 1

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT repo, issue_number, session_uuid FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0] == ("owner/repo", 42, "sess-1")

    def test_issue_comment_event_creates_link(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-2", "issue_comment", "owner/repo", "7")
        conn.close()

        count = link_issues("owner/repo", gh_db, sess_db)
        assert count >= 1

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT repo, issue_number, session_uuid FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 1
        assert rows[0] == ("owner/repo", 7, "sess-2")

    def test_pending_ref_skipped(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-3", "issue_create", "owner/repo", "pending")
        conn.close()

        count = link_issues("owner/repo", gh_db, sess_db)
        assert count == 0

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 0

    def test_non_numeric_ref_skipped(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-4", "issue_create", "owner/repo", "abc")
        conn.close()

        count = link_issues("owner/repo", gh_db, sess_db)
        assert count == 0

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 0

    def test_idempotent_no_duplicates(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-5", "issue_create", "owner/repo", "10")
        conn.close()

        link_issues("owner/repo", gh_db, sess_db)
        link_issues("owner/repo", gh_db, sess_db)

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_other_repo_not_linked(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-6", "issue_create", "owner/repo-A", "5")
        conn.close()

        # link_issues called for repo-B — should find nothing
        count = link_issues("owner/repo-B", gh_db, sess_db)
        assert count == 0

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        assert len(rows) == 0

    def test_both_event_types_deduplicated(self, tmp_path):
        """Same session+issue appears as both issue_create and issue_comment — only 1 row in issue_sessions."""
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        sess_db.touch()

        init_github_db(gh_db)
        conn = sqlite3.connect(str(gh_db))
        _insert_gh_event(conn, "sess-7", "issue_create", "owner/repo", "99")
        _insert_gh_event(conn, "sess-7", "issue_comment", "owner/repo", "99")
        conn.close()

        count = link_issues("owner/repo", gh_db, sess_db)

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM issue_sessions").fetchall()
        conn.close()
        # DISTINCT in the query means only 1 row despite 2 events
        assert len(rows) == 1
        assert count == 1
