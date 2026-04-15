"""Tests for collector/github_poller/session_linker.py (C2-5).

Uses temporary SQLite databases; no live network calls.
"""

from __future__ import annotations

import sqlite3

import pytest

from am_i_shipping.db import init_github_db, init_sessions_db
from collector.github_poller.session_linker import link_sessions
from collector.github_poller.store import upsert_pr


def _setup_github_db(db_path, prs):
    """Set up github.db with some PR rows."""
    init_github_db(db_path)
    for pr in prs:
        upsert_pr(pr["repo"], pr, db_path)


def _setup_sessions_db(db_path, sessions):
    """Set up sessions.db with some session rows."""
    init_sessions_db(db_path)
    conn = sqlite3.connect(str(db_path))
    for s in sessions:
        conn.execute(
            """
            INSERT INTO sessions (session_uuid, turn_count, tool_call_count,
                tool_failure_count, reprompt_count, bail_out,
                session_duration_seconds, working_directory, git_branch,
                raw_content_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                s["uuid"], 5, 10, 0, 0, 0, 120.0,
                s.get("workdir", "/workspaces/repo"),
                s.get("branch", "main"),
                "[]",
            ),
        )
    conn.commit()
    conn.close()


class TestSessionLinker:
    def test_no_sessions_db_returns_zero(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "nonexistent.db"
        _setup_github_db(gh_db, [
            {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
             "title": "", "body": "", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ])
        assert link_sessions("owner/repo", gh_db, sess_db) == 0

    def test_matching_branch_creates_link(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        _setup_github_db(gh_db, [
            {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
             "title": "", "body": "", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ])
        _setup_sessions_db(sess_db, [
            {"uuid": "sess-1", "branch": "fix/1-bug",
             "workdir": "/workspaces/repo"},
        ])

        count = link_sessions("owner/repo", gh_db, sess_db)
        assert count >= 1

        # Verify the link exists
        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM pr_sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_no_matching_branch_zero_links(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        _setup_github_db(gh_db, [
            {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
             "title": "", "body": "", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ])
        _setup_sessions_db(sess_db, [
            {"uuid": "sess-1", "branch": "feature/2-other",
             "workdir": "/workspaces/repo"},
        ])

        count = link_sessions("owner/repo", gh_db, sess_db)
        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM pr_sessions").fetchall()
        conn.close()
        assert len(rows) == 0

    def test_idempotent_no_duplicates(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        _setup_github_db(gh_db, [
            {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
             "title": "", "body": "", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ])
        _setup_sessions_db(sess_db, [
            {"uuid": "sess-1", "branch": "fix/1-bug",
             "workdir": "/workspaces/repo"},
        ])

        link_sessions("owner/repo", gh_db, sess_db)
        link_sessions("owner/repo", gh_db, sess_db)

        conn = sqlite3.connect(str(gh_db))
        rows = conn.execute("SELECT * FROM pr_sessions").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_empty_sessions_db_returns_zero(self, tmp_path):
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        _setup_github_db(gh_db, [
            {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
             "title": "", "body": "", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ])
        init_sessions_db(sess_db)  # empty table

        count = link_sessions("owner/repo", gh_db, sess_db)
        assert count == 0
