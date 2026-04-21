"""Tests for collector/github_poller/session_linker.py (C2-5).

Uses temporary SQLite databases; no live network calls.
"""

from __future__ import annotations

import sqlite3

import pytest

from am_i_shipping.db import init_github_db, init_sessions_db
from collector.github_poller.session_linker import link_sessions
from collector.github_poller.store import upsert_pr


def _setup_github_db(db_path, prs, gh_events=None):
    """Set up github.db with some PR rows and optional session_gh_events rows.

    Parameters
    ----------
    db_path:
        Path to github.db.
    prs:
        List of PR dicts (passed to upsert_pr).
    gh_events:
        Optional list of dicts with keys ``session_uuid``, ``repo``.  Each
        dict results in one ``session_gh_events`` row so that
        ``session_linker.link_sessions`` sees the session as having touched
        that repo via an observed gh-CLI event.
    """
    init_github_db(db_path)
    for pr in prs:
        upsert_pr(pr["repo"], pr, db_path)
    if gh_events:
        conn = sqlite3.connect(str(db_path))
        for ev in gh_events:
            conn.execute(
                "INSERT OR IGNORE INTO session_gh_events "
                "(session_uuid, event_type, repo, ref, url, confidence, created_at) "
                "VALUES (?, 'pr_create', ?, '1', '', 'high', '')",
                (ev["session_uuid"], ev["repo"]),
            )
        conn.commit()
        conn.close()


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

        _setup_github_db(
            gh_db,
            [
                {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
                 "title": "", "body": "", "review_comments": [],
                 "review_comment_count": 0, "push_count": 0,
                 "created_at": None, "merged_at": None},
            ],
            gh_events=[{"session_uuid": "sess-1", "repo": "owner/repo"}],
        )
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

        _setup_github_db(
            gh_db,
            [
                {"repo": "owner/repo", "number": 1, "head_ref": "fix/1-bug",
                 "title": "", "body": "", "review_comments": [],
                 "review_comment_count": 0, "push_count": 0,
                 "created_at": None, "merged_at": None},
            ],
            gh_events=[{"session_uuid": "sess-1", "repo": "owner/repo"}],
        )
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

    def test_mismatched_workdir_still_links(self, tmp_path):
        """Regression test for issue #83.

        A session whose ``working_directory`` does NOT contain the repo name
        slug (e.g. the local clone is named ``claude-rts`` while the GitHub
        repo is ``supreme-claudemander``) must still be linked when a
        ``session_gh_events`` row confirms the session issued a gh command
        targeting that repo.  Under the old working_directory substring
        heuristic this case would silently produce zero links.
        """
        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        # The repo slug "owner/supreme-claudemander" does NOT appear in the
        # working_directory path "/workspaces/claude-rts".
        _setup_github_db(
            gh_db,
            [
                {"repo": "owner/supreme-claudemander", "number": 7,
                 "head_ref": "feat/cool-feature",
                 "title": "", "body": "", "review_comments": [],
                 "review_comment_count": 0, "push_count": 0,
                 "created_at": None, "merged_at": None},
            ],
            gh_events=[
                {"session_uuid": "sess-mismatch", "repo": "owner/supreme-claudemander"},
            ],
        )
        _setup_sessions_db(sess_db, [
            {
                "uuid": "sess-mismatch",
                "branch": "feat/cool-feature",
                # working_directory has no substring matching the repo name
                "workdir": "/workspaces/claude-rts",
            },
        ])

        count = link_sessions("owner/supreme-claudemander", gh_db, sess_db)
        assert count >= 1, (
            "Expected a link for sess-mismatch even though working_directory "
            "does not contain the repo name slug (issue #83 regression)"
        )

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
