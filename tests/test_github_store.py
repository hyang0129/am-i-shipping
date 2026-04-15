"""Tests for collector/github_poller/store.py (C2-4).

Uses temporary SQLite databases; asserts idempotent upserts and
field validation.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from collector.github_poller.store import (
    upsert_issue,
    upsert_pr,
    upsert_pr_issue_link,
    insert_issue_body_edit,
    insert_issue_comment_edit,
    insert_pr_body_edit,
    insert_pr_review_comment_edit,
)


def _make_issue(**overrides):
    defaults = {
        "number": 1,
        "title": "Test issue",
        "type_label": "bug",
        "state": "OPEN",
        "body": "Fix this.",
        "comments": [{"author": "alice", "body": "ok", "created_at": "2024-01-01"}],
        "created_at": "2024-01-01T00:00:00Z",
        "closed_at": None,
    }
    defaults.update(overrides)
    return defaults


def _make_pr(**overrides):
    defaults = {
        "number": 10,
        "title": "Fix bug",
        "head_ref": "fix/1-bug",
        "body": "Closes #1",
        "review_comments": [],
        "review_comment_count": 0,
        "push_count": 0,
        "created_at": "2024-01-02T00:00:00Z",
        "merged_at": None,
    }
    defaults.update(overrides)
    return defaults


class TestUpsertIssue:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        upsert_issue("owner/repo", _make_issue(), db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issues").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_two_identical_upserts_one_row(self, tmp_path):
        db = tmp_path / "github.db"
        issue = _make_issue()
        upsert_issue("owner/repo", issue, db)
        upsert_issue("owner/repo", issue, db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issues").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_upsert_updates_values(self, tmp_path):
        db = tmp_path / "github.db"
        upsert_issue("owner/repo", _make_issue(title="v1"), db)
        upsert_issue("owner/repo", _make_issue(title="v2"), db)

        conn = sqlite3.connect(str(db))
        title = conn.execute("SELECT title FROM issues").fetchone()[0]
        conn.close()
        assert title == "v2"

    def test_comments_stored_as_json(self, tmp_path):
        db = tmp_path / "github.db"
        comments = [{"author": "bob", "body": "lgtm", "created_at": "2024-01-02"}]
        upsert_issue("owner/repo", _make_issue(comments=comments), db)

        conn = sqlite3.connect(str(db))
        raw = conn.execute("SELECT comments_json FROM issues").fetchone()[0]
        conn.close()
        parsed = json.loads(raw)
        assert parsed[0]["author"] == "bob"

    def test_missing_repo_raises(self, tmp_path):
        db = tmp_path / "github.db"
        with pytest.raises(ValueError, match="repo is required"):
            upsert_issue("", _make_issue(), db)

    def test_missing_number_raises(self, tmp_path):
        db = tmp_path / "github.db"
        issue = _make_issue()
        del issue["number"]
        with pytest.raises(ValueError, match="issue number is required"):
            upsert_issue("owner/repo", issue, db)


class TestUpsertPR:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        upsert_pr("owner/repo", _make_pr(), db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pull_requests").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_two_identical_upserts_one_row(self, tmp_path):
        db = tmp_path / "github.db"
        pr = _make_pr()
        upsert_pr("owner/repo", pr, db)
        upsert_pr("owner/repo", pr, db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pull_requests").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_missing_repo_raises(self, tmp_path):
        db = tmp_path / "github.db"
        with pytest.raises(ValueError, match="repo is required"):
            upsert_pr("", _make_pr(), db)

    def test_missing_number_raises(self, tmp_path):
        db = tmp_path / "github.db"
        pr = _make_pr()
        del pr["number"]
        with pytest.raises(ValueError, match="pr number is required"):
            upsert_pr("owner/repo", pr, db)


class TestUpsertPRIssueLink:
    def test_insert_link(self, tmp_path):
        db = tmp_path / "github.db"
        upsert_pr_issue_link("owner/repo", 10, 1, db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_issues").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_duplicate_link_idempotent(self, tmp_path):
        db = tmp_path / "github.db"
        upsert_pr_issue_link("owner/repo", 10, 1, db)
        upsert_pr_issue_link("owner/repo", 10, 1, db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_issues").fetchall()
        conn.close()
        assert len(rows) == 1


class TestInsertIssueBodyEdit:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        insert_issue_body_edit("owner/repo", 1, "2024-01-20T16:00:00Z", "- old\n+ new", "alice", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issue_body_edits").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_duplicate_insert_idempotent(self, tmp_path):
        db = tmp_path / "github.db"
        insert_issue_body_edit("owner/repo", 1, "2024-01-20T16:00:00Z", "diff", "alice", db)
        insert_issue_body_edit("owner/repo", 1, "2024-01-20T16:00:00Z", "diff", "alice", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issue_body_edits").fetchall()
        conn.close()
        assert len(rows) == 1


class TestInsertIssueCommentEdit:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        insert_issue_comment_edit("owner/repo", 1, 123, "2024-01-20T17:00:00Z", "- old\n+ new", "bob", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issue_comment_edits").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_duplicate_insert_idempotent(self, tmp_path):
        db = tmp_path / "github.db"
        insert_issue_comment_edit("owner/repo", 1, 123, "2024-01-20T17:00:00Z", "diff", "bob", db)
        insert_issue_comment_edit("owner/repo", 1, 123, "2024-01-20T17:00:00Z", "diff", "bob", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM issue_comment_edits").fetchall()
        conn.close()
        assert len(rows) == 1


class TestInsertPrBodyEdit:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        insert_pr_body_edit("owner/repo", 10, "2024-01-21T10:00:00Z", "- old pr\n+ new pr", "carol", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_body_edits").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_duplicate_insert_idempotent(self, tmp_path):
        db = tmp_path / "github.db"
        insert_pr_body_edit("owner/repo", 10, "2024-01-21T10:00:00Z", "diff", "carol", db)
        insert_pr_body_edit("owner/repo", 10, "2024-01-21T10:00:00Z", "diff", "carol", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_body_edits").fetchall()
        conn.close()
        assert len(rows) == 1


class TestInsertPrReviewCommentEdit:
    def test_insert_creates_row(self, tmp_path):
        db = tmp_path / "github.db"
        insert_pr_review_comment_edit("owner/repo", 10, 456, "2024-01-21T11:00:00Z", "- old\n+ new", "dave", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_review_comment_edits").fetchall()
        conn.close()
        assert len(rows) == 1

    def test_duplicate_insert_idempotent(self, tmp_path):
        db = tmp_path / "github.db"
        insert_pr_review_comment_edit("owner/repo", 10, 456, "2024-01-21T11:00:00Z", "diff", "dave", db)
        insert_pr_review_comment_edit("owner/repo", 10, 456, "2024-01-21T11:00:00Z", "diff", "dave", db)

        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT * FROM pr_review_comment_edits").fetchall()
        conn.close()
        assert len(rows) == 1


class TestUpsertIssueUpdatedAt:
    def test_updated_at_stored(self, tmp_path):
        db = tmp_path / "github.db"
        issue = _make_issue(updated_at="2024-01-20T16:00:00Z")
        upsert_issue("owner/repo", issue, db)

        conn = sqlite3.connect(str(db))
        val = conn.execute("SELECT updated_at FROM issues").fetchone()[0]
        conn.close()
        assert val == "2024-01-20T16:00:00Z"

    def test_updated_at_defaults_to_none(self, tmp_path):
        """Existing callers that don't pass updated_at still work."""
        db = tmp_path / "github.db"
        issue = _make_issue()  # no updated_at override
        upsert_issue("owner/repo", issue, db)

        conn = sqlite3.connect(str(db))
        val = conn.execute("SELECT updated_at FROM issues").fetchone()[0]
        conn.close()
        assert val is None


class TestUpsertPrUpdatedAt:
    def test_updated_at_stored(self, tmp_path):
        db = tmp_path / "github.db"
        pr = _make_pr(updated_at="2024-01-22T10:00:00Z")
        upsert_pr("owner/repo", pr, db)

        conn = sqlite3.connect(str(db))
        val = conn.execute("SELECT updated_at FROM pull_requests").fetchone()[0]
        conn.close()
        assert val == "2024-01-22T10:00:00Z"

    def test_updated_at_defaults_to_none(self, tmp_path):
        """Existing callers that don't pass updated_at still work."""
        db = tmp_path / "github.db"
        pr = _make_pr()  # no updated_at override
        upsert_pr("owner/repo", pr, db)

        conn = sqlite3.connect(str(db))
        val = conn.execute("SELECT updated_at FROM pull_requests").fetchone()[0]
        conn.close()
        assert val is None
