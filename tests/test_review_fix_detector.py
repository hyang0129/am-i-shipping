"""Tests for collector/github_poller/review_fix_detector.py (Issue #86)."""

from __future__ import annotations

import sqlite3

import pytest

from am_i_shipping.db import init_github_db
from collector.github_poller.review_fix_detector import (
    REVIEW_FIX_SUMMARY_MARKER,
    count_fix_commits_after,
    detect_review_fix_event,
    find_review_fix_marker_comment,
    upsert_pr_review_fix_event,
)


SAMPLE_SUMMARY_BODY = (
    REVIEW_FIX_SUMMARY_MARKER
    + "\n## Automated Review-Fix Summary\n\nCycles run: 1 of 2\n"
)


class TestFindReviewFixMarkerComment:
    def test_single_marker_comment(self):
        comments = [
            {"id": 1, "body": "normal comment", "createdAt": "2026-04-18T18:00:00Z"},
            {"id": 2, "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T19:38:31Z"},
        ]
        assert find_review_fix_marker_comment(comments)["id"] == 2

    def test_no_marker_returns_none(self):
        comments = [
            {"id": 1, "body": "plain comment", "createdAt": "2026-04-18T18:00:00Z"},
        ]
        assert find_review_fix_marker_comment(comments) is None

    def test_empty_list_returns_none(self):
        assert find_review_fix_marker_comment([]) is None

    def test_earliest_marker_wins(self):
        """When /review-fix was re-run, the earliest summary comment is used
        as the cycle's anchor — commits between the two marker comments are
        still included in fix_commit_count."""
        comments = [
            {"id": 1, "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T10:00:00Z"},
            {"id": 2, "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T19:38:31Z"},
        ]
        assert find_review_fix_marker_comment(comments)["id"] == 1

    def test_malformed_entries_skipped(self):
        comments = [
            "not a dict",
            {"id": 1, "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T10:00:00Z"},
        ]
        assert find_review_fix_marker_comment(comments)["id"] == 1


class TestCountFixCommitsAfter:
    def test_strictly_after_cutoff(self):
        commits = [
            {"authored_at": "2026-04-18T10:00:00Z"},
            {"authored_at": "2026-04-18T19:38:31Z"},  # exactly cutoff — excluded
            {"authored_at": "2026-04-18T20:00:00Z"},
            {"authored_at": "2026-04-18T21:00:00Z"},
        ]
        assert count_fix_commits_after(commits, "2026-04-18T19:38:31Z") == 2

    def test_falls_back_to_pushed_at(self):
        """When authored_at is missing, pushed_at is the secondary signal."""
        commits = [{"pushed_at": "2026-04-18T20:00:00Z"}]
        assert count_fix_commits_after(commits, "2026-04-18T19:00:00Z") == 1

    def test_none_commits(self):
        assert count_fix_commits_after(None, "2026-04-18T19:00:00Z") == 0

    def test_none_cutoff(self):
        commits = [{"authored_at": "2026-04-18T20:00:00Z"}]
        assert count_fix_commits_after(commits, None) == 0


class TestDetectReviewFixEvent:
    def test_pr_with_marker_returns_event(self):
        pr = {
            "number": 189,
            "comments": [
                {"id": 99, "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T19:38:31Z"},
            ],
        }
        commits = [
            {"authored_at": "2026-04-18T20:00:00Z"},
            {"authored_at": "2026-04-18T21:00:00Z"},
        ]
        event = detect_review_fix_event(pr, commits)
        assert event["pr_number"] == 189
        assert event["posted_at"] == "2026-04-18T19:38:31Z"
        assert event["fix_commit_count"] == 2

    def test_pr_without_marker_returns_none(self):
        pr = {"number": 1, "comments": [{"body": "nothing interesting"}]}
        assert detect_review_fix_event(pr, []) is None

    def test_non_numeric_comment_id_stored_as_null(self):
        """GraphQL node IDs like IC_kwDO... aren't numeric; column is INTEGER
        so we store NULL rather than coercing."""
        pr = {
            "number": 189,
            "comments": [
                {"id": "IC_kwDOSCwKac7_0uSZ", "body": SAMPLE_SUMMARY_BODY, "createdAt": "2026-04-18T19:38:31Z"},
            ],
        }
        event = detect_review_fix_event(pr, None)
        assert event["summary_comment_id"] is None
        assert event["fix_commit_count"] == 0  # no commits supplied


class TestUpsertPrReviewFixEvent:
    @pytest.fixture
    def db(self, tmp_path):
        db_path = tmp_path / "github.db"
        init_github_db(db_path)
        yield db_path

    def test_insert_then_update(self, db):
        event1 = {
            "pr_number": 189,
            "summary_comment_id": None,
            "posted_at": "2026-04-18T19:38:31Z",
            "fix_commit_count": 2,
        }
        upsert_pr_review_fix_event("org/repo", event1, db)

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT posted_at, fix_commit_count FROM pr_review_fix_events "
                "WHERE repo=? AND pr_number=?",
                ("org/repo", 189),
            ).fetchone()
            assert row == ("2026-04-18T19:38:31Z", 2)
        finally:
            conn.close()

        # Re-upsert with an updated count (more fix commits landed).
        event2 = dict(event1)
        event2["fix_commit_count"] = 5
        upsert_pr_review_fix_event("org/repo", event2, db)

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT fix_commit_count FROM pr_review_fix_events "
                "WHERE repo=? AND pr_number=?",
                ("org/repo", 189),
            ).fetchone()
            assert row == (5,)
        finally:
            conn.close()

    def test_missing_repo_raises(self, db):
        with pytest.raises(ValueError):
            upsert_pr_review_fix_event("", {"pr_number": 1}, db)

    def test_missing_pr_number_raises(self, db):
        with pytest.raises(ValueError):
            upsert_pr_review_fix_event("r", {}, db)
