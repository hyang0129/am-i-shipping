"""Tests for collector/github_poller/run.py (C2-5 orchestrator).

Uses mocks for all external calls — no live network or gh CLI calls.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from collector.github_poller.run import run, _poll_repo


@pytest.fixture
def config_file(tmp_path):
    """Create a minimal config.yaml in tmp_path."""
    config = {
        "session": {
            "projects_path": str(tmp_path / "projects"),
        },
        "github": {
            "repos": ["owner/repo"],
            "backfill_days": 30,
        },
        "data": {
            "data_dir": str(tmp_path / "data"),
        },
    }
    config_path = tmp_path / "config.yaml"
    import yaml
    config_path.write_text(yaml.dump(config))
    return str(config_path)


class TestDryRun:
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_dry_run_no_db_writes(
        self, mock_issues, mock_prs, mock_push, config_file, tmp_path
    ):
        mock_issues.return_value = [
            {"number": 1, "title": "t", "state": "OPEN", "body": "",
             "comments": [], "type_label": None,
             "created_at": None, "closed_at": None},
        ]
        mock_prs.return_value = [
            {"number": 10, "title": "p", "head_ref": "fix/1",
             "body": "Closes #1", "review_comments": [],
             "review_comment_count": 0, "push_count": 0,
             "created_at": None, "merged_at": None},
        ]
        mock_push.return_value = 0

        count, ok = run(config_path=config_file, dry_run=True)
        assert count == 2  # 1 issue + 1 PR
        assert ok is True

        # Verify no DB was created
        data_dir = tmp_path / "data"
        github_db = data_dir / "github.db"
        # In dry-run, init_github_db is not called, so the DB
        # may or may not exist depending on implementation.
        # The key assertion is that no rows were written.
        if github_db.exists():
            conn = sqlite3.connect(str(github_db))
            try:
                issues = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
                prs = conn.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
                assert issues == 0
                assert prs == 0
            except sqlite3.OperationalError:
                pass  # table doesn't exist — that's fine in dry-run
            finally:
                conn.close()

        # No health.json in dry-run
        health = data_dir / "health.json"
        assert not health.exists()


class TestPollRepo:
    @patch("collector.github_poller.run.fetch_and_store_issue_timelines", return_value={})
    @patch("collector.github_poller.run.fetch_and_store_pr_commits", return_value=[])
    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_full_poll_produces_rows(
        self, mock_issues, mock_prs, mock_issue_comments, mock_pr_comments,
        mock_edit_batch, mock_pr_edit, mock_push, mock_link,
        mock_fetch_commits, mock_fetch_timeline,
        tmp_path
    ):
        mock_issues.return_value = [
            {"number": 1, "title": "Issue 1", "state": "OPEN",
             "body": "body", "comments": [], "type_label": "bug",
             "created_at": "2024-01-01", "closed_at": None},
        ]
        mock_prs.return_value = [
            {"number": 10, "title": "PR 10", "head_ref": "fix/1-bug",
             "body": "Closes #1", "review_comments": [],
             "review_comment_count": 0, "created_at": "2024-01-02",
             "merged_at": None},
        ]
        mock_push.return_value = 2
        mock_link.return_value = 0

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        count = _poll_repo(
            "owner/repo", gh_db, sess_db,
            backfill_days=30, dry_run=False,
        )
        assert count == 2

        # Verify DB contents
        conn = sqlite3.connect(str(gh_db))
        issues = conn.execute("SELECT * FROM issues").fetchall()
        prs = conn.execute("SELECT * FROM pull_requests").fetchall()
        links = conn.execute("SELECT * FROM pr_issues").fetchall()
        cursor = conn.execute("SELECT * FROM poll_cursor").fetchall()
        conn.close()

        assert len(issues) == 1
        assert len(prs) == 1
        assert len(links) == 1  # fix/1-bug resolves to issue #1
        assert len(cursor) == 1  # cursor advanced

    @patch("collector.github_poller.run.fetch_and_store_issue_timelines", return_value={})
    @patch("collector.github_poller.run.fetch_and_store_pr_commits", return_value=[])
    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_rerun_produces_same_row_count(
        self, mock_issues, mock_prs, mock_issue_comments, mock_pr_comments,
        mock_edit_batch, mock_issue_edit, mock_pr_edit, mock_push, mock_link,
        mock_fetch_commits, mock_fetch_timeline,
        tmp_path
    ):
        """Re-running poll with same data produces identical row count."""
        issue = {
            "number": 1, "title": "Issue 1", "state": "OPEN",
            "body": "body", "comments": [], "type_label": None,
            "created_at": "2024-01-01", "closed_at": None,
        }
        pr = {
            "number": 10, "title": "PR 10", "head_ref": "main",
            "body": "no link", "review_comments": [],
            "review_comment_count": 0, "created_at": "2024-01-02",
            "merged_at": None,
        }
        mock_issues.return_value = [issue]
        mock_prs.return_value = [pr]
        mock_push.return_value = 0
        mock_link.return_value = 0

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        _poll_repo("owner/repo", gh_db, sess_db, 30, False)
        _poll_repo("owner/repo", gh_db, sess_db, 30, False)

        conn = sqlite3.connect(str(gh_db))
        issues = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        prs = conn.execute("SELECT COUNT(*) FROM pull_requests").fetchone()[0]
        conn.close()

        assert issues == 1
        assert prs == 1


class TestHealthJson:
    @patch("collector.github_poller.run.fetch_and_store_issue_timelines", return_value={})
    @patch("collector.github_poller.run.fetch_and_store_pr_commits", return_value=[])
    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_health_written_on_success(
        self, mock_issues, mock_prs, mock_push, mock_link,
        mock_fetch_commits, mock_fetch_timeline,
        config_file, tmp_path
    ):
        mock_issues.return_value = []
        mock_prs.return_value = []
        mock_push.return_value = 0
        mock_link.return_value = 0

        run(config_path=config_file, dry_run=False)

        health = tmp_path / "data" / "health.json"
        assert health.exists()
        data = json.loads(health.read_text())
        assert "github_poller" in data
        assert "last_success" in data["github_poller"]

    @patch("collector.github_poller.run.fetch_and_store_issue_timelines", return_value={})
    @patch("collector.github_poller.run.fetch_and_store_pr_commits", return_value=[])
    @patch("collector.github_poller.run.link_sessions")
    @patch("collector.github_poller.run.count_pushes_after_review")
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    def test_health_not_written_on_failure(
        self, mock_issues, mock_prs, mock_push, mock_link,
        mock_fetch_commits, mock_fetch_timeline,
        config_file, tmp_path
    ):
        mock_issues.side_effect = Exception("API down")

        run(config_path=config_file, dry_run=False)

        health = tmp_path / "data" / "health.json"
        assert not health.exists()


class TestEpic17FetchIntegration:
    """Epic #17 Sub-Issue 2 (#35): wiring for fetch_commits / fetch_timeline.

    Asserts that ``_poll_repo`` calls the new E-1/E-2 helpers when the
    flags are on, and skips them when the flags are off — without changing
    behaviour of the pre-existing issue/PR path.
    """

    @patch("collector.github_poller.run.link_sessions", return_value=0)
    @patch("collector.github_poller.run.count_pushes_after_review", return_value=0)
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    @patch("collector.github_poller.run.fetch_and_store_issue_timelines")
    @patch("collector.github_poller.run.fetch_and_store_pr_commits")
    def test_fetchers_called_by_default(
        self,
        mock_fetch_commits,
        mock_fetch_timeline,
        mock_issues,
        mock_prs,
        mock_issue_comments,
        mock_pr_review_comments,
        mock_edit_batch,
        mock_pr_edit,
        mock_push,
        mock_link,
        tmp_path,
    ):
        mock_issues.return_value = [
            {"number": 1, "title": "I", "state": "OPEN", "body": "",
             "comments": [], "type_label": None, "created_at": None,
             "closed_at": None},
        ]
        mock_prs.return_value = [
            {"number": 10, "title": "P", "head_ref": "fix/1",
             "body": "Closes #1", "review_comments": [],
             "review_comment_count": 0, "created_at": None,
             "merged_at": None},
        ]
        mock_fetch_commits.return_value = []
        mock_fetch_timeline.return_value = {}

        _poll_repo(
            "owner/repo",
            tmp_path / "github.db",
            tmp_path / "sessions.db",
            backfill_days=30,
            dry_run=False,
            fetch_commits_enabled=True,
            fetch_timeline_enabled=True,
        )

        # fetch_and_store_pr_commits was called once per PR.
        assert mock_fetch_commits.call_count == 1
        # fetch_and_store_issue_timelines was called with the list of issue numbers.
        assert mock_fetch_timeline.call_count == 1
        call_args = mock_fetch_timeline.call_args
        assert call_args.args[0] == "owner/repo"
        assert call_args.args[1] == [1]

    @patch("collector.github_poller.run.link_sessions", return_value=0)
    @patch("collector.github_poller.run.count_pushes_after_review", return_value=0)
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    @patch("collector.github_poller.run.fetch_and_store_issue_timelines")
    @patch("collector.github_poller.run.fetch_and_store_pr_commits")
    def test_fetchers_skipped_when_flags_off(
        self,
        mock_fetch_commits,
        mock_fetch_timeline,
        mock_issues,
        mock_prs,
        mock_issue_comments,
        mock_pr_review_comments,
        mock_edit_batch,
        mock_pr_edit,
        mock_push,
        mock_link,
        tmp_path,
    ):
        mock_issues.return_value = [
            {"number": 1, "title": "I", "state": "OPEN", "body": "",
             "comments": [], "type_label": None, "created_at": None,
             "closed_at": None},
        ]
        mock_prs.return_value = [
            {"number": 10, "title": "P", "head_ref": "fix/1",
             "body": "Closes #1", "review_comments": [],
             "review_comment_count": 0, "created_at": None,
             "merged_at": None},
        ]

        _poll_repo(
            "owner/repo",
            tmp_path / "github.db",
            tmp_path / "sessions.db",
            backfill_days=30,
            dry_run=False,
            fetch_commits_enabled=False,
            fetch_timeline_enabled=False,
        )

        # Neither new fetcher was invoked.
        assert mock_fetch_commits.call_count == 0
        assert mock_fetch_timeline.call_count == 0

    @patch("collector.github_poller.run.link_sessions", return_value=0)
    @patch("collector.github_poller.run.count_pushes_after_review", return_value=7)
    @patch("collector.github_poller.run.fetch_pr_edit_history", return_value={})
    @patch("collector.github_poller.run.fetch_issue_edit_history_batch", return_value={})
    @patch("collector.github_poller.run.fetch_pr_review_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_issue_comments", return_value=[])
    @patch("collector.github_poller.run.fetch_prs")
    @patch("collector.github_poller.run.fetch_issues")
    @patch("collector.github_poller.run.fetch_and_store_issue_timelines", return_value={})
    @patch("collector.github_poller.run.fetch_and_store_pr_commits")
    def test_fetch_commits_error_falls_back_to_push_counter_self_fetch(
        self,
        mock_fetch_commits,
        mock_fetch_timeline,
        mock_issues,
        mock_prs,
        mock_issue_comments,
        mock_pr_review_comments,
        mock_edit_batch,
        mock_pr_edit,
        mock_push,
        mock_link,
        tmp_path,
    ):
        """F-2 invariant: when fetch_and_store_pr_commits raises GhCliError,
        count_pushes_after_review is invoked WITHOUT the `commits` kwarg, so
        push_counter can perform its own /commits fetch. The push_count must
        NOT collapse to 0 on a transient fetch_commits failure."""
        from collector.github_poller.gh_client import GhCliError

        mock_issues.return_value = []
        mock_prs.return_value = [
            {"number": 10, "title": "P", "head_ref": "fix/1",
             "body": "Closes #1", "review_comments": [],
             "review_comment_count": 0, "created_at": None,
             "merged_at": None},
        ]
        # fetch_commits blows up with a transient error.
        mock_fetch_commits.side_effect = GhCliError(["gh"], 1, "boom")

        _poll_repo(
            "owner/repo",
            tmp_path / "github.db",
            tmp_path / "sessions.db",
            backfill_days=30,
            dry_run=False,
            fetch_commits_enabled=True,
            fetch_timeline_enabled=False,
        )

        # count_pushes_after_review must have been called without the kwarg
        # so push_counter re-fetches /commits itself and returns its own count
        # (7 per the mock) — NOT 0.
        assert mock_push.call_count == 1
        call_kwargs = mock_push.call_args.kwargs
        assert "commits" not in call_kwargs, (
            "Fallback path must NOT pass commits=[] — that would collapse "
            "push_count to 0. See F-2."
        )
