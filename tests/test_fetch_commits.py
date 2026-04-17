"""Tests for collector/github_poller/fetch_commits.py (Epic #17, E-1).

Uses fixture data and mocked ``gh_api`` — no live network calls.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import patch, MagicMock

import pytest

from am_i_shipping.db import init_github_db
from collector.github_poller.fetch_commits import (
    _normalize_commit,
    fetch_and_store_pr_commits,
    fetch_pr_commits,
)
from collector.github_poller.gh_client import GhCliError


# ---------------------------------------------------------------------------
# Fixture data — the raw REST shape that gh api returns for /pulls/{n}/commits.
# ---------------------------------------------------------------------------

REST_COMMITS_FIXTURE = [
    {
        "sha": "aaaa1111",
        "commit": {
            "message": "initial commit",
            "author": {
                "name": "Alice Author",
                "date": "2024-01-19T10:00:00Z",
            },
            "committer": {
                "name": "Alice Author",
                "date": "2024-01-19T10:00:00Z",
            },
        },
        "author": {"login": "alice"},
    },
    {
        "sha": "bbbb2222",
        "commit": {
            "message": "address review feedback",
            "author": {
                "name": "Bob Bytes",
                "date": "2024-01-20T14:00:00Z",
            },
            "committer": {
                "name": "Bob Bytes",
                "date": "2024-01-20T14:00:00Z",
            },
        },
        "author": {"login": "bob"},
    },
    # A detached-author commit: no top-level "author" key with login.
    {
        "sha": "cccc3333",
        "commit": {
            "message": "cherry-pick",
            "author": {
                "name": "Charlie Cherry",
                "date": "2024-01-21T09:00:00Z",
            },
            "committer": {
                "name": "GitHub",
                "date": "2024-01-21T09:00:00Z",
            },
        },
        "author": None,
    },
]


class TestNormalizeCommit:
    def test_normalize_prefers_login(self):
        normalized = _normalize_commit(REST_COMMITS_FIXTURE[0], pr_number=1)
        assert normalized["sha"] == "aaaa1111"
        assert normalized["author"] == "alice"
        assert normalized["authored_at"] == "2024-01-19T10:00:00Z"
        assert normalized["message"] == "initial commit"
        assert normalized["pr_number"] == 1
        assert normalized["pushed_at"] == "2024-01-19T10:00:00Z"

    def test_normalize_falls_back_to_commit_author_name(self):
        """When the top-level author.login is missing, use commit.author.name."""
        normalized = _normalize_commit(REST_COMMITS_FIXTURE[2], pr_number=7)
        assert normalized["author"] == "Charlie Cherry"


class TestFetchPrCommits:
    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_returns_normalized_list(self, mock_api):
        mock_api.return_value = REST_COMMITS_FIXTURE
        result = fetch_pr_commits("owner/repo", 42)

        assert len(result) == 3
        # Verify the correct endpoint was called with pagination
        args, kwargs = mock_api.call_args
        assert args[0] == "/repos/owner/repo/pulls/42/commits"
        assert kwargs.get("paginate") is True
        # Every row carries the pr_number
        assert all(c["pr_number"] == 42 for c in result)
        # SHAs preserved
        assert [c["sha"] for c in result] == ["aaaa1111", "bbbb2222", "cccc3333"]

    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_api_error_propagates(self, mock_api):
        """Post-F-2: GhCliError is now surfaced to the caller so
        push_counter can fall back instead of silently collapsing push_count
        to 0 on a transient /commits failure."""
        mock_api.side_effect = GhCliError(["gh", "api"], 1, "boom")
        with pytest.raises(GhCliError):
            fetch_pr_commits("owner/repo", 1)

    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_non_list_response_returns_empty(self, mock_api):
        mock_api.return_value = {"unexpected": "object"}
        assert fetch_pr_commits("owner/repo", 1) == []

    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_skips_rows_without_sha(self, mock_api):
        mock_api.return_value = [
            {"commit": {"author": {"date": "2024-01-01T00:00:00Z"}}},  # no sha
            REST_COMMITS_FIXTURE[0],
        ]
        result = fetch_pr_commits("owner/repo", 1)
        assert len(result) == 1
        assert result[0]["sha"] == "aaaa1111"


class TestFetchAndStorePrCommits:
    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_persists_each_commit(self, mock_api, tmp_path):
        mock_api.return_value = REST_COMMITS_FIXTURE
        db = tmp_path / "github.db"
        init_github_db(db)

        returned = fetch_and_store_pr_commits("owner/repo", 42, db)
        assert len(returned) == 3

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT sha, pr_number, author FROM commits "
                "WHERE repo = 'owner/repo' ORDER BY sha"
            ).fetchall()
        finally:
            conn.close()

        assert rows == [
            ("aaaa1111", 42, "alice"),
            ("bbbb2222", 42, "bob"),
            ("cccc3333", 42, "Charlie Cherry"),
        ]

    @patch("collector.github_poller.fetch_commits.gh_api")
    def test_idempotent_double_store(self, mock_api, tmp_path):
        """Running twice produces the same row count — ON CONFLICT DO UPDATE."""
        mock_api.return_value = REST_COMMITS_FIXTURE
        db = tmp_path / "github.db"
        init_github_db(db)

        fetch_and_store_pr_commits("owner/repo", 42, db)
        fetch_and_store_pr_commits("owner/repo", 42, db)

        conn = sqlite3.connect(str(db))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM commits WHERE repo = 'owner/repo'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert count == 3

    def test_skips_fetch_when_commits_supplied(self, tmp_path):
        """If the caller already has the commits, no gh_api call is made."""
        db = tmp_path / "github.db"
        init_github_db(db)

        pre_fetched = [
            {
                "sha": "ffff0000",
                "author": "external",
                "authored_at": "2024-02-01T00:00:00Z",
                "message": "pre-fetched",
                "pr_number": 99,
                "pushed_at": "2024-02-01T00:00:00Z",
            },
        ]

        with patch("collector.github_poller.fetch_commits.gh_api") as mock_api:
            returned = fetch_and_store_pr_commits(
                "owner/repo", 99, db, commits=pre_fetched,
            )
            assert mock_api.call_count == 0

        assert returned == pre_fetched
        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT sha, pr_number FROM commits WHERE repo = 'owner/repo'"
            ).fetchone()
        finally:
            conn.close()
        assert row == ("ffff0000", 99)
