"""Tests for collector/github_poller/push_counter.py (C2-3).

Uses fixture data and mocked gh_api — no live network calls.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from collector.github_poller.push_counter import count_pushes_after_review


# --- Fixture data ---

REVIEWS_FIXTURE = [
    {
        "submitted_at": "2024-01-20T12:00:00Z",
        "state": "CHANGES_REQUESTED",
    },
    {
        "submitted_at": "2024-01-21T10:00:00Z",
        "state": "APPROVED",
    },
]

COMMITS_FIXTURE = [
    {
        "commit": {
            "committer": {"date": "2024-01-19T10:00:00Z"},
            "message": "initial commit",
        }
    },
    {
        "commit": {
            "committer": {"date": "2024-01-20T14:00:00Z"},
            "message": "address review feedback",
        }
    },
    {
        "commit": {
            "committer": {"date": "2024-01-21T09:00:00Z"},
            "message": "more fixes",
        }
    },
]


class TestPushCounter:
    @patch("collector.github_poller.push_counter.gh_api")
    def test_no_reviews_returns_zero(self, mock_api):
        mock_api.return_value = []
        assert count_pushes_after_review("owner/repo", 1) == 0

    @patch("collector.github_poller.push_counter.gh_api")
    def test_commits_before_first_review_not_counted(self, mock_api):
        """Commits before first review timestamp should not be counted."""
        reviews = [{"submitted_at": "2024-01-20T12:00:00Z"}]
        commits = [
            {"commit": {"committer": {"date": "2024-01-19T10:00:00Z"}}},
            {"commit": {"committer": {"date": "2024-01-20T11:00:00Z"}}},
        ]
        mock_api.side_effect = [reviews, commits]
        assert count_pushes_after_review("owner/repo", 1) == 0

    @patch("collector.github_poller.push_counter.gh_api")
    def test_mixed_commits(self, mock_api):
        """Commits after first review are counted; before are not."""
        mock_api.side_effect = [REVIEWS_FIXTURE, COMMITS_FIXTURE]
        # First review at 2024-01-20T12:00:00Z
        # Commits: 01-19 (before), 01-20 14:00 (after), 01-21 09:00 (after)
        assert count_pushes_after_review("owner/repo", 1) == 2

    @patch("collector.github_poller.push_counter.gh_api")
    def test_all_commits_before_review(self, mock_api):
        """All commits before the first review → 0."""
        reviews = [{"submitted_at": "2024-02-01T00:00:00Z"}]
        commits = COMMITS_FIXTURE
        mock_api.side_effect = [reviews, commits]
        assert count_pushes_after_review("owner/repo", 1) == 0

    @patch("collector.github_poller.push_counter.gh_api")
    def test_api_failure_returns_zero(self, mock_api):
        """If API calls fail, returns 0 rather than raising."""
        mock_api.side_effect = Exception("timeout")
        assert count_pushes_after_review("owner/repo", 1) == 0
