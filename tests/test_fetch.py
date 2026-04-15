"""Tests for collector/github_poller/fetch_issues.py and fetch_prs.py (C2-1).

Uses fixture JSON and mocked gh_client — no live network calls.
"""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from collector.github_poller.fetch_issues import fetch_issues, _extract_type_label
from collector.github_poller.fetch_prs import fetch_prs


# --- Fixture data ---

ISSUE_FIXTURE = [
    {
        "number": 1,
        "title": "Infrastructure Foundation",
        "labels": [{"name": "epic"}, {"name": "enhancement"}],
        "createdAt": "2024-01-15T10:00:00Z",
        "closedAt": "2024-01-20T15:00:00Z",
        "state": "CLOSED",
        "body": "Set up the base infrastructure.",
    },
    {
        "number": 2,
        "title": "Session Parser",
        "labels": [{"name": "feature"}],
        "createdAt": "2024-01-16T10:00:00Z",
        "closedAt": None,
        "state": "OPEN",
        "body": "Build the session parser.",
    },
]

ISSUE_COMMENTS_FIXTURE = [
    {
        "user": {"login": "alice"},
        "body": "Looks good!",
        "created_at": "2024-01-15T12:00:00Z",
    },
]

PR_FIXTURE = [
    {
        "number": 6,
        "title": "fix(#1): Infrastructure foundation",
        "createdAt": "2024-01-18T10:00:00Z",
        "mergedAt": "2024-01-20T14:00:00Z",
        "headRefName": "fix/issue-1-infra-foundation",
        "body": "Closes #1\n\nSets up DB schema and config.",
    },
]

PR_REVIEW_COMMENTS_FIXTURE = [
    {
        "user": {"login": "bob"},
        "body": "Consider using pathlib here.",
        "created_at": "2024-01-19T10:00:00Z",
    },
]


class TestFetchIssues:
    @patch("collector.github_poller.fetch_issues.gh_api")
    @patch("collector.github_poller.fetch_issues.run_gh_json")
    def test_returns_normalized_dicts(self, mock_list, mock_api):
        mock_list.return_value = ISSUE_FIXTURE
        mock_api.return_value = ISSUE_COMMENTS_FIXTURE

        results = fetch_issues("owner/repo", since="2024-01-01")

        assert len(results) == 2
        issue = results[0]
        assert issue["number"] == 1
        assert issue["title"] == "Infrastructure Foundation"
        assert issue["state"] == "CLOSED"
        assert issue["body"] == "Set up the base infrastructure."
        assert isinstance(issue["comments"], list)
        assert issue["comments"][0]["author"] == "alice"

    @patch("collector.github_poller.fetch_issues.gh_api")
    @patch("collector.github_poller.fetch_issues.run_gh_json")
    def test_correct_field_types(self, mock_list, mock_api):
        mock_list.return_value = ISSUE_FIXTURE
        mock_api.return_value = []

        results = fetch_issues("owner/repo")

        for issue in results:
            assert isinstance(issue["number"], int)
            assert isinstance(issue["title"], str)
            assert isinstance(issue["state"], str)
            assert isinstance(issue["body"], str)
            assert isinstance(issue["comments"], list)
            # type_label can be str or None
            assert issue["type_label"] is None or isinstance(issue["type_label"], str)

    @patch("collector.github_poller.fetch_issues.gh_api")
    @patch("collector.github_poller.fetch_issues.run_gh_json")
    def test_comments_on_api_failure(self, mock_list, mock_api):
        """If comment fetch fails, returns empty list, not an error."""
        mock_list.return_value = [ISSUE_FIXTURE[0]]
        mock_api.side_effect = Exception("rate limited")

        results = fetch_issues("owner/repo")
        assert len(results) == 1
        assert results[0]["comments"] == []


class TestExtractTypeLabel:
    def test_finds_feature(self):
        labels = [{"name": "feature"}, {"name": "priority:high"}]
        assert _extract_type_label(labels) == "feature"

    def test_finds_bug(self):
        labels = [{"name": "bug"}]
        assert _extract_type_label(labels) == "bug"

    def test_finds_type_prefix(self):
        labels = [{"name": "type:enhancement"}]
        assert _extract_type_label(labels) == "type:enhancement"

    def test_returns_none(self):
        labels = [{"name": "priority:high"}, {"name": "area:backend"}]
        assert _extract_type_label(labels) is None

    def test_empty_labels(self):
        assert _extract_type_label([]) is None


class TestFetchPRs:
    @patch("collector.github_poller.fetch_prs.gh_api")
    @patch("collector.github_poller.fetch_prs.run_gh_json")
    def test_returns_normalized_dicts(self, mock_list, mock_api):
        mock_list.return_value = PR_FIXTURE
        mock_api.return_value = PR_REVIEW_COMMENTS_FIXTURE

        results = fetch_prs("owner/repo", since="2024-01-01")

        assert len(results) == 1
        pr = results[0]
        assert pr["number"] == 6
        assert pr["head_ref"] == "fix/issue-1-infra-foundation"
        assert pr["body"] == "Closes #1\n\nSets up DB schema and config."
        assert pr["review_comment_count"] == 1
        assert pr["review_comments"][0]["author"] == "bob"

    @patch("collector.github_poller.fetch_prs.gh_api")
    @patch("collector.github_poller.fetch_prs.run_gh_json")
    def test_correct_field_types(self, mock_list, mock_api):
        mock_list.return_value = PR_FIXTURE
        mock_api.return_value = []

        results = fetch_prs("owner/repo")

        for pr in results:
            assert isinstance(pr["number"], int)
            assert isinstance(pr["head_ref"], str)
            assert isinstance(pr["body"], str)
            assert isinstance(pr["review_comment_count"], int)
            assert isinstance(pr["review_comments"], list)

    @patch("collector.github_poller.fetch_prs.gh_api")
    @patch("collector.github_poller.fetch_prs.run_gh_json")
    def test_review_comments_on_api_failure(self, mock_list, mock_api):
        """If review comment fetch fails, returns empty list."""
        mock_list.return_value = [PR_FIXTURE[0]]
        mock_api.side_effect = Exception("timeout")

        results = fetch_prs("owner/repo")
        assert results[0]["review_comments"] == []
        assert results[0]["review_comment_count"] == 0
