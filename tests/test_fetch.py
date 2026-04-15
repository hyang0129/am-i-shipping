"""Tests for collector/github_poller/fetch_issues.py and fetch_prs.py (C2-1).

Uses fixture JSON and mocked gh_client — no live network calls.
"""

from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from collector.github_poller.fetch_issues import (
    fetch_issues,
    fetch_issue_edit_history,
    fetch_issue_edit_history_batch,
    _extract_type_label,
)
from collector.github_poller.fetch_prs import fetch_prs, fetch_pr_edit_history
from collector.github_poller.gh_client import GhCliError


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
        "id": 12345,
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
        "id": 67890,
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
        mock_api.side_effect = GhCliError(["gh", "api"], 1, "rate limited")

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
        mock_api.side_effect = GhCliError(["gh", "api"], 1, "timeout")

        results = fetch_prs("owner/repo")
        assert results[0]["review_comments"] == []
        assert results[0]["review_comment_count"] == 0


class TestCommentIds:
    @patch("collector.github_poller.fetch_issues.gh_api")
    @patch("collector.github_poller.fetch_issues.run_gh_json")
    def test_issue_comment_has_id(self, mock_list, mock_api):
        """Issue comment dicts include the 'id' key from the REST response."""
        mock_list.return_value = [ISSUE_FIXTURE[0]]
        mock_api.return_value = ISSUE_COMMENTS_FIXTURE

        results = fetch_issues("owner/repo")
        assert results[0]["comments"][0]["id"] == 12345

    @patch("collector.github_poller.fetch_issues.gh_api")
    @patch("collector.github_poller.fetch_issues.run_gh_json")
    def test_issue_has_updated_at(self, mock_list, mock_api):
        """Issue dicts include 'updated_at' from the REST updatedAt field."""
        fixture = [{**ISSUE_FIXTURE[0], "updatedAt": "2024-01-20T16:00:00Z"}]
        mock_list.return_value = fixture
        mock_api.return_value = []

        results = fetch_issues("owner/repo")
        assert results[0]["updated_at"] == "2024-01-20T16:00:00Z"

    @patch("collector.github_poller.fetch_prs.gh_api")
    @patch("collector.github_poller.fetch_prs.run_gh_json")
    def test_pr_review_comment_has_id(self, mock_list, mock_api):
        """PR review comment dicts include the 'id' key from the REST response."""
        mock_list.return_value = [PR_FIXTURE[0]]
        mock_api.return_value = PR_REVIEW_COMMENTS_FIXTURE

        results = fetch_prs("owner/repo")
        assert results[0]["review_comments"][0]["id"] == 67890

    @patch("collector.github_poller.fetch_prs.gh_api")
    @patch("collector.github_poller.fetch_prs.run_gh_json")
    def test_pr_has_updated_at(self, mock_list, mock_api):
        """PR dicts include 'updated_at' from the REST updatedAt field."""
        fixture = [{**PR_FIXTURE[0], "updatedAt": "2024-01-22T10:00:00Z"}]
        mock_list.return_value = fixture
        mock_api.return_value = []

        results = fetch_prs("owner/repo")
        assert results[0]["updated_at"] == "2024-01-22T10:00:00Z"


class TestFetchIssueEditHistory:
    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_returns_body_and_comment_edits(self, mock_gql):
        """fetch_issue_edit_history returns body_edits and comment_edits lists."""
        mock_gql.return_value = {
            "data": {"repository": {"issue": {
                "userContentEdits": {"nodes": [
                    {
                        "editedAt": "2024-01-20T16:00:00Z",
                        "diff": "- old\n+ new",
                        "editor": {"login": "alice"},
                    }
                ]},
                "comments": {"nodes": [
                    {
                        "databaseId": 123,
                        "userContentEdits": {"nodes": [
                            {
                                "editedAt": "2024-01-20T17:00:00Z",
                                "diff": "- old comment\n+ new comment",
                                "editor": {"login": "bob"},
                            }
                        ]},
                    }
                ]},
            }}}
        }

        result = fetch_issue_edit_history("owner/repo", 1)
        assert len(result["body_edits"]) == 1
        assert result["body_edits"][0]["editor"] == "alice"
        assert result["body_edits"][0]["edited_at"] == "2024-01-20T16:00:00Z"
        assert len(result["comment_edits"]) == 1
        assert result["comment_edits"][0]["comment_id"] == 123
        assert result["comment_edits"][0]["editor"] == "bob"

    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_graceful_empty_on_graphql_error(self, mock_gql):
        """fetch_issue_edit_history returns empty dict on GraphQL error response."""
        mock_gql.return_value = {"errors": [{"message": "not found"}]}

        result = fetch_issue_edit_history("owner/repo", 999)
        assert result == {}

    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_graceful_empty_on_exception(self, mock_gql):
        """fetch_issue_edit_history returns empty dict when gh_graphql raises."""
        mock_gql.side_effect = GhCliError(["gh", "api", "graphql"], 1, "network error")

        result = fetch_issue_edit_history("owner/repo", 1)
        assert result == {}

    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_empty_edits_when_no_edits(self, mock_gql):
        """Returns empty lists for body_edits and comment_edits when there are none."""
        mock_gql.return_value = {
            "data": {"repository": {"issue": {
                "userContentEdits": {"nodes": []},
                "comments": {"nodes": []},
            }}}
        }

        result = fetch_issue_edit_history("owner/repo", 1)
        assert result["body_edits"] == []
        assert result["comment_edits"] == []


class TestFetchIssueEditHistoryBatch:
    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_chunks_large_list(self, mock_gql):
        """fetch_issue_edit_history_batch makes multiple calls for >20 issues."""
        mock_gql.return_value = {
            "data": {"repository": {
                f"issue{i}": {
                    "number": i + 1,
                    "userContentEdits": {"nodes": []},
                    "comments": {"nodes": []},
                }
                for i in range(20)
            }}
        }

        issue_numbers = list(range(1, 42))  # 41 issues -> 3 calls (20+20+1)
        result = fetch_issue_edit_history_batch("owner/repo", issue_numbers)

        assert mock_gql.call_count == 3
        assert len(result) == 41

    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_returns_mapping_of_issue_number_to_edits(self, mock_gql):
        """Batch result maps issue_number -> edit history dict."""
        mock_gql.return_value = {
            "data": {"repository": {
                "issue0": {
                    "number": 1,
                    "userContentEdits": {"nodes": [
                        {"editedAt": "2024-01-20T16:00:00Z", "diff": "x", "editor": {"login": "alice"}}
                    ]},
                    "comments": {"nodes": []},
                },
                "issue1": {
                    "number": 2,
                    "userContentEdits": {"nodes": []},
                    "comments": {"nodes": []},
                },
            }}
        }

        result = fetch_issue_edit_history_batch("owner/repo", [1, 2])
        assert 1 in result
        assert 2 in result
        assert len(result[1]["body_edits"]) == 1
        assert result[1]["body_edits"][0]["editor"] == "alice"
        assert result[2]["body_edits"] == []

    @patch("collector.github_poller.fetch_issues.gh_graphql")
    def test_skips_chunk_on_graphql_error(self, mock_gql):
        """Batch silently skips chunks that return GraphQL errors."""
        mock_gql.return_value = {"errors": [{"message": "server error"}]}

        result = fetch_issue_edit_history_batch("owner/repo", [1, 2, 3])
        assert result == {}


class TestFetchPrEditHistory:
    @patch("collector.github_poller.fetch_prs.gh_graphql")
    def test_returns_body_and_review_comment_edits(self, mock_gql):
        """fetch_pr_edit_history returns body_edits and review_comment_edits."""
        mock_gql.return_value = {
            "data": {"repository": {"pullRequest": {
                "userContentEdits": {"nodes": [
                    {
                        "editedAt": "2024-01-21T10:00:00Z",
                        "diff": "- old pr\n+ new pr",
                        "editor": {"login": "carol"},
                    }
                ]},
                "reviews": {"nodes": [
                    {
                        "comments": {"nodes": [
                            {
                                "databaseId": 456,
                                "userContentEdits": {"nodes": [
                                    {
                                        "editedAt": "2024-01-21T11:00:00Z",
                                        "diff": "- old review\n+ new review",
                                        "editor": {"login": "dave"},
                                    }
                                ]},
                            }
                        ]}
                    }
                ]},
            }}}
        }

        result = fetch_pr_edit_history("owner/repo", 6)
        assert len(result["body_edits"]) == 1
        assert result["body_edits"][0]["editor"] == "carol"
        assert len(result["review_comment_edits"]) == 1
        assert result["review_comment_edits"][0]["comment_id"] == 456
        assert result["review_comment_edits"][0]["editor"] == "dave"

    @patch("collector.github_poller.fetch_prs.gh_graphql")
    def test_graceful_empty_on_graphql_error(self, mock_gql):
        """fetch_pr_edit_history returns empty dict on GraphQL error response."""
        mock_gql.return_value = {"errors": [{"message": "not found"}]}

        result = fetch_pr_edit_history("owner/repo", 999)
        assert result == {}

    @patch("collector.github_poller.fetch_prs.gh_graphql")
    def test_graceful_empty_on_exception(self, mock_gql):
        """fetch_pr_edit_history returns empty dict when gh_graphql raises."""
        mock_gql.side_effect = GhCliError(["gh", "api", "graphql"], 1, "timeout")

        result = fetch_pr_edit_history("owner/repo", 6)
        assert result == {}
