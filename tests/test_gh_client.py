"""Tests for collector/github_poller/gh_client.py (C2-1).

Uses unittest.mock to patch subprocess.run — no live network calls.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from collector.github_poller.gh_client import GhCliError, run_gh, run_gh_json, gh_api, gh_graphql


class TestGhCliError:
    def test_attributes(self):
        err = GhCliError(["gh", "issue", "list"], 1, "not found")
        assert err.returncode == 1
        assert err.stderr == "not found"
        assert err.cmd == ["gh", "issue", "list"]
        assert "not found" in str(err)

    def test_is_exception(self):
        assert issubclass(GhCliError, Exception)


class TestRunGh:
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="hello")
        result = run_gh(["issue", "list"])
        assert result == "hello"
        mock_run.assert_called_once()

    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_raises_on_failure(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="rate limited"
        )
        with pytest.raises(GhCliError) as exc_info:
            run_gh(["issue", "list"], max_retries=0)
        assert exc_info.value.returncode == 1
        assert "rate limited" in exc_info.value.stderr

    @patch("collector.github_poller.gh_client.time.sleep")
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_retries_with_backoff(self, mock_run, mock_sleep):
        """Retries up to max_retries times before raising."""
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="error"
        )
        with pytest.raises(GhCliError):
            run_gh(["api", "/test"], max_retries=2, backoff_base=2.0)
        # 3 total attempts: initial + 2 retries
        assert mock_run.call_count == 3
        # 2 sleep calls (before retry 1 and retry 2)
        assert mock_sleep.call_count == 2

    @patch("collector.github_poller.gh_client.time.sleep")
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_succeeds_on_retry(self, mock_run, mock_sleep):
        """If first attempt fails but second succeeds, returns result."""
        mock_run.side_effect = [
            MagicMock(returncode=1, stdout="", stderr="error"),
            MagicMock(returncode=0, stdout="ok"),
        ]
        result = run_gh(["api", "/test"], max_retries=2)
        assert result == "ok"
        assert mock_run.call_count == 2


class TestRunGhJson:
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_parses_json(self, mock_run):
        data = [{"number": 1, "title": "test"}]
        mock_run.return_value = MagicMock(
            returncode=0, stdout=json.dumps(data)
        )
        result = run_gh_json(["issue", "list", "--json", "number,title"])
        assert result == data


class TestGhApi:
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_basic_get(self, mock_run):
        data = [{"id": 1}]
        mock_run.return_value = MagicMock(
            returncode=0, stdout=json.dumps(data)
        )
        result = gh_api("/repos/owner/repo/issues")
        assert result == data

    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_paginate_merges_arrays(self, mock_run):
        """When --paginate returns multiple JSON arrays, they are merged."""
        line1 = json.dumps([{"id": 1}])
        line2 = json.dumps([{"id": 2}])
        # Simulate concatenated output that is not valid single JSON
        mock_run.return_value = MagicMock(
            returncode=0, stdout=f"{line1}\n{line2}"
        )
        result = gh_api("/repos/owner/repo/issues", paginate=True)
        assert result == [{"id": 1}, {"id": 2}]


class TestGhGraphql:
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_basic_query_returns_parsed_data(self, mock_run):
        """gh_graphql returns parsed data from GraphQL response."""
        response = {"data": {"repository": {"issue": {"number": 1}}}}
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(response))
        result = gh_graphql("query { ... }", {"owner": "test", "name": "repo"})
        assert result == response

    @patch("collector.github_poller.gh_client.time.sleep")
    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_retries_on_failure(self, mock_run, mock_sleep):
        """gh_graphql retries with backoff like run_gh."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="rate limited")
        with pytest.raises(GhCliError):
            gh_graphql("query { ... }", {}, max_retries=1)
        # 2 total attempts: initial + 1 retry
        assert mock_run.call_count == 2

    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_string_vars_use_lowercase_f(self, mock_run):
        """String variables are passed with -f (untyped)."""
        mock_run.return_value = MagicMock(returncode=0, stdout='{"data":{}}')
        gh_graphql("query($owner: String!) { ... }", {"owner": "myorg"})
        cmd = mock_run.call_args[0][0]
        # Find the index of "-f" that precedes "owner=myorg"
        assert "-f" in cmd
        f_indices = [i for i, a in enumerate(cmd) if a == "-f"]
        owner_args = [cmd[i + 1] for i in f_indices if i + 1 < len(cmd)]
        assert any("owner=myorg" in arg for arg in owner_args)

    @patch("collector.github_poller.gh_client.subprocess.run")
    def test_non_string_vars_use_uppercase_f(self, mock_run):
        """Non-string variables (int, bool) are passed with -F (typed)."""
        mock_run.return_value = MagicMock(returncode=0, stdout='{"data":{}}')
        gh_graphql("query($n: Int!) { ... }", {"n": 42})
        cmd = mock_run.call_args[0][0]
        assert "-F" in cmd
        F_indices = [i for i, a in enumerate(cmd) if a == "-F"]
        n_args = [cmd[i + 1] for i in F_indices if i + 1 < len(cmd)]
        assert any("n=" in arg for arg in n_args)
