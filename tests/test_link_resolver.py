"""Tests for collector/github_poller/link_resolver.py (C2-2).

No live data or DB dependency.
"""

from __future__ import annotations

import pytest

from collector.github_poller.link_resolver import resolve_link


class TestBranchNameResolution:
    """Strategy 1: branch name regex."""

    def test_fix_slash_number(self):
        assert resolve_link("fix/42-add-logging", "") == 42

    def test_feature_slash_number(self):
        assert resolve_link("feature/123-new-widget", "") == 123

    def test_bugfix_slash_number(self):
        assert resolve_link("bugfix/7-crash-on-start", "") == 7

    def test_issue_dash_number(self):
        assert resolve_link("issue-99", "") == 99

    def test_number_dash_slug(self):
        assert resolve_link("42-quick-fix", "") == 42

    def test_no_match(self):
        assert resolve_link("main", "") is None

    def test_empty_branch(self):
        assert resolve_link("", "") is None


class TestBodyScanResolution:
    """Strategy 2: body keyword scan."""

    def test_closes_hash(self):
        assert resolve_link("", "Closes #42") == 42

    def test_fixes_hash(self):
        assert resolve_link("", "fixes #10") == 10

    def test_resolves_hash(self):
        assert resolve_link("", "Resolves #7") == 7

    def test_closed_hash(self):
        assert resolve_link("", "closed #99") == 99

    def test_case_insensitive(self):
        assert resolve_link("", "FIXES #123") == 123

    def test_no_keyword(self):
        assert resolve_link("", "This PR adds a new feature.") is None

    def test_empty_body(self):
        assert resolve_link("", "") is None


class TestCombinedResolution:
    """Both strategies present — branch wins."""

    def test_branch_wins_over_body(self):
        result = resolve_link("fix/42-slug", "Closes #99")
        assert result == 42  # branch takes priority

    def test_body_fallback_when_branch_unmatched(self):
        result = resolve_link("main", "Closes #99")
        assert result == 99

    def test_neither_match(self):
        result = resolve_link("main", "Just a description")
        assert result is None
