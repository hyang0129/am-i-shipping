"""Tests for collector/github_poller/cursor.py (C2-4).

Uses temporary SQLite databases.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from collector.github_poller.cursor import read_cursor, compute_since, advance_cursor


class TestReadCursor:
    def test_no_db_returns_none(self, tmp_path):
        db = tmp_path / "github.db"
        assert read_cursor("owner/repo", db) is None

    def test_no_cursor_returns_none(self, tmp_path):
        """DB exists but no cursor row for this repo."""
        db = tmp_path / "github.db"
        from am_i_shipping.db import init_github_db
        init_github_db(db)
        assert read_cursor("owner/repo", db) is None

    def test_reads_existing_cursor(self, tmp_path):
        db = tmp_path / "github.db"
        advance_cursor("owner/repo", db)
        result = read_cursor("owner/repo", db)
        assert result is not None
        # Should be today's date
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert result == today


class TestComputeSince:
    def test_with_cursor_returns_cursor(self):
        assert compute_since("2024-01-15") == "2024-01-15"

    def test_without_cursor_returns_backfill(self):
        result = compute_since(None, backfill_days=90)
        expected = (
            datetime.now(timezone.utc) - timedelta(days=90)
        ).strftime("%Y-%m-%d")
        assert result == expected

    def test_custom_backfill_days(self):
        result = compute_since(None, backfill_days=30)
        expected = (
            datetime.now(timezone.utc) - timedelta(days=30)
        ).strftime("%Y-%m-%d")
        assert result == expected


class TestAdvanceCursor:
    def test_creates_cursor(self, tmp_path):
        db = tmp_path / "github.db"
        advance_cursor("owner/repo", db)
        result = read_cursor("owner/repo", db)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert result == today

    def test_updates_existing_cursor(self, tmp_path):
        """Running advance twice overwrites the cursor."""
        db = tmp_path / "github.db"
        advance_cursor("owner/repo", db)
        advance_cursor("owner/repo", db)  # should not fail
        result = read_cursor("owner/repo", db)
        assert result is not None

    def test_different_repos_independent(self, tmp_path):
        """Each repo has its own cursor."""
        db = tmp_path / "github.db"
        advance_cursor("owner/repo-a", db)
        assert read_cursor("owner/repo-a", db) is not None
        assert read_cursor("owner/repo-b", db) is None
