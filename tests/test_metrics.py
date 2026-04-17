"""Unit tests for ``synthesis/metrics.py`` (Epic #17 — Issue #37).

Each metric is tested with hand-crafted inputs so failures point at the
calculation, not at fixture wiring. The fixture-wired end-to-end
assertions live in ``tests/test_unit_identifier.py``.
"""

from __future__ import annotations

import sqlite3

import pytest

from synthesis import metrics


# ---------------------------------------------------------------------------
# elapsed_days
# ---------------------------------------------------------------------------


class TestElapsedDays:
    def test_two_timestamps_one_day_apart(self):
        assert metrics.elapsed_days([
            "2025-01-06T09:00:00Z",
            "2025-01-07T09:00:00Z",
        ]) == pytest.approx(1.0)

    def test_with_none_values_ignored(self):
        assert metrics.elapsed_days([
            None,
            "2025-01-06T09:00:00Z",
            "",
            "2025-01-06T21:00:00Z",
            None,
        ]) == pytest.approx(0.5)

    def test_single_timestamp_returns_zero(self):
        assert metrics.elapsed_days(["2025-01-06T09:00:00Z"]) == 0.0

    def test_empty_returns_zero(self):
        assert metrics.elapsed_days([]) == 0.0

    def test_all_none_returns_zero(self):
        assert metrics.elapsed_days([None, None, ""]) == 0.0

    def test_malformed_skipped(self):
        # "not-a-date" is dropped; valid pair still produces a span.
        assert metrics.elapsed_days([
            "not-a-date",
            "2025-01-06T00:00:00Z",
            "2025-01-08T00:00:00Z",
        ]) == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# dark_time_pct
# ---------------------------------------------------------------------------


class TestDarkTimePct:
    def test_two_disjoint_sessions(self):
        # Two 1-hour sessions within a 10-hour window → 8h dark / 10h
        intervals = [
            ("2025-01-06T00:00:00Z", "2025-01-06T01:00:00Z"),
            ("2025-01-06T09:00:00Z", "2025-01-06T10:00:00Z"),
        ]
        assert metrics.dark_time_pct(intervals) == pytest.approx(0.8)

    def test_single_session_returns_zero(self):
        """ADR Decision 3: dark_time has no meaning for a single session."""
        assert metrics.dark_time_pct([
            ("2025-01-06T09:00:00Z", "2025-01-06T10:00:00Z"),
        ]) == 0.0

    def test_empty_returns_zero(self):
        assert metrics.dark_time_pct([]) == 0.0

    def test_missing_timestamps_skipped(self):
        intervals = [
            (None, "2025-01-06T01:00:00Z"),
            ("2025-01-06T02:00:00Z", None),
            ("2025-01-06T09:00:00Z", "2025-01-06T10:00:00Z"),
        ]
        # After dropping the broken two, one valid interval → 0.0
        assert metrics.dark_time_pct(intervals) == 0.0

    def test_overlapping_sessions_clamped(self):
        # Two overlapping 1h sessions cover only 1.5h of real time;
        # active = 2h, span = 1.5h, ratio = 1 - 4/3 = negative → clamp 0
        intervals = [
            ("2025-01-06T00:00:00Z", "2025-01-06T01:00:00Z"),
            ("2025-01-06T00:30:00Z", "2025-01-06T01:30:00Z"),
        ]
        assert metrics.dark_time_pct(intervals) == 0.0

    def test_malformed_order_skipped(self):
        # end before start should be rejected.
        intervals = [
            ("2025-01-06T02:00:00Z", "2025-01-06T01:00:00Z"),
            ("2025-01-06T09:00:00Z", "2025-01-06T10:00:00Z"),
        ]
        # One valid after filtering → 0.0
        assert metrics.dark_time_pct(intervals) == 0.0


# ---------------------------------------------------------------------------
# total_reprompts
# ---------------------------------------------------------------------------


class TestTotalReprompts:
    @pytest.fixture
    def sessions_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE sessions (session_uuid TEXT PRIMARY KEY, reprompt_count INTEGER)"
        )
        conn.executemany(
            "INSERT INTO sessions VALUES (?, ?)",
            [("a", 2), ("b", 5), ("c", None), ("d", 0)],
        )
        conn.commit()
        yield conn
        conn.close()

    def test_sum_across_rows(self, sessions_conn):
        assert metrics.total_reprompts(["a", "b"], sessions_conn) == 7

    def test_null_coerced_to_zero(self, sessions_conn):
        assert metrics.total_reprompts(["a", "c"], sessions_conn) == 2

    def test_missing_rows_ignored(self, sessions_conn):
        assert metrics.total_reprompts(["a", "does-not-exist"], sessions_conn) == 2

    def test_empty_returns_zero(self, sessions_conn):
        assert metrics.total_reprompts([], sessions_conn) == 0


# ---------------------------------------------------------------------------
# review_cycles
# ---------------------------------------------------------------------------


class TestReviewCycles:
    @pytest.fixture
    def github_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE pull_requests ("
            "repo TEXT, pr_number INTEGER, "
            "review_comments_json TEXT, push_count INTEGER, "
            "PRIMARY KEY (repo, pr_number))"
        )
        conn.executemany(
            "INSERT INTO pull_requests VALUES (?, ?, ?, ?)",
            [
                ("r", 1, '[{"id": 1}, {"id": 2}, {"id": 3}]', 0),
                ("r", 2, "[]", 4),
                ("r", 3, None, 2),
                ("r", 4, "malformed[", 1),
                ("r", 5, "[]", 0),
            ],
        )
        conn.commit()
        yield conn
        conn.close()

    def test_review_comments_preferred(self, github_conn):
        assert metrics.review_cycles([("r", 1)], github_conn) == 3

    def test_fallback_to_push_count(self, github_conn):
        # empty array → fall back to push_count=4
        assert metrics.review_cycles([("r", 2)], github_conn) == 4

    def test_null_review_comments_falls_back(self, github_conn):
        assert metrics.review_cycles([("r", 3)], github_conn) == 2

    def test_malformed_json_falls_back(self, github_conn):
        assert metrics.review_cycles([("r", 4)], github_conn) == 1

    def test_both_empty_returns_zero(self, github_conn):
        assert metrics.review_cycles([("r", 5)], github_conn) == 0

    def test_multiple_prs_sum(self, github_conn):
        assert metrics.review_cycles(
            [("r", 1), ("r", 2)], github_conn
        ) == 3 + 4

    def test_missing_pr_ignored(self, github_conn):
        assert metrics.review_cycles(
            [("r", 1), ("r", 999)], github_conn
        ) == 3

    def test_empty_returns_zero(self, github_conn):
        assert metrics.review_cycles([], github_conn) == 0
