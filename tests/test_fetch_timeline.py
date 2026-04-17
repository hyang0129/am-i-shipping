"""Tests for collector/github_poller/fetch_timeline.py (Epic #17, E-2).

Uses mocked ``gh_graphql`` — no live network calls.
"""

from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest

from am_i_shipping.db import init_github_db
from collector.github_poller.fetch_timeline import (
    TIMELINE_EVENT_TYPES,
    _build_batch_query,
    _event_id_from_node,
    _normalize_node,
    fetch_and_store_issue_timelines,
    fetch_issue_timeline_batch,
)
from collector.github_poller.gh_client import BudgetExhausted


# ---------------------------------------------------------------------------
# Fixture builders — GraphQL response shapes as returned by gh_graphql.
# ---------------------------------------------------------------------------


def _gql_response_for_chunk(chunk: list[int], events_by_number: dict[int, list[dict]]):
    """Build a mock GraphQL response that covers one chunk of aliased queries."""
    repo_block = {}
    for i, num in enumerate(chunk):
        nodes = events_by_number.get(num, [])
        repo_block[f"issue{i}"] = {
            "number": num,
            "timelineItems": {"nodes": nodes},
        }
    return {
        "data": {
            "rateLimit": {"cost": 1, "remaining": 4999},
            "repository": repo_block,
        }
    }


_ASSIGNED_NODE = {
    "__typename": "AssignedEvent",
    "id": "AE_111",
    "createdAt": "2024-01-15T10:00:00Z",
    "actor": {"login": "alice"},
    "assignee": {"login": "bob"},
}

_LABELED_NODE = {
    "__typename": "LabeledEvent",
    "id": "LE_222",
    "createdAt": "2024-01-15T11:00:00Z",
    "actor": {"login": "carol"},
    "label": {"name": "bug"},
}

_CROSS_REF_NODE = {
    "__typename": "CrossReferencedEvent",
    "id": "CRE_333",
    "createdAt": "2024-01-16T09:00:00Z",
    "actor": {"login": "dave"},
    "source": {"number": 42, "repository": {"nameWithOwner": "owner/repo"}},
}


class TestNodeNormalization:
    def test_event_id_stable_across_calls(self):
        a = _event_id_from_node({"id": "LE_222"})
        b = _event_id_from_node({"id": "LE_222"})
        assert a == b
        assert isinstance(a, int)
        assert a > 0

    def test_event_id_differs_across_relay_ids(self):
        assert _event_id_from_node({"id": "AE_111"}) != _event_id_from_node({"id": "AE_112"})

    def test_event_id_missing(self):
        assert _event_id_from_node({}) is None

    def test_normalize_assigned(self):
        row = _normalize_node(7, _ASSIGNED_NODE)
        assert row is not None
        assert row["issue_number"] == 7
        assert row["event_type"] == "assigned"
        assert row["actor"] == "alice"
        assert row["created_at"] == "2024-01-15T10:00:00Z"
        payload = json.loads(row["payload_json"])
        assert payload["__typename"] == "AssignedEvent"
        assert payload["assignee"]["login"] == "bob"

    def test_normalize_labeled(self):
        row = _normalize_node(7, _LABELED_NODE)
        assert row["event_type"] == "labeled"
        assert row["actor"] == "carol"

    def test_normalize_cross_referenced(self):
        row = _normalize_node(7, _CROSS_REF_NODE)
        assert row["event_type"] == "cross-referenced"
        payload = json.loads(row["payload_json"])
        assert payload["source"]["number"] == 42

    def test_normalize_unknown_type_returns_none(self):
        assert _normalize_node(7, {"__typename": "SomeOtherEvent", "id": "X"}) is None


class TestBuildBatchQuery:
    def test_query_includes_all_event_types(self):
        q = _build_batch_query([1, 2, 3])
        for ev in TIMELINE_EVENT_TYPES:
            assert ev in q

    def test_query_uses_aliases(self):
        q = _build_batch_query([10, 20, 30])
        assert "issue0: issue(number: 10)" in q
        assert "issue1: issue(number: 20)" in q
        assert "issue2: issue(number: 30)" in q


class TestFetchIssueTimelineBatch:
    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_empty_input_returns_empty(self, mock_gql):
        result = fetch_issue_timeline_batch("owner/repo", [])
        assert result == {}
        assert mock_gql.call_count == 0

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_single_chunk_normalizes_events(self, mock_gql):
        mock_gql.return_value = _gql_response_for_chunk(
            [1, 2],
            {1: [_ASSIGNED_NODE, _LABELED_NODE], 2: []},
        )
        result = fetch_issue_timeline_batch("owner/repo", [1, 2])
        assert mock_gql.call_count == 1
        assert set(result.keys()) == {1, 2}
        assert [e["event_type"] for e in result[1]] == ["assigned", "labeled"]
        assert result[2] == []

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_chunks_greater_than_twenty(self, mock_gql):
        """25 issues should trigger two GraphQL calls (20 + 5)."""
        issue_numbers = list(range(1, 26))

        calls: list[int] = []

        def _side_effect(query, variables):
            calls.append(1)
            # First call: issues 1..20 (aliases issue0..issue19).
            # Second call: issues 21..25 (aliases issue0..issue4).
            if len(calls) == 1:
                return _gql_response_for_chunk(list(range(1, 21)), {})
            return _gql_response_for_chunk(list(range(21, 26)), {})

        mock_gql.side_effect = _side_effect

        result = fetch_issue_timeline_batch("owner/repo", issue_numbers)
        assert mock_gql.call_count == 2
        assert set(result.keys()) == set(issue_numbers)

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_graphql_error_skips_chunk(self, mock_gql):
        """A chunk that raises is logged and skipped; other chunks still land."""
        issue_numbers = list(range(1, 26))

        calls: list[int] = []

        def _side_effect(query, variables):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("first chunk explodes")
            return _gql_response_for_chunk(list(range(21, 26)), {})

        mock_gql.side_effect = _side_effect

        result = fetch_issue_timeline_batch("owner/repo", issue_numbers)
        # Missing issues from chunk 1, present from chunk 2.
        assert all(n not in result for n in range(1, 21))
        assert all(n in result for n in range(21, 26))

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_deleted_issue_gets_empty_list(self, mock_gql):
        """When GraphQL returns null for an alias (transferred/deleted), we
        record an empty event list rather than omitting the issue."""
        mock_gql.return_value = {
            "data": {
                "rateLimit": {"cost": 1, "remaining": 4999},
                "repository": {
                    "issue0": None,
                    "issue1": {"number": 2, "timelineItems": {"nodes": []}},
                },
            }
        }
        result = fetch_issue_timeline_batch("owner/repo", [1, 2])
        assert result == {1: [], 2: []}

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_budget_exhausted_propagates(self, mock_gql):
        """Invariant: BudgetExhausted MUST escape fetch_issue_timeline_batch so
        the outer poll cycle can stop cleanly. A generic except-Exception would
        silently continue iterating chunks while the budget stays pinned — this
        negative-assertion test guards against that regression (see F-1)."""
        mock_gql.side_effect = BudgetExhausted(100, 100, 600.0)
        with pytest.raises(BudgetExhausted):
            fetch_issue_timeline_batch("owner/repo", [1, 2, 3])
        # Exactly one chunk attempted — we did NOT continue past the budget cap.
        assert mock_gql.call_count == 1

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_budget_exhausted_propagates_through_store(self, mock_gql, tmp_path):
        """fetch_and_store_issue_timelines must also re-raise BudgetExhausted
        so _fetch_timeline_step's handler in run.py observes it."""
        mock_gql.side_effect = BudgetExhausted(100, 100, 600.0)
        db = tmp_path / "github.db"
        init_github_db(db)
        with pytest.raises(BudgetExhausted):
            fetch_and_store_issue_timelines("owner/repo", [1], db)


class TestFetchAndStoreIssueTimelines:
    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_persists_events(self, mock_gql, tmp_path):
        mock_gql.return_value = _gql_response_for_chunk(
            [1], {1: [_ASSIGNED_NODE, _LABELED_NODE, _CROSS_REF_NODE]},
        )
        db = tmp_path / "github.db"
        init_github_db(db)

        fetch_and_store_issue_timelines("owner/repo", [1], db)

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT event_type, actor FROM timeline_events "
                "WHERE repo = 'owner/repo' AND issue_number = 1 "
                "ORDER BY event_type"
            ).fetchall()
        finally:
            conn.close()
        assert set(rows) == {
            ("assigned", "alice"),
            ("cross-referenced", "dave"),
            ("labeled", "carol"),
        }

    @patch("collector.github_poller.fetch_timeline.gh_graphql")
    def test_idempotent(self, mock_gql, tmp_path):
        mock_gql.return_value = _gql_response_for_chunk([1], {1: [_ASSIGNED_NODE]})
        db = tmp_path / "github.db"
        init_github_db(db)

        fetch_and_store_issue_timelines("owner/repo", [1], db)
        fetch_and_store_issue_timelines("owner/repo", [1], db)

        conn = sqlite3.connect(str(db))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM timeline_events "
                "WHERE repo = 'owner/repo' AND issue_number = 1"
            ).fetchone()[0]
        finally:
            conn.close()
        assert count == 1
