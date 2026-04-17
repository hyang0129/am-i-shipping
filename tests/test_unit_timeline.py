"""Tests for ``synthesis/unit_timeline.py`` (Epic #17 — Issue #37).

Snapshot-style assertions against the golden fixture. The renderer is a
pure function — same inputs produce byte-identical events — so these
tests pin both ordering and descriptions.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

from synthesis.unit_timeline import render_timeline


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"


def _fresh_fixture(tmp_path: Path) -> Path:
    dst = tmp_path / "golden.sqlite"
    shutil.copy(FIXTURE_SRC, dst)
    return dst


# ---------------------------------------------------------------------------
# Unit 1 — multi-session / multi-PR
# ---------------------------------------------------------------------------


def _unit_1_nodes():
    """Match the fixture's Unit 1 component."""
    return [
        ("n-u1-issue", "issue", "example/repo#201"),
        ("n-u1-pr-a", "pr", "example/repo#301"),
        ("n-u1-pr-b", "pr", "example/repo#302"),
        ("n-u1-sess-a", "session", "00000000-0000-0000-0000-000000000101"),
        ("n-u1-sess-b", "session", "00000000-0000-0000-0000-000000000102"),
    ]


class TestRenderTimelineUnit1:
    def test_event_count(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(_unit_1_nodes(), conn, conn)
        finally:
            conn.close()
        # 1 issue_opened + 1 issue_closed + 2 pr_opened + 2 pr_merged
        # + 2 session_start + 2 session_end = 10
        assert len(events) == 10

    def test_chronological_order(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(_unit_1_nodes(), conn, conn)
        finally:
            conn.close()
        timestamps = [e["timestamp"] for e in events]
        assert timestamps == sorted(timestamps)

    def test_snapshot(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(_unit_1_nodes(), conn, conn)
        finally:
            conn.close()

        # First event: issue opened at 2025-01-06T08:55:00Z
        assert events[0] == {
            "timestamp": "2025-01-06T08:55:00Z",
            "type": "issue_opened",
            "node_id": "n-u1-issue",
            "description": "Issue example/repo#201 opened: Unit 1 multi-session issue",
        }
        # Last event: issue closed at 2025-01-08T15:00:00Z
        assert events[-1] == {
            "timestamp": "2025-01-08T15:00:00Z",
            "type": "issue_closed",
            "node_id": "n-u1-issue",
            "description": "Issue example/repo#201 closed",
        }

    def test_session_and_pr_events_present(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(_unit_1_nodes(), conn, conn)
        finally:
            conn.close()
        types = [e["type"] for e in events]
        assert types.count("session_start") == 2
        assert types.count("session_end") == 2
        assert types.count("pr_opened") == 2
        assert types.count("pr_merged") == 2


# ---------------------------------------------------------------------------
# Unit 2 — abandoned (open issue + closed-unmerged PR, no sessions)
# ---------------------------------------------------------------------------


class TestRenderTimelineUnit2:
    def _unit_2_nodes(self):
        return [
            ("n-u2-issue", "issue", "example/repo#202"),
            ("n-u2-pr", "pr", "example/repo#303"),
        ]

    def test_no_merge_event_for_unmerged_pr(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(self._unit_2_nodes(), conn, conn)
        finally:
            conn.close()
        types = {e["type"] for e in events}
        # Unit 2's PR 303 never merged, issue 202 still open.
        assert "pr_merged" not in types
        assert "issue_closed" not in types
        assert "issue_opened" in types
        assert "pr_opened" in types


# ---------------------------------------------------------------------------
# Unit 3 — singleton session
# ---------------------------------------------------------------------------


class TestRenderTimelineUnit3:
    def test_only_session_events(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            events = render_timeline(
                [("n-u3-sess", "session", "00000000-0000-0000-0000-000000000303")],
                conn,
                conn,
            )
        finally:
            conn.close()
        assert [e["type"] for e in events] == ["session_start", "session_end"]
        assert events[0]["timestamp"] == "2025-01-07T16:00:00Z"
        assert events[1]["timestamp"] == "2025-01-07T16:20:00Z"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_two_calls_identical(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            a = render_timeline(_unit_1_nodes(), conn, conn)
            b = render_timeline(_unit_1_nodes(), conn, conn)
        finally:
            conn.close()
        assert a == b

    def test_input_order_does_not_matter(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            a = render_timeline(_unit_1_nodes(), conn, conn)
            b = render_timeline(list(reversed(_unit_1_nodes())), conn, conn)
        finally:
            conn.close()
        assert a == b

    def test_empty_nodes_returns_empty(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            assert render_timeline([], conn, conn) == []
        finally:
            conn.close()
