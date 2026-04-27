"""Tests for ``synthesis/unit_identifier.py`` (Epic #17 — Issue #37).

Uses the committed golden fixture
(``tests/fixtures/synthesis/golden.sqlite``). The fixture ships with
pre-populated ``units`` rows that represent the *ground truth topology*
(two connected components (session-only components are dropped per Issue #66)); these tests truncate that pre-fill and
re-derive the same two components via ``identify_units``.

All assertions pin ``now=datetime(2025, 1, 13)`` — one week after
unit 1's last activity, ~26 days after unit 2's — so the
status-abandonment logic is deterministic across CI runs.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from synthesis.unit_identifier import identify_units, _UnionFind, _unit_id_from_nodes


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"
EXPECTED_PATH = Path(__file__).parent / "fixtures" / "synthesis" / "expected_units.json"

# Pinned "now" matching ``expected_units.json``. 2025-01-13 is 5 days
# after unit 1's last event, 33 days after unit 2's last event.
PINNED_NOW = datetime(2025, 1, 13, 0, 0, 0)
WEEK_START = "2025-01-06"


def _fresh_fixture(tmp_path: Path) -> Path:
    """Return a writable copy of the golden fixture with ``units`` cleared.

    The fixture's committed ``units`` rows are ground-truth documentation
    (three components, known topology). For tests that exercise the
    identifier we truncate them so re-population is visible.
    """
    dst = tmp_path / "golden.sqlite"
    shutil.copy(FIXTURE_SRC, dst)
    conn = sqlite3.connect(str(dst))
    try:
        conn.execute("DELETE FROM units")
        conn.commit()
    finally:
        conn.close()
    return dst


# ---------------------------------------------------------------------------
# Union-find sanity
# ---------------------------------------------------------------------------


class TestUnionFind:
    def test_singletons_stay_separate(self):
        uf = _UnionFind()
        uf.add("a")
        uf.add("b")
        uf.add("c")
        assert uf.components() == [["a"], ["b"], ["c"]]

    def test_chain_merges(self):
        uf = _UnionFind()
        uf.union("a", "b")
        uf.union("b", "c")
        assert uf.components() == [["a", "b", "c"]]

    def test_two_components(self):
        uf = _UnionFind()
        uf.union("a", "b")
        uf.union("c", "d")
        assert uf.components() == [["a", "b"], ["c", "d"]]

    def test_idempotent_union(self):
        uf = _UnionFind()
        uf.union("a", "b")
        uf.union("a", "b")  # second call is a no-op
        assert uf.components() == [["a", "b"]]


class TestUnitIdGeneration:
    def test_id_is_deterministic(self):
        a = _unit_id_from_nodes(["a", "b", "c"])
        b = _unit_id_from_nodes(["c", "b", "a"])  # order independent
        assert a == b
        assert len(a) == 16

    def test_id_differs_by_membership(self):
        assert _unit_id_from_nodes(["a", "b"]) != _unit_id_from_nodes(["a", "c"])


# ---------------------------------------------------------------------------
# Fixture-level assertions
# ---------------------------------------------------------------------------


class TestIdentifyUnitsFixture:
    def test_two_units_produced(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        inserted = identify_units(
            db, db, WEEK_START, now=PINNED_NOW,
        )
        assert inserted == 2

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT unit_id, root_node_type, root_node_id, status "
                "FROM units WHERE week_start = ? "
                "ORDER BY unit_id",
                (WEEK_START,),
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 2

    def test_expected_unit_ids_and_membership(self, tmp_path):
        """Unit IDs and their node membership match the committed snapshot."""
        db = _fresh_fixture(tmp_path)
        identify_units(db, db, WEEK_START, now=PINNED_NOW)

        expected = json.loads(EXPECTED_PATH.read_text())
        expected_ids = sorted(u["unit_id"] for u in expected["units"])

        conn = sqlite3.connect(str(db))
        try:
            actual_ids = sorted(r[0] for r in conn.execute(
                "SELECT unit_id FROM units WHERE week_start = ?",
                (WEEK_START,),
            ).fetchall())
        finally:
            conn.close()
        assert actual_ids == expected_ids

    def test_metric_values_match_snapshot(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        identify_units(db, db, WEEK_START, now=PINNED_NOW)
        expected = {
            u["unit_id"]: u
            for u in json.loads(EXPECTED_PATH.read_text())["units"]
        }

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT unit_id, root_node_type, root_node_id, "
                "elapsed_days, dark_time_pct, total_reprompts, "
                "review_cycles, status "
                "FROM units WHERE week_start = ?",
                (WEEK_START,),
            ).fetchall()
        finally:
            conn.close()

        for unit_id, rtype, rid, elapsed, dark, reprompts, cycles, status in rows:
            exp = expected[unit_id]
            assert rtype == exp["root_node_type"], unit_id
            assert rid == exp["root_node_id"], unit_id
            assert elapsed == pytest.approx(exp["elapsed_days"]), unit_id
            assert dark == pytest.approx(exp["dark_time_pct"]), unit_id
            assert reprompts == exp["total_reprompts"], unit_id
            assert cycles == exp["review_cycles"], unit_id
            assert status == exp["status"], unit_id

    def test_unit_ids_deterministic_across_runs(self, tmp_path):
        """Two fresh runs produce identical unit IDs in the same order."""
        first = _fresh_fixture(tmp_path)
        identify_units(first, first, WEEK_START, now=PINNED_NOW)

        second_dir = tmp_path / "run2"
        second_dir.mkdir()
        second = _fresh_fixture(second_dir)
        identify_units(second, second, WEEK_START, now=PINNED_NOW)

        def _ids(path: Path) -> list[str]:
            conn = sqlite3.connect(str(path))
            try:
                return [r[0] for r in conn.execute(
                    "SELECT unit_id FROM units WHERE week_start = ? ORDER BY unit_id",
                    (WEEK_START,),
                ).fetchall()]
            finally:
                conn.close()

        assert _ids(first) == _ids(second)

    def test_rerun_is_no_op(self, tmp_path):
        """Second identify_units for the same week_start inserts 0 rows."""
        db = _fresh_fixture(tmp_path)
        first_inserted = identify_units(db, db, WEEK_START, now=PINNED_NOW)
        second_inserted = identify_units(db, db, WEEK_START, now=PINNED_NOW)
        assert first_inserted == 2
        assert second_inserted == 0

        # Row count didn't change.
        conn = sqlite3.connect(str(db))
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM units WHERE week_start = ?",
                (WEEK_START,),
            ).fetchone()[0]
        finally:
            conn.close()
        assert n == 2

    def test_rerun_preserves_existing_rows(self, tmp_path):
        """Pre-existing units for the same (week, unit_id) are preserved.

        We seed a row with a bogus metric value; after identify_units
        runs, that value MUST still be there — append-only semantics.
        """
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            # Pre-seed the singleton unit with sentinel values. Use an
            # explicit column list so the positional tuple keeps working
            # even when later migrations (e.g. Sub-Issue 5's
            # outlier_flags / abandonment_flag) extend the table.
            conn.execute(
                "INSERT INTO units "
                "(week_start, unit_id, root_node_type, root_node_id, "
                " elapsed_days, dark_time_pct, total_reprompts, "
                " review_cycles, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (WEEK_START, "0e5eb65932c9f5f7", "session", "n-u3-sess",
                 999.0, 0.5, 42, 7, "sentinel"),
            )
            conn.commit()
        finally:
            conn.close()

        # After the pre-seed for the singleton (unit_id=0e5eb65932c9f5f7),
        # identify_units should insert only the other two components —
        # the seeded row is preserved via INSERT OR IGNORE. Pinning the
        # return value here closes the gap where a silent overwrite +
        # re-insert would still make the row-value assertions below pass.
        inserted = identify_units(db, db, WEEK_START, now=PINNED_NOW)
        assert inserted == 2

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT elapsed_days, status FROM units "
                "WHERE week_start = ? AND unit_id = ?",
                (WEEK_START, "0e5eb65932c9f5f7"),
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == 999.0  # sentinel preserved
        assert row[1] == "sentinel"  # sentinel preserved

    def test_singleton_handling(self, tmp_path):
        """The singleton session is dropped (#66): components without an issue/PR anchor produce no units row."""
        db = _fresh_fixture(tmp_path)
        identify_units(db, db, WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT root_node_type, root_node_id, dark_time_pct "
                "FROM units WHERE week_start = ? AND unit_id = ?",
                (WEEK_START, "0e5eb65932c9f5f7"),
            ).fetchone()
        finally:
            conn.close()
        assert row is None

    def test_empty_graph_returns_zero(self, tmp_path):
        """No graph_nodes rows for the week → nothing written."""
        db = _fresh_fixture(tmp_path)
        conn = sqlite3.connect(str(db))
        try:
            conn.execute("DELETE FROM graph_nodes WHERE week_start = ?", (WEEK_START,))
            conn.execute("DELETE FROM graph_edges WHERE week_start = ?", (WEEK_START,))
            conn.commit()
        finally:
            conn.close()
        inserted = identify_units(db, db, WEEK_START, now=PINNED_NOW)
        assert inserted == 0

    def test_separate_db_paths(self, tmp_path):
        """identify_units accepts distinct github/sessions paths.

        Sanity check — the same fixture copied twice should produce
        identical units regardless of whether both paths point at the
        same file or two identical files.
        """
        gh = _fresh_fixture(tmp_path)
        sess = tmp_path / "sessions_copy.sqlite"
        shutil.copy(gh, sess)
        inserted = identify_units(gh, sess, WEEK_START, now=PINNED_NOW)
        assert inserted == 2


# ---------------------------------------------------------------------------
# Issue #66 — session-only components dropped; issue/pr anchored components kept
# ---------------------------------------------------------------------------


def _init_minimal_dbs(tmp_path: Path) -> tuple:
    """Return (gh_path, sess_path) for a freshly initialised pair of DBs."""
    from am_i_shipping.db import init_github_db, init_sessions_db

    gh_path = tmp_path / "github.db"
    sess_path = tmp_path / "sessions.db"
    init_github_db(gh_path)
    init_sessions_db(sess_path)
    return gh_path, sess_path


class TestUnitFilterByIssueOrPr:
    """identify_units skips components with no issue/pr node (Issue #66)."""

    def test_session_only_component_dropped(self, tmp_path):
        """A connected component containing only session nodes produces no unit row.

        New behaviour (Issue #66): components lacking an issue or pr anchor
        are filtered out before the INSERT so they never appear in ``units``.
        """
        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        session_node_id = "session:ss-only-uuid"

        conn = sqlite3.connect(str(gh_path))
        try:
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, session_node_id, "session", "ss-only-uuid", "2025-01-07T10:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        # Also insert the session row in sessions.db so metrics lookup works
        sess_conn = sqlite3.connect(str(sess_path))
        try:
            sess_conn.execute(
                "INSERT OR IGNORE INTO sessions "
                "(session_uuid, turn_count, tool_call_count, tool_failure_count, "
                " reprompt_count, bail_out, session_duration_seconds, "
                " working_directory, git_branch, raw_content_json, "
                " input_tokens, output_tokens, cache_creation_tokens, "
                " cache_read_tokens, fast_mode_turns, "
                " session_started_at, session_ended_at) "
                "VALUES (?, 1, 0, 0, 0, 0, 60.0, '/tmp', 'main', '[]', "
                "        0, 0, 0, 0, 0, ?, ?)",
                ("ss-only-uuid", "2025-01-07T10:00:00Z", "2025-01-07T10:01:00Z"),
            )
            sess_conn.commit()
        finally:
            sess_conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 0, (
            f"Session-only component must be dropped; expected 0 inserted, got {inserted}"
        )

        conn = sqlite3.connect(str(gh_path))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert count == 0

    def test_component_with_issue_still_written(self, tmp_path):
        """A component containing at least one issue node produces a unit row.

        The issue node is the anchor; the session node is also present but
        is not sufficient on its own to produce a unit. Together they form
        a valid component.
        """
        from am_i_shipping.db import init_github_db, init_sessions_db

        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        session_uuid = "issue-anchored-uuid"
        session_node_id = f"session:{session_uuid}"
        issue_node_id = "issue:example/repo#10"

        conn = sqlite3.connect(str(gh_path))
        try:
            # Seed graph nodes: one session + one issue
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, session_node_id, "session", session_uuid, "2025-01-06T10:00:00Z"),
            )
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, "issue", "example/repo#10", "2025-01-06T09:00:00Z"),
            )
            # Edge connecting issue to session (Epic #93: issue→session)
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, session_node_id, "issue_has_session", "own"),
            )
            # Seed issues row for metric aggregation
            conn.execute(
                "INSERT INTO issues "
                "(repo, issue_number, title, type_label, state, body, "
                " comments_json, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "example/repo", 10, "Test issue", "feature", "open",
                    "", "[]", "2025-01-06T09:00:00Z", None,
                    "2025-01-06T09:00:00Z",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        # Seed session in sessions.db
        sess_conn = sqlite3.connect(str(sess_path))
        try:
            sess_conn.execute(
                "INSERT OR IGNORE INTO sessions "
                "(session_uuid, turn_count, tool_call_count, tool_failure_count, "
                " reprompt_count, bail_out, session_duration_seconds, "
                " working_directory, git_branch, raw_content_json, "
                " input_tokens, output_tokens, cache_creation_tokens, "
                " cache_read_tokens, fast_mode_turns, "
                " session_started_at, session_ended_at) "
                "VALUES (?, 1, 0, 0, 0, 0, 60.0, '/tmp', 'main', '[]', "
                "        0, 0, 0, 0, 0, ?, ?)",
                (session_uuid, "2025-01-06T10:00:00Z", "2025-01-06T10:01:00Z"),
            )
            sess_conn.commit()
        finally:
            sess_conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 1, (
            f"Component with issue node must produce a unit; expected 1 inserted, got {inserted}"
        )

        conn = sqlite3.connect(str(gh_path))
        try:
            row = conn.execute(
                "SELECT root_node_type FROM units WHERE week_start = ?", (week,)
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        # Issue has higher root priority than session
        assert row[0] == "issue", (
            f"Root node type should be 'issue' (higher priority than session), got {row[0]!r}"
        )

    def test_two_session_only_components_both_dropped(self, tmp_path):
        """Multiple session-only components are all dropped."""
        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        conn = sqlite3.connect(str(gh_path))
        try:
            for i in range(3):
                nid = f"session:sess-{i:04d}"
                conn.execute(
                    "INSERT INTO graph_nodes "
                    "(week_start, node_id, node_type, node_ref, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (week, nid, "session", f"sess-{i:04d}", "2025-01-07T10:00:00Z"),
                )
            conn.commit()
        finally:
            conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 0, (
            f"All session-only components must be dropped; expected 0, got {inserted}"
        )


class TestRootEligibilityWindow:
    """Issue #100 follow-up: past-closed issues/PRs do not anchor units.

    The graph builder may pull a past-closed issue into the week's graph as
    *context* (in-week session commented on it; in-week PR linked to it).
    The unit identifier must not promote such past-closed anchors to unit
    roots, because doing so creates a "shipped this week" unit whose root
    actually closed in a prior week — double-counting velocity.
    """

    def test_past_closed_issue_with_in_week_session_drops_unit(self, tmp_path):
        """Issue closed before week_start + in-week session commenting on it
        → no unit, even though the component has an issue node."""
        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        session_uuid = "ctx-session-uuid"
        session_node_id = f"session:{session_uuid}"
        issue_node_id = "example/repo#10"  # parsed from node_ref

        conn = sqlite3.connect(str(gh_path))
        try:
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, session_node_id, "session", session_uuid,
                 "2025-01-07T10:00:00Z"),
            )
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, f"issue:{issue_node_id}", "issue", issue_node_id,
                 "2024-12-15T09:00:00Z"),
            )
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, f"issue:{issue_node_id}", session_node_id,
                 "issue_has_session", "own"),
            )
            # Issue closed two weeks before week_start — past-shipped
            conn.execute(
                "INSERT INTO issues "
                "(repo, issue_number, title, type_label, state, body, "
                " comments_json, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("example/repo", 10, "Past issue", "feature", "closed",
                 "", "[]", "2024-12-15T09:00:00Z", "2024-12-22T12:00:00Z",
                 "2025-01-07T10:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        sess_conn = sqlite3.connect(str(sess_path))
        try:
            sess_conn.execute(
                "INSERT OR IGNORE INTO sessions "
                "(session_uuid, turn_count, tool_call_count, tool_failure_count, "
                " reprompt_count, bail_out, session_duration_seconds, "
                " working_directory, git_branch, raw_content_json, "
                " input_tokens, output_tokens, cache_creation_tokens, "
                " cache_read_tokens, fast_mode_turns, "
                " session_started_at, session_ended_at) "
                "VALUES (?, 1, 0, 0, 0, 0, 60.0, '/tmp', 'main', '[]', "
                "        0, 0, 0, 0, 0, ?, ?)",
                (session_uuid, "2025-01-07T10:00:00Z", "2025-01-07T10:01:00Z"),
            )
            sess_conn.commit()
        finally:
            sess_conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 0, (
            "Past-closed issue must not anchor a unit attributed to this week"
        )

    def test_in_week_pr_linked_to_past_issue_uses_pr_as_root(self, tmp_path):
        """In-week PR + past-closed issue (linked) → unit created with PR
        as root, not the past-closed issue."""
        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        issue_ref = "example/repo#10"
        pr_ref = "example/repo#42"
        issue_node_id = f"issue:{issue_ref}"
        pr_node_id = f"pr:{pr_ref}"

        conn = sqlite3.connect(str(gh_path))
        try:
            # Past-closed issue (closed 2 weeks before week_start)
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, "issue", issue_ref,
                 "2024-12-15T09:00:00Z"),
            )
            # In-week PR (merged in-week)
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, pr_node_id, "pr", pr_ref, "2025-01-07T08:00:00Z"),
            )
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, pr_node_id, "issue_has_pr", "own"),
            )
            conn.execute(
                "INSERT INTO issues "
                "(repo, issue_number, title, type_label, state, body, "
                " comments_json, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("example/repo", 10, "Past issue", "feature", "closed",
                 "", "[]", "2024-12-15T09:00:00Z", "2024-12-22T12:00:00Z",
                 "2025-01-07T08:30:00Z"),
            )
            conn.execute(
                "INSERT INTO pull_requests "
                "(repo, pr_number, head_ref, title, body, review_comments_json, "
                " review_comment_count, push_count, created_at, merged_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("example/repo", 42, "feat/x", "PR title", "", "[]", 0, 1,
                 "2025-01-07T08:00:00Z", "2025-01-07T11:00:00Z",
                 "2025-01-07T11:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 1, (
            f"In-week PR should still anchor a unit; got {inserted}"
        )

        conn = sqlite3.connect(str(gh_path))
        try:
            row = conn.execute(
                "SELECT root_node_type, root_node_id FROM units "
                "WHERE week_start = ?", (week,)
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == "pr", (
            f"Past-closed issue must not be picked over in-window PR; got {row[0]!r}"
        )
        assert row[1] == pr_node_id

    def test_open_issue_with_null_closed_at_is_eligible(self, tmp_path):
        """An issue that is still open (closed_at IS NULL) must remain a
        valid anchor regardless of when it was created."""
        gh_path, sess_path = _init_minimal_dbs(tmp_path)

        week = "2025-01-06"
        issue_ref = "example/repo#11"
        issue_node_id = f"issue:{issue_ref}"

        conn = sqlite3.connect(str(gh_path))
        try:
            # Issue created last year, still open
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, "issue", issue_ref,
                 "2024-09-01T09:00:00Z"),
            )
            conn.execute(
                "INSERT INTO issues "
                "(repo, issue_number, title, type_label, state, body, "
                " comments_json, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("example/repo", 11, "Long-open issue", "feature", "open",
                 "", "[]", "2024-09-01T09:00:00Z", None,
                 "2025-01-08T10:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        inserted = identify_units(gh_path, sess_path, week, now=PINNED_NOW)
        assert inserted == 1, (
            "Long-open issue (closed_at IS NULL) must still anchor a unit"
        )


# ---------------------------------------------------------------------------
# Issue #68 — AS-2, AS-3, AS-4, AS-5, AS-8
# Two-issue and issue-only fixture tests.
# ---------------------------------------------------------------------------

from datetime import datetime as _datetime

TWO_ISSUE_FIXTURE = Path(__file__).parent / "fixtures" / "two_issue_session.jsonl"
ISSUE_ONLY_FIXTURE = Path(__file__).parent / "fixtures" / "issue_only_session.jsonl"

TWO_ISSUE_SESSION_UUID = "f2000000-0000-0000-0000-000000000002"
ISSUE_ONLY_SESSION_UUID = "f3000000-0000-0000-0000-000000000003"
TWO_ISSUE_REPO = "hyang0129/video_agent_long"
TWO_ISSUE_WEEK = "2026-03-23"
ISSUE_ONLY_WEEK = "2026-03-30"

# Pinned "now" well after both fixtures so no abandonment edge cases.
PINNED_NOW_68 = _datetime(2026, 4, 19, 0, 0, 0)


def _ingest_fixture(tmp_path: Path, fixture_path: Path) -> tuple:
    """Parse and upsert a JSONL fixture; return (sess_db, gh_db).

    Both DBs land in *tmp_path* so that ``upsert_session`` finds
    ``github.db`` next to ``sessions.db`` automatically.
    """
    from am_i_shipping.db import init_github_db, init_sessions_db
    from collector.session_parser import parse_session
    from collector.store import upsert_session
    from synthesis.graph_builder import build_graph

    sess_db = tmp_path / "sessions.db"
    gh_db = tmp_path / "github.db"
    init_sessions_db(sess_db)
    init_github_db(gh_db)

    record = parse_session(fixture_path)
    upsert_session(record, db_path=sess_db, data_dir=tmp_path, skip_health=True)
    return sess_db, gh_db


class TestIssue68TwoIssueFixture:
    """AS-2, AS-3, AS-5 against the canonical two-issue session fixture."""

    def test_identify_units_yields_two_units(self, tmp_path):
        """AS-2: identify_units on the two-issue fixture writes exactly 2 units."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, TWO_ISSUE_FIXTURE)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        inserted = identify_units(gh_db, sess_db, TWO_ISSUE_WEEK, now=PINNED_NOW_68)
        assert inserted == 2, (
            f"Expected exactly 2 units for the two-issue fixture (AS-2), got {inserted}"
        )

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT root_node_id FROM units WHERE week_start = ?",
                (TWO_ISSUE_WEEK,),
            ).fetchall()
        finally:
            conn.close()

        root_ids = {r[0] for r in rows}
        assert f"issue:{TWO_ISSUE_REPO}#305" in root_ids, (
            f"Expected issue:{TWO_ISSUE_REPO}#305 as a unit root, got: {root_ids}"
        )
        assert f"issue:{TWO_ISSUE_REPO}#312" in root_ids, (
            f"Expected issue:{TWO_ISSUE_REPO}#312 as a unit root, got: {root_ids}"
        )

    def test_pr_session_edges_bridge_session_to_prs(self, tmp_path):
        """Epic #93: pr_refs_session / pr_has_session edges (PR → session,
        traversal='own') keep the session in the same component as the PRs."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, TWO_ISSUE_FIXTURE)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            pr_edges = conn.execute(
                "SELECT src_node_id, dst_node_id, edge_type FROM graph_edges "
                "WHERE edge_type IN ('pr_refs_session', 'pr_has_session') "
                "AND week_start = ? AND traversal = 'own'",
                (TWO_ISSUE_WEEK,),
            ).fetchall()
        finally:
            conn.close()

        pr_srcs = {src for src, _, _ in pr_edges}
        sess_dsts = {dst for _, dst, _ in pr_edges}
        assert f"pr:{TWO_ISSUE_REPO}#306" in pr_srcs or (
            f"session:{TWO_ISSUE_SESSION_UUID}" in sess_dsts
        ), (
            "Expected PR↔session edges bridging the session to PR #306. "
            f"PR edges found: {pr_edges}"
        )

    def test_fractional_attribution_half_for_two_issue_session(self, tmp_path):
        """AS-5: session_issue_attribution rows for the two-issue fixture each
        have fraction == 0.5 (one session touching 2 issues → 1/2)."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, TWO_ISSUE_FIXTURE)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT issue_number, fraction FROM session_issue_attribution "
                "WHERE session_uuid = ? AND week_start = ?",
                (TWO_ISSUE_SESSION_UUID, TWO_ISSUE_WEEK),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2, (
            f"Expected 2 session_issue_attribution rows for the two-issue session "
            f"(AS-5), got {len(rows)}: {rows}"
        )
        for issue_number, fraction in rows:
            assert abs(fraction - 0.5) < 1e-9, (
                f"Expected fraction=0.5 for issue #{issue_number} (AS-5), "
                f"got {fraction}"
            )

    def test_phase_planning_for_issue_create_events(self, tmp_path):
        """AS-8 (planning branch): issues created inside the session carry
        phase='planning' in session_issue_attribution."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, TWO_ISSUE_FIXTURE)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            phases = conn.execute(
                "SELECT issue_number, phase FROM session_issue_attribution "
                "WHERE session_uuid = ? AND week_start = ?",
                (TWO_ISSUE_SESSION_UUID, TWO_ISSUE_WEEK),
            ).fetchall()
        finally:
            conn.close()

        assert len(phases) == 2, (
            f"Expected 2 attribution rows; got {len(phases)}"
        )
        for issue_number, phase in phases:
            assert phase == "planning", (
                f"Issue #{issue_number} was created in-session (issue_create event) "
                f"so phase must be 'planning' (AS-8), got {phase!r}"
            )


class TestIssue68IssueOnlyFixture:
    """AS-4, AS-8 (execution branch) against the issue-only session fixture."""

    def test_issue_only_fixture_yields_one_unit(self, tmp_path):
        """AS-4: issue-only fixture (one issue_create, no PRs) yields exactly
        1 unit anchored on the stub issue node."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, ISSUE_ONLY_FIXTURE)
        build_graph(sess_db, gh_db, week_start=ISSUE_ONLY_WEEK)

        inserted = identify_units(gh_db, sess_db, ISSUE_ONLY_WEEK, now=PINNED_NOW_68)
        assert inserted == 1, (
            f"Expected exactly 1 unit for the issue-only fixture (AS-4), got {inserted}"
        )

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT root_node_type, root_node_id FROM units "
                "WHERE week_start = ?",
                (ISSUE_ONLY_WEEK,),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, f"Expected 1 units row, got {rows}"
        root_type, root_id = rows[0]
        assert root_type == "issue", (
            f"Root node type must be 'issue' for issue-only fixture, got {root_type!r}"
        )
        assert root_id == f"issue:{TWO_ISSUE_REPO}#400", (
            f"Root node id must be 'issue:{TWO_ISSUE_REPO}#400', got {root_id!r}"
        )

    def test_issue_only_attribution_fraction_one(self, tmp_path):
        """AS-4/AS-5: issue-only fixture yields fraction=1.0 in attribution table."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, ISSUE_ONLY_FIXTURE)
        build_graph(sess_db, gh_db, week_start=ISSUE_ONLY_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT fraction FROM session_issue_attribution "
                "WHERE session_uuid = ? AND week_start = ?",
                (ISSUE_ONLY_SESSION_UUID, ISSUE_ONLY_WEEK),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected 1 attribution row for issue-only session (AS-4), got {rows}"
        )
        assert abs(rows[0][0] - 1.0) < 1e-9, (
            f"Expected fraction=1.0 for single-issue session, got {rows[0][0]}"
        )

    def test_issue_only_attribution_phase_planning(self, tmp_path):
        """AS-8 (planning branch): issue created in-session → phase='planning'."""
        from synthesis.graph_builder import build_graph

        sess_db, gh_db = _ingest_fixture(tmp_path, ISSUE_ONLY_FIXTURE)
        build_graph(sess_db, gh_db, week_start=ISSUE_ONLY_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT phase FROM session_issue_attribution "
                "WHERE session_uuid = ? AND week_start = ?",
                (ISSUE_ONLY_SESSION_UUID, ISSUE_ONLY_WEEK),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1
        assert rows[0][0] == "planning", (
            f"Issue created in-session must have phase='planning' (AS-8), "
            f"got {rows[0][0]!r}"
        )


class TestIssue68PhaseExecutionBranch:
    """AS-8 (execution branch): a session that only comments on a pre-existing
    issue (no issue_create event) must receive phase='execution'."""

    def test_execution_phase_for_comment_only_session(self, tmp_path):
        """AS-8 execution branch: session with issue_comment (no issue_create)
        → phase='execution' in session_issue_attribution."""
        from am_i_shipping.db import init_github_db, init_sessions_db
        from synthesis.graph_builder import build_graph

        gh_path = tmp_path / "github.db"
        sess_path = tmp_path / "sessions.db"
        init_github_db(gh_path)
        init_sessions_db(sess_path)

        # A session that only comments on a pre-existing issue (no issue_create).
        week = "2026-04-13"
        session_uuid = "e0000000-0000-0000-0000-000000000008"
        repo = "owner/repo"
        issue_number = 77

        # Seed session node
        sess_conn = sqlite3.connect(str(sess_path))
        try:
            sess_conn.execute(
                "INSERT OR IGNORE INTO sessions "
                "(session_uuid, turn_count, tool_call_count, tool_failure_count, "
                " reprompt_count, bail_out, session_duration_seconds, "
                " working_directory, git_branch, raw_content_json, "
                " input_tokens, output_tokens, cache_creation_tokens, "
                " cache_read_tokens, fast_mode_turns, "
                " session_started_at, session_ended_at) "
                "VALUES (?, 2, 1, 0, 0, 0, 120.0, '/tmp', 'main', '[]', "
                "        0, 0, 0, 0, 0, ?, ?)",
                (session_uuid, "2026-04-13T09:00:00Z", "2026-04-13T09:02:00Z"),
            )
            sess_conn.commit()
        finally:
            sess_conn.close()

        # Seed github DB: existing issue + issue_comment event (no issue_create)
        gh_conn = sqlite3.connect(str(gh_path))
        try:
            gh_conn.execute(
                "INSERT OR IGNORE INTO issues "
                "(repo, issue_number, title, type_label, state, body, "
                " comments_json, created_at, closed_at, updated_at) "
                "VALUES (?, ?, 'Pre-existing issue', 'feature', 'open', '', '[]', "
                " '2026-04-10T08:00:00Z', NULL, '2026-04-10T08:00:00Z')",
                (repo, issue_number),
            )
            # issue_comment, NOT issue_create — phase must be 'execution'
            gh_conn.execute(
                "INSERT OR IGNORE INTO session_gh_events "
                "(session_uuid, event_type, repo, ref, url, confidence, created_at) "
                "VALUES (?, ?, ?, ?, '', 'high', '2026-04-13T09:01:00Z')",
                (session_uuid, "issue_comment", repo, str(issue_number)),
            )
            gh_conn.commit()
        finally:
            gh_conn.close()

        build_graph(sess_path, gh_path, week_start=week)

        conn = sqlite3.connect(str(gh_path))
        try:
            rows = conn.execute(
                "SELECT phase FROM session_issue_attribution "
                "WHERE session_uuid = ? AND week_start = ?",
                (session_uuid, week),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected 1 attribution row for comment-only session (AS-8), got {rows}"
        )
        assert rows[0][0] == "execution", (
            f"issue_comment (no issue_create) must yield phase='execution' (AS-8), "
            f"got {rows[0][0]!r}"
        )


# ---------------------------------------------------------------------------
# Issue #98 — 6-status taxonomy: shipped / completed-no-pr / not-planned /
#              closed-unknown / abandoned / open
# ---------------------------------------------------------------------------


def _seed_status_fixture(
    tmp_path: Path,
    *,
    issue_state: str = "closed",
    state_reason: str = "",
    merged_at: str | None = None,
    has_pr_closes_issue_edge: bool = False,
    last_issue_activity: str = "2025-01-10T10:00:00Z",
    now: datetime | None = None,
    issue_number: int = 900,
    pr_number: int = 800,
    week: str = "2025-01-06",
) -> tuple:
    """Return (gh_path, week, now) with a single unit seeded according to params.

    The unit always has one issue node and (when merged_at or
    has_pr_closes_issue_edge is set) one PR node connected via a
    ``pr_closes_issue`` edge.
    """
    from am_i_shipping.db import init_github_db, init_sessions_db

    gh_path = tmp_path / "github.db"
    sess_path = tmp_path / "sessions.db"
    init_github_db(gh_path)
    init_sessions_db(sess_path)

    repo = "test/repo"
    issue_node_id = f"issue:{repo}#{issue_number}"
    pr_node_id = f"pr:{repo}#{pr_number}"

    if now is None:
        now = datetime(2025, 1, 20, 0, 0, 0)  # 10 days after last_issue_activity

    conn = sqlite3.connect(str(gh_path))
    try:
        # Seed issue
        conn.execute(
            "INSERT INTO issues "
            "(repo, issue_number, title, type_label, state, body, "
            " comments_json, created_at, closed_at, updated_at, state_reason) "
            "VALUES (?, ?, ?, ?, ?, '', '[]', ?, ?, ?, ?)",
            (
                repo, issue_number, "Test issue", "feature",
                issue_state,
                "2025-01-01T10:00:00Z",
                "2025-01-09T10:00:00Z" if issue_state == "closed" else None,
                last_issue_activity,
                state_reason,
            ),
        )
        # Seed graph nodes: issue + possibly PR
        conn.execute(
            "INSERT INTO graph_nodes "
            "(week_start, node_id, node_type, node_ref, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (week, issue_node_id, "issue", f"{repo}#{issue_number}",
             "2025-01-01T10:00:00Z"),
        )
        nodes = [issue_node_id]

        if merged_at is not None or has_pr_closes_issue_edge:
            # Seed PR
            conn.execute(
                "INSERT INTO pull_requests "
                "(repo, pr_number, head_ref, title, body, comments_json, "
                " review_comments_json, review_comment_count, push_count, "
                " created_at, merged_at, updated_at) "
                "VALUES (?, ?, 'branch', 'PR title', '', '[]', '[]', 0, 0, "
                " '2025-01-08T10:00:00Z', ?, '2025-01-09T10:00:00Z')",
                (repo, pr_number, merged_at),
            )
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, pr_node_id, "pr", f"{repo}#{pr_number}",
                 "2025-01-08T10:00:00Z"),
            )
            nodes.append(pr_node_id)
            # Add issue_has_pr edge if requested (Epic #93: issue→PR inversion)
            if has_pr_closes_issue_edge:
                conn.execute(
                    "INSERT INTO graph_edges "
                    "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (week, issue_node_id, pr_node_id, "issue_has_pr", "own"),
                )
            # Always connect PR to issue with a generic ownership edge so
            # union-find puts them in the same component.
            conn.execute(
                "INSERT OR IGNORE INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
                "VALUES (?, ?, ?, ?, ?)",
                (week, issue_node_id, pr_node_id, "closes", "own"),
            )

        conn.commit()
    finally:
        conn.close()

    return gh_path, sess_path, week, now


class TestIssue98StatusTaxonomy:
    """Six-status taxonomy produced by _summarise_unit (issue #98)."""

    @pytest.mark.parametrize("state_reason", ["COMPLETED", ""])
    def test_shipped_with_merged_pr_and_pr_closes_issue_edge(
        self, tmp_path, state_reason
    ):
        """shipped: closed + (COMPLETED or empty) + pr_closes_issue edge to merged PR."""
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason=state_reason,
            merged_at="2025-01-09T15:00:00Z",
            has_pr_closes_issue_edge=True,
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "shipped", (
            f"Expected 'shipped' for closed+state_reason={state_reason!r}+merged PR, "
            f"got {status!r}"
        )

    def test_shipped_legacy_empty_reason_component_pr_merged(self, tmp_path):
        """shipped (legacy): closed + empty state_reason + component PR has merged_at.

        When there is no pr_closes_issue edge but the component contains a
        merged PR, _has_merged_linked_pr falls back to checking component PRs.
        """
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason="",
            merged_at="2025-01-09T15:00:00Z",
            has_pr_closes_issue_edge=False,  # no explicit pr_closes_issue edge
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "shipped", (
            f"Expected 'shipped' for legacy closed issue with merged component PR, "
            f"got {status!r}"
        )

    def test_completed_no_pr_with_state_reason_completed(self, tmp_path):
        """completed-no-pr: closed + COMPLETED + no merged linked PR."""
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason="COMPLETED",
            merged_at=None,
            has_pr_closes_issue_edge=False,
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "completed-no-pr", (
            f"Expected 'completed-no-pr' for closed+COMPLETED+no merged PR, "
            f"got {status!r}"
        )

    def test_completed_no_pr_with_unmerged_linked_pr(self, tmp_path):
        """completed-no-pr (b): COMPLETED + pr_closes_issue edge to unmerged PR."""
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason="COMPLETED",
            merged_at=None,  # PR not merged
            has_pr_closes_issue_edge=True,
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "completed-no-pr", (
            f"Expected 'completed-no-pr' for COMPLETED+unmerged PR, got {status!r}"
        )

    @pytest.mark.parametrize("has_merged_pr", [False, True])
    def test_not_planned_regardless_of_pr(self, tmp_path, has_merged_pr):
        """not-planned: NOT_PLANNED takes precedence regardless of PR linkage."""
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason="NOT_PLANNED",
            merged_at="2025-01-09T15:00:00Z" if has_merged_pr else None,
            has_pr_closes_issue_edge=has_merged_pr,
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "not-planned", (
            f"Expected 'not-planned' for NOT_PLANNED (has_merged_pr={has_merged_pr}), "
            f"got {status!r}"
        )

    def test_closed_unknown_legacy_no_pr(self, tmp_path):
        """closed-unknown: closed + empty state_reason + no merged linked PR."""
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="closed",
            state_reason="",
            merged_at=None,
            has_pr_closes_issue_edge=False,
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "closed-unknown", (
            f"Expected 'closed-unknown' for legacy closed+no merged PR, got {status!r}"
        )

    def test_abandoned_open_stale(self, tmp_path):
        """abandoned: open issue with no activity > 14 days."""
        # last_issue_activity: 2025-01-01 (19 days before now=2025-01-20)
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="open",
            state_reason="",
            merged_at=None,
            has_pr_closes_issue_edge=False,
            last_issue_activity="2025-01-01T10:00:00Z",
            now=datetime(2025, 1, 20, 0, 0, 0),
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "abandoned", (
            f"Expected 'abandoned' for open stale unit, got {status!r}"
        )

    def test_open_recent_activity(self, tmp_path):
        """open: open issue with activity within 14 days → status is 'open', not abandoned."""
        # last_issue_activity: 2025-01-15 (5 days before now=2025-01-20)
        gh_path, sess_path, week, now = _seed_status_fixture(
            tmp_path,
            issue_state="open",
            state_reason="",
            merged_at=None,
            has_pr_closes_issue_edge=False,
            last_issue_activity="2025-01-15T10:00:00Z",
            now=datetime(2025, 1, 20, 0, 0, 0),
        )
        inserted = identify_units(gh_path, sess_path, week, now=now)
        assert inserted == 1

        conn = sqlite3.connect(str(gh_path))
        try:
            status = conn.execute(
                "SELECT status FROM units WHERE week_start = ?", (week,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert status == "open", (
            f"Expected 'open' for open unit with recent activity, got {status!r}"
        )
