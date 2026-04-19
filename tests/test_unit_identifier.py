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
            # Edge connecting session to issue
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type) "
                "VALUES (?, ?, ?, ?)",
                (week, session_node_id, issue_node_id, "session_refs_issue"),
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
