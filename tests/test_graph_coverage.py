"""Tests for the graph coverage diagnostic (Epic #93 — Issue #105).

One test class per acceptance scenario in the child-4 spec
(``docs/child-4-coverage-diagnostic.md``). Fixtures use direct
``graph_nodes`` / ``graph_edges`` inserts so they don't depend on the
graph_builder pipeline — the diagnostic must read whatever graph rows
exist, not whatever the builder happens to produce today.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.db import init_github_db
from synthesis.coverage import (
    GraphCoverageReport,
    GraphWeekRow,
    collect_graph_coverage,
    format_graph_text,
    run_coverage,
    run_graph_coverage,
)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _insert_node(conn: sqlite3.Connection, week: str, nid: str, ntype: str) -> None:
    conn.execute(
        "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref) "
        "VALUES (?, ?, ?, ?)",
        (week, nid, ntype, nid),
    )


def _insert_edge(
    conn: sqlite3.Connection,
    week: str,
    src: str,
    dst: str,
    edge_type: str,
    traversal: str = "own",
) -> None:
    conn.execute(
        "INSERT INTO graph_edges "
        "(week_start, src_node_id, dst_node_id, edge_type, traversal) "
        "VALUES (?, ?, ?, ?, ?)",
        (week, src, dst, edge_type, traversal),
    )


@pytest.fixture
def github_db(tmp_path) -> Path:
    db = tmp_path / "github.db"
    init_github_db(db)
    return db


# ---------------------------------------------------------------------------
# Scenario 1 — Per-week session-reachability ratio
# ---------------------------------------------------------------------------


class TestSessionReachabilityRatio:
    def test_eight_of_ten_reachable(self, github_db):
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            # 1 issue node, 10 session nodes; 8 sessions linked via
            # issue_has_session edges.
            _insert_node(conn, wk, "issue:owner/repo#1", "issue")
            for i in range(10):
                _insert_node(conn, wk, f"sess:{i}", "session")
            for i in range(8):
                _insert_edge(
                    conn, wk, "issue:owner/repo#1", f"sess:{i}",
                    "issue_has_session", "own",
                )
            conn.commit()
        finally:
            conn.close()

        report = collect_graph_coverage(github_db, week_start=wk)
        assert len(report.weeks) == 1
        row = report.weeks[0]
        assert row.total_session_nodes == 10
        assert row.reachable_session_nodes == 8
        assert row.session_reachability_ratio == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# Scenario 2 — Per-week issue-with-session ratio
# ---------------------------------------------------------------------------


class TestIssueLinkageRatio:
    def test_nine_of_ten_issues_linked(self, github_db):
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            for i in range(10):
                _insert_node(conn, wk, f"issue:i{i}", "issue")
            _insert_node(conn, wk, "sess:s0", "session")
            # 9 of 10 issues have an outgoing issue_has_session edge.
            for i in range(9):
                _insert_edge(
                    conn, wk, f"issue:i{i}", "sess:s0",
                    "issue_has_session", "own",
                )
            conn.commit()
        finally:
            conn.close()

        report = collect_graph_coverage(github_db, week_start=wk)
        row = report.weeks[0]
        assert row.total_issue_nodes == 10
        assert row.issues_with_linked_session == 9
        assert row.issue_linkage_ratio == pytest.approx(0.9)

    def test_issue_refs_session_also_counts(self, github_db):
        """Both edge types — issue_has_session AND issue_refs_session —
        count toward issue linkage. The spec lists both explicitly."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:a", "issue")
            _insert_node(conn, wk, "issue:b", "issue")
            _insert_node(conn, wk, "sess:s0", "session")
            _insert_edge(
                conn, wk, "issue:a", "sess:s0",
                "issue_has_session", "own",
            )
            _insert_edge(
                conn, wk, "issue:b", "sess:s0",
                "issue_refs_session", "own",
            )
            conn.commit()
        finally:
            conn.close()

        row = collect_graph_coverage(github_db, week_start=wk).weeks[0]
        assert row.issues_with_linked_session == 2
        assert row.issue_linkage_ratio == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Scenario 3 — All weeks reported in one invocation
# ---------------------------------------------------------------------------


class TestMultiWeek:
    def test_all_weeks_reported(self, github_db):
        weeks = ["2026-03-31", "2026-04-07", "2026-04-14", "2026-04-21"]
        conn = sqlite3.connect(str(github_db))
        try:
            for wk in weeks:
                _insert_node(conn, wk, f"issue:{wk}", "issue")
                _insert_node(conn, wk, f"sess:{wk}", "session")
                _insert_edge(
                    conn, wk, f"issue:{wk}", f"sess:{wk}",
                    "issue_has_session", "own",
                )
            conn.commit()
        finally:
            conn.close()

        report = collect_graph_coverage(github_db)  # no week filter
        reported = [r.week_start for r in report.weeks]
        assert reported == weeks  # ascending order
        for r in report.weeks:
            assert r.session_reachability_ratio == pytest.approx(1.0)
            assert r.issue_linkage_ratio == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Scenario 4 — Filter consistency with BFS walkers (traversal='own' only)
# ---------------------------------------------------------------------------


class TestTraversalFilter:
    def test_ref_edge_does_not_count_as_reachable(self, github_db):
        """A session reachable only via a 'ref' edge must NOT be counted
        as reachable — the diagnostic must agree with the BFS walkers."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:i1", "issue")
            _insert_node(conn, wk, "commit:c1", "commit")
            _insert_node(conn, wk, "sess:s_own", "session")
            _insert_node(conn, wk, "sess:s_ref", "session")
            # Reachable: issue -> sess_own via 'own'.
            _insert_edge(
                conn, wk, "issue:i1", "sess:s_own",
                "issue_has_session", "own",
            )
            # NOT reachable for our purposes: commit -> issue via 'ref'
            # edge; even if we seeded from commit it shouldn't matter
            # because we seed from issue/PR only and 'ref' is filtered out.
            _insert_edge(
                conn, wk, "commit:c1", "issue:i1",
                "commit_refs_issue", "ref",
            )
            # And: a session connected via a ref edge from an issue.
            _insert_edge(
                conn, wk, "issue:i1", "sess:s_ref",
                "issue_refs_session", "ref",
            )
            conn.commit()
        finally:
            conn.close()

        row = collect_graph_coverage(github_db, week_start=wk).weeks[0]
        assert row.total_session_nodes == 2
        assert row.reachable_session_nodes == 1  # only s_own
        assert row.session_reachability_ratio == pytest.approx(0.5)

    def test_ref_edge_does_not_count_for_issue_linkage(self, github_db):
        """An issue whose only outgoing edge is traversal='ref' must NOT
        be counted as having a linked session."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:i1", "issue")
            _insert_node(conn, wk, "sess:s0", "session")
            _insert_edge(
                conn, wk, "issue:i1", "sess:s0",
                "issue_refs_session", "ref",
            )
            conn.commit()
        finally:
            conn.close()

        row = collect_graph_coverage(github_db, week_start=wk).weeks[0]
        assert row.total_issue_nodes == 1
        assert row.issues_with_linked_session == 0


# ---------------------------------------------------------------------------
# Scenario 5 — CLI integration
# ---------------------------------------------------------------------------


class TestCliIntegration:
    def test_text_output_contains_ratios_and_labels(self, github_db, capsys):
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:i1", "issue")
            _insert_node(conn, wk, "sess:s1", "session")
            _insert_edge(
                conn, wk, "issue:i1", "sess:s1",
                "issue_has_session", "own",
            )
            conn.commit()
        finally:
            conn.close()

        rc = run_graph_coverage(github_db=github_db, week=wk)
        assert rc == 0
        out = capsys.readouterr().out
        # 2-decimal ratio and clear labels.
        assert "session_reachability=1.00" in out
        assert "issue_linkage=1.00" in out
        assert wk in out

    def test_json_mode_structure(self, github_db, capsys):
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:i1", "issue")
            _insert_node(conn, wk, "sess:s1", "session")
            _insert_edge(
                conn, wk, "issue:i1", "sess:s1",
                "issue_has_session", "own",
            )
            conn.commit()
        finally:
            conn.close()

        rc = run_graph_coverage(github_db=github_db, week=wk, emit_json=True)
        assert rc == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert "weeks" in parsed
        assert len(parsed["weeks"]) == 1
        row = parsed["weeks"][0]
        assert row["week_start"] == wk
        assert row["total_session_nodes"] == 1
        assert row["reachable_session_nodes"] == 1
        assert row["session_reachability_ratio"] == pytest.approx(1.0)
        assert row["total_issue_nodes"] == 1
        assert row["issues_with_linked_session"] == 1
        assert row["issue_linkage_ratio"] == pytest.approx(1.0)

    def test_run_coverage_dispatches_graph_mode(self, github_db, tmp_path, capsys):
        """run_coverage(graph_mode=True, ...) routes to run_graph_coverage."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "issue:i1", "issue")
            conn.commit()
        finally:
            conn.close()

        rc = run_coverage(
            sessions_db=tmp_path / "sessions.db",
            projects_path=tmp_path / "projects",
            week_start="monday",
            data_dir=tmp_path,
            graph_mode=True,
            github_db=github_db,
            week=wk,
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Graph coverage diagnostic" in out

    def test_graph_with_backfill_is_rejected(self, github_db, tmp_path, capsys):
        rc = run_coverage(
            sessions_db=tmp_path / "sessions.db",
            projects_path=tmp_path / "projects",
            week_start="monday",
            data_dir=tmp_path,
            graph_mode=True,
            do_backfill=True,
            github_db=github_db,
        )
        assert rc == 2
        err = capsys.readouterr().err
        assert "--graph" in err


# ---------------------------------------------------------------------------
# Scenario 6 — Behaves correctly on empty input
# ---------------------------------------------------------------------------


class TestEmptyInput:
    def test_empty_db_no_exception(self, github_db):
        # Fresh init_github_db: tables exist, no rows.
        report = collect_graph_coverage(github_db)
        assert report.weeks == []

    def test_missing_db_no_exception(self, tmp_path):
        report = collect_graph_coverage(tmp_path / "does-not-exist.db")
        assert report.weeks == []

    def test_zero_denominator_ratio_is_none(self, github_db):
        """A week with no session and no issue nodes must report ratios as
        ``None`` (not 0.0). The output must distinguish 'no data' from
        '0% reachability'."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            # Insert a single PR node so the week appears, but no sessions
            # and no issues.
            _insert_node(conn, wk, "pr:p1", "pr")
            conn.commit()
        finally:
            conn.close()

        row = collect_graph_coverage(github_db, week_start=wk).weeks[0]
        assert row.total_session_nodes == 0
        assert row.session_reachability_ratio is None
        assert row.total_issue_nodes == 0
        assert row.issue_linkage_ratio is None

    def test_format_text_no_data(self):
        text = format_graph_text(GraphCoverageReport(weeks=[]))
        assert "(no data)" in text

    def test_format_text_renders_na_for_none(self):
        report = GraphCoverageReport(weeks=[
            GraphWeekRow(week_start="2026-04-14"),
        ])
        text = format_graph_text(report)
        assert "n/a" in text


# ---------------------------------------------------------------------------
# Reachability through PR nodes (multi-hop)
# ---------------------------------------------------------------------------


class TestMultiHopReachability:
    def test_pr_to_session_reachable(self, github_db):
        """PR nodes are also valid roots — pr_has_session edges (own)
        contribute to the reachable set."""
        wk = "2026-04-14"
        conn = sqlite3.connect(str(github_db))
        try:
            _insert_node(conn, wk, "pr:p1", "pr")
            _insert_node(conn, wk, "sess:s1", "session")
            _insert_edge(
                conn, wk, "pr:p1", "sess:s1",
                "pr_has_session", "own",
            )
            conn.commit()
        finally:
            conn.close()

        row = collect_graph_coverage(github_db, week_start=wk).weeks[0]
        assert row.reachable_session_nodes == 1
        assert row.session_reachability_ratio == pytest.approx(1.0)
