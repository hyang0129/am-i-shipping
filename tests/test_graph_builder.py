"""Tests for ``synthesis/graph_builder.py`` (Epic #17 — Sub-Issue 3).

Exercises the builder against the committed golden fixture
(``tests/fixtures/synthesis/golden.sqlite``). The fixture packs sessions
and github tables into a single SQLite file; the builder accepts the same
path for both arguments, which is also the runtime-supported shape when a
dev points it at a merged debugging DB.

No network / LLM — graph_builder is offline by design.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from synthesis.graph_builder import (
    _extract_hash_refs,
    _extract_timeline_target,
    build_graph,
    session_matches_pr,
)


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fresh_fixture(tmp_path: Path) -> Path:
    """Copy the golden fixture into ``tmp_path`` and wipe the existing
    ``graph_nodes``/``graph_edges`` rows so the builder starts clean.

    The committed fixture ships with pre-populated graph rows (they exist
    so other sub-issues can test DOWN-stream readers without running the
    builder). These tests care about what ``build_graph`` writes, not what
    the fixture ships with, so we truncate first.
    """
    dst = tmp_path / "golden.sqlite"
    shutil.copy(FIXTURE_SRC, dst)
    conn = sqlite3.connect(str(dst))
    try:
        conn.execute("DELETE FROM graph_nodes")
        conn.execute("DELETE FROM graph_edges")
        conn.commit()
    finally:
        conn.close()
    return dst


def _nodes(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT week_start, node_id, node_type, node_ref, created_at "
        "FROM graph_nodes ORDER BY node_type, node_id"
    ).fetchall()


def _edges(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT week_start, src_node_id, dst_node_id, edge_type "
        "FROM graph_edges ORDER BY src_node_id, dst_node_id, edge_type"
    ).fetchall()


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestSessionMatchesPr:
    def test_exact_branch_and_workdir(self):
        assert session_matches_pr(
            "fix/1-bug", "/workspaces/repo", "fix/1-bug", "owner/repo"
        )

    def test_branch_mismatch(self):
        assert not session_matches_pr(
            "main", "/workspaces/repo", "fix/1-bug", "owner/repo"
        )

    def test_workdir_missing_repo_name(self):
        assert not session_matches_pr(
            "fix/1-bug", "/tmp/elsewhere", "fix/1-bug", "owner/repo"
        )

    def test_empty_branch_is_miss(self):
        assert not session_matches_pr(
            "", "/workspaces/repo", "fix/1-bug", "owner/repo"
        )

    def test_empty_pr_head_is_miss(self):
        assert not session_matches_pr(
            "fix/1-bug", "/workspaces/repo", "", "owner/repo"
        )

    def test_empty_repo_is_miss(self):
        assert not session_matches_pr(
            "fix/1-bug", "/workspaces/repo", "fix/1-bug", ""
        )


class TestExtractHashRefs:
    def test_single(self):
        assert _extract_hash_refs("refs #42 only") == [42]

    def test_multiple_deduped(self):
        assert _extract_hash_refs("see #1 and #2 and #1 again") == [1, 2]

    def test_ignores_embedded(self):
        # '#'` that isn't word-boundary leading should still match because
        # the regex allows start-of-string; ensure URL-like substrings
        # (e.g. inside shas) don't false-positive.
        assert _extract_hash_refs("abc123#4") == []

    def test_empty(self):
        assert _extract_hash_refs("") == []


class TestExtractTimelineTarget:
    def test_nested_issue(self):
        payload = '{"source": {"issue": {"number": 7}}}'
        assert _extract_timeline_target(payload) == (7, "issue")

    def test_nested_pull_request(self):
        payload = '{"source": {"issue": {"number": 7, "pull_request": {"url": "x"}}}}'
        assert _extract_timeline_target(payload) == (7, "pull_request")

    def test_flat(self):
        assert _extract_timeline_target('{"number": 9, "type": "issue"}') == (
            9,
            "issue",
        )

    def test_malformed(self):
        assert _extract_timeline_target("not json") == (None, None)

    def test_empty(self):
        assert _extract_timeline_target(None) == (None, None)


# ---------------------------------------------------------------------------
# build_graph against the golden fixture
# ---------------------------------------------------------------------------


class TestBuildGraphFixture:
    def test_nodes_cover_expected_types(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        build_graph(db, db)

        conn = sqlite3.connect(str(db))
        try:
            counts = dict(
                conn.execute(
                    "SELECT node_type, COUNT(*) FROM graph_nodes GROUP BY node_type"
                ).fetchall()
            )
        finally:
            conn.close()

        # Fixture topology: 2 issues, 3 PRs, 3 commits, 3 sessions.
        assert counts.get("issue") == 2
        assert counts.get("pr") == 3
        assert counts.get("commit") == 3
        assert counts.get("session") == 3

    def test_expected_edges_present(self, tmp_path):
        db = _fresh_fixture(tmp_path)
        build_graph(db, db)

        conn = sqlite3.connect(str(db))
        try:
            edges = set(
                (src, dst, etype)
                for _week, src, dst, etype in _edges(conn)
            )
        finally:
            conn.close()

        # Unit 1 closes — from pr_issues
        assert (
            "pr:example/repo#301",
            "issue:example/repo#201",
            "pr_closes_issue",
        ) in edges
        assert (
            "pr:example/repo#302",
            "issue:example/repo#201",
            "pr_closes_issue",
        ) in edges
        # Unit 2 closes — from pr_issues
        assert (
            "pr:example/repo#303",
            "issue:example/repo#202",
            "pr_closes_issue",
        ) in edges

        # pr_has_commit — each commit in the fixture is linked to its PR
        assert (
            "pr:example/repo#301",
            "commit:" + ("a" * 40),
            "pr_has_commit",
        ) in edges
        assert (
            "pr:example/repo#302",
            "commit:" + ("b" * 40),
            "pr_has_commit",
        ) in edges
        assert (
            "pr:example/repo#303",
            "commit:" + ("c" * 40),
            "pr_has_commit",
        ) in edges

        # session_on_pr — from pr_sessions (collector-written rows)
        assert (
            "session:00000000-0000-0000-0000-000000000101",
            "pr:example/repo#301",
            "session_on_pr",
        ) in edges
        assert (
            "session:00000000-0000-0000-0000-000000000102",
            "pr:example/repo#302",
            "session_on_pr",
        ) in edges

    def test_singleton_session_has_node_no_edges(self, tmp_path):
        """Unit 3's session has no PR link — it should exist as a node
        but have zero edges touching it."""
        db = _fresh_fixture(tmp_path)
        build_graph(db, db)

        singleton = "session:00000000-0000-0000-0000-000000000303"
        conn = sqlite3.connect(str(db))
        try:
            node = conn.execute(
                "SELECT node_id FROM graph_nodes WHERE node_id = ?",
                (singleton,),
            ).fetchone()
            edge_count = conn.execute(
                "SELECT COUNT(*) FROM graph_edges WHERE src_node_id = ? OR dst_node_id = ?",
                (singleton, singleton),
            ).fetchone()[0]
        finally:
            conn.close()

        assert node is not None
        assert edge_count == 0

    def test_determinism_two_runs_identical(self, tmp_path):
        """Two consecutive builds against the same DB produce byte-identical
        graph rows — INSERT OR IGNORE plus deterministic sort keys keep the
        second pass from flipping row order or duplicating anything."""
        db = _fresh_fixture(tmp_path)
        build_graph(db, db)

        conn = sqlite3.connect(str(db))
        try:
            nodes_first = _nodes(conn)
            edges_first = _edges(conn)
        finally:
            conn.close()

        build_graph(db, db)  # run again — must be a no-op

        conn = sqlite3.connect(str(db))
        try:
            nodes_second = _nodes(conn)
            edges_second = _edges(conn)
        finally:
            conn.close()

        assert nodes_first == nodes_second
        assert edges_first == edges_second

    def test_commit_hash_ref_extraction(self, tmp_path):
        """A commit message containing ``#N`` produces a ``commit_refs_issue``
        edge to the referenced issue — even when that issue is separate from
        the PR's own close-target."""
        db = _fresh_fixture(tmp_path)

        # Mutate one commit message in-place to reference issue 202 so we
        # can observe the scan producing a cross-unit edge. The commit
        # belongs to PR 301 (which closes issue 201); the ``#202`` in the
        # message should surface as a second edge from that commit.
        conn = sqlite3.connect(str(db))
        try:
            conn.execute(
                "UPDATE commits SET message = ? WHERE sha = ?",
                ("Unit 1 PR A commit — also touches #202", "a" * 40),
            )
            conn.commit()
        finally:
            conn.close()

        build_graph(db, db)

        conn = sqlite3.connect(str(db))
        try:
            edges = set(
                (src, dst, etype)
                for _week, src, dst, etype in _edges(conn)
            )
        finally:
            conn.close()

        assert (
            "commit:" + ("a" * 40),
            "issue:example/repo#202",
            "commit_refs_issue",
        ) in edges

    def test_week_start_filters_sessions(self, tmp_path):
        """When ``week_start`` is passed, only sessions whose
        ``session_started_at`` falls in that 7-day window become nodes."""
        db = _fresh_fixture(tmp_path)
        # Fixture sessions span 2025-01-06, 2025-01-07, 2025-01-08 — all
        # inside the week starting Monday 2025-01-06, so all three should
        # land as nodes for that week.
        build_graph(db, db, week_start="2025-01-06")

        conn = sqlite3.connect(str(db))
        try:
            sessions_in_week = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes "
                "WHERE week_start = ? AND node_type = 'session'",
                ("2025-01-06",),
            ).fetchone()[0]
            sessions_next_week = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes "
                "WHERE week_start = ? AND node_type = 'session'",
                ("2025-01-13",),
            ).fetchone()[0]
        finally:
            conn.close()

        assert sessions_in_week == 3

        # Run again for a week the sessions don't fall into.
        build_graph(db, db, week_start="2025-01-13")
        conn = sqlite3.connect(str(db))
        try:
            sessions_next_week = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes "
                "WHERE week_start = ? AND node_type = 'session'",
                ("2025-01-13",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert sessions_next_week == 0

    def test_separate_sessions_and_github_dbs(self, tmp_path):
        """Realistic shape: sessions.db and github.db are distinct files.

        Copies the fixture into two files and strips the non-overlapping
        schemas from each. The builder must still produce the same graph
        structure it does when both paths point to one file.
        """
        from am_i_shipping.db import init_github_db, init_sessions_db

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"

        init_github_db(gh_db)
        init_sessions_db(sess_db)

        # Copy github-side rows from the fixture into gh_db.
        src = sqlite3.connect(str(FIXTURE_SRC))
        dst_gh = sqlite3.connect(str(gh_db))
        try:
            for table in (
                "issues",
                "pull_requests",
                "pr_issues",
                "pr_sessions",
                "commits",
                "timeline_events",
            ):
                rows = src.execute(f"SELECT * FROM {table}").fetchall()
                if not rows:
                    continue
                placeholders = ",".join("?" * len(rows[0]))
                dst_gh.executemany(
                    f"INSERT INTO {table} VALUES ({placeholders})", rows
                )
            dst_gh.commit()

            # Copy sessions-side rows into sess_db.
            session_rows = src.execute("SELECT * FROM sessions").fetchall()
        finally:
            src.close()
            dst_gh.close()

        dst_sess = sqlite3.connect(str(sess_db))
        try:
            if session_rows:
                placeholders = ",".join("?" * len(session_rows[0]))
                dst_sess.executemany(
                    f"INSERT INTO sessions VALUES ({placeholders})",
                    session_rows,
                )
                dst_sess.commit()
        finally:
            dst_sess.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            counts = dict(
                conn.execute(
                    "SELECT node_type, COUNT(*) FROM graph_nodes GROUP BY node_type"
                ).fetchall()
            )
        finally:
            conn.close()

        assert counts.get("issue") == 2
        assert counts.get("pr") == 3
        assert counts.get("commit") == 3
        assert counts.get("session") == 3
