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


# ---------------------------------------------------------------------------
# Issue #66 — session_refs_issue / session_refs_pr edges from session_gh_events
# ---------------------------------------------------------------------------


def _make_gh_db(tmp_path: Path) -> Path:
    """Create a fresh github.db (all tables, no rows) in tmp_path."""
    from am_i_shipping.db import init_github_db

    path = tmp_path / "github.db"
    init_github_db(path)
    return path


def _make_sess_db(tmp_path: Path) -> Path:
    """Create a fresh sessions.db (all tables, no rows) in tmp_path."""
    from am_i_shipping.db import init_sessions_db

    path = tmp_path / "sessions.db"
    init_sessions_db(path)
    return path


def _seed_session(conn: sqlite3.Connection, uuid: str, started_at: str) -> None:
    """Insert a minimal sessions row."""
    conn.execute(
        "INSERT OR IGNORE INTO sessions "
        "(session_uuid, turn_count, tool_call_count, tool_failure_count, "
        " reprompt_count, bail_out, session_duration_seconds, "
        " working_directory, git_branch, raw_content_json, "
        " input_tokens, output_tokens, cache_creation_tokens, "
        " cache_read_tokens, fast_mode_turns, "
        " session_started_at, session_ended_at) "
        "VALUES (?, 1, 0, 0, 0, 0, 60.0, '/tmp', 'main', '[]', "
        " 0, 0, 0, 0, 0, ?, ?)",
        (uuid, started_at, started_at),
    )


def _seed_issue(conn: sqlite3.Connection, repo: str, number: int) -> None:
    """Insert a minimal issues row."""
    conn.execute(
        "INSERT OR IGNORE INTO issues "
        "(repo, issue_number, title, type_label, state, body, comments_json, "
        " created_at, closed_at, updated_at) "
        "VALUES (?, ?, 'Test issue', 'feature', 'open', '', '[]', "
        " '2025-01-10T09:00:00Z', NULL, '2025-01-10T09:00:00Z')",
        (repo, number),
    )


def _seed_pr(conn: sqlite3.Connection, repo: str, number: int) -> None:
    """Insert a minimal pull_requests row."""
    conn.execute(
        "INSERT OR IGNORE INTO pull_requests "
        "(repo, pr_number, head_ref, title, body, comments_json, "
        " review_comments_json, review_comment_count, push_count, "
        " created_at, merged_at, updated_at) "
        "VALUES (?, ?, 'fix/test', 'Test PR', '', '[]', '[]', 0, 0, "
        " '2025-01-10T09:00:00Z', NULL, '2025-01-10T09:00:00Z')",
        (repo, number),
    )


def _seed_session_gh_event(
    conn: sqlite3.Connection,
    session_uuid: str,
    event_type: str,
    repo: str,
    ref: str,
) -> None:
    """Insert a session_gh_events row."""
    conn.execute(
        "INSERT OR IGNORE INTO session_gh_events "
        "(session_uuid, event_type, repo, ref, url, confidence, created_at) "
        "VALUES (?, ?, ?, ?, '', 'high', '2025-01-10T10:00:00Z')",
        (session_uuid, event_type, repo, ref),
    )


SESSION_UUID = "s1s1s1s1-0000-0000-0000-000000000001"
REPO = "owner/repo"
WEEK = "2025-01-06"


class TestSessionRefsIssueEdge:
    """build_graph emits session_refs_issue edges from session_gh_events."""

    def test_session_refs_issue_edge_emitted(self, tmp_path):
        """An issue_comment event pointing to an existing issue creates an edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_issue(gh_conn, REPO, 42)
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "issue_comment", REPO, "42"
            )
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            edges = {
                (src, dst, etype)
                for _week, src, dst, etype in conn.execute(
                    "SELECT week_start, src_node_id, dst_node_id, edge_type "
                    "FROM graph_edges"
                ).fetchall()
            }
        finally:
            conn.close()

        expected_src = f"session:{SESSION_UUID}"
        expected_dst = f"issue:{REPO}#42"
        assert (expected_src, expected_dst, "session_refs_issue") in edges, (
            f"Expected session_refs_issue edge not found. Edges present: {edges}"
        )

    def test_session_refs_issue_skipped_when_issue_missing(self, tmp_path):
        """No edge is emitted when the target issue node does not exist."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            # No issues row — only the session_gh_events event
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "issue_comment", REPO, "42"
            )
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            edges = [
                (src, dst, etype)
                for _week, src, dst, etype in conn.execute(
                    "SELECT week_start, src_node_id, dst_node_id, edge_type "
                    "FROM graph_edges"
                ).fetchall()
            ]
            nodes = [
                nid
                for _week, nid, *_ in conn.execute(
                    "SELECT week_start, node_id, node_type, node_ref, created_at "
                    "FROM graph_nodes"
                ).fetchall()
            ]
        finally:
            conn.close()

        # No session_refs_issue edge should exist
        refs_issue_edges = [
            e for e in edges if e[2] == "session_refs_issue"
        ]
        assert refs_issue_edges == [], (
            f"Expected no session_refs_issue edge, got: {refs_issue_edges}"
        )

        # No stub node for the missing issue
        issue_node = f"issue:{REPO}#42"
        assert issue_node not in nodes, (
            f"Stub node {issue_node!r} must not be created when issue is missing"
        )

    def test_git_push_does_not_emit_edge(self, tmp_path):
        """git_push events in session_gh_events do not produce any session_refs edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            # Add an issue so the session *could* link (but shouldn't via git_push)
            _seed_issue(gh_conn, REPO, 42)
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "git_push", REPO, "feature-x"
            )
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            edges = [
                (src, dst, etype)
                for _week, src, dst, etype in conn.execute(
                    "SELECT week_start, src_node_id, dst_node_id, edge_type "
                    "FROM graph_edges"
                ).fetchall()
            ]
        finally:
            conn.close()

        # No session_refs_* edge should come from the git_push event
        session_refs_edges = [
            e for e in edges
            if e[0].startswith(f"session:{SESSION_UUID}")
            and e[2].startswith("session_refs")
        ]
        assert session_refs_edges == [], (
            f"git_push must not emit session_refs edge, got: {session_refs_edges}"
        )

    def test_session_refs_pr_edge_emitted(self, tmp_path):
        """A pr_comment event pointing to an existing PR creates a session_refs_pr edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_pr(gh_conn, REPO, 99)
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "pr_comment", REPO, "99"
            )
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            edges = {
                (src, dst, etype)
                for _week, src, dst, etype in conn.execute(
                    "SELECT week_start, src_node_id, dst_node_id, edge_type "
                    "FROM graph_edges"
                ).fetchall()
            }
        finally:
            conn.close()

        expected_src = f"session:{SESSION_UUID}"
        expected_dst = f"pr:{REPO}#99"
        assert (expected_src, expected_dst, "session_refs_pr") in edges, (
            f"Expected session_refs_pr edge not found. Edges present: {edges}"
        )

    def test_pending_ref_skipped(self, tmp_path):
        """A session_gh_events row with ref='pending' is not turned into an edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_issue(gh_conn, REPO, 1)
            # Seed with ref="pending" — should be ignored by graph builder
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "issue_create", REPO, "pending"
            )
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            refs_issue_edges = [
                (src, dst, etype)
                for _week, src, dst, etype in conn.execute(
                    "SELECT week_start, src_node_id, dst_node_id, edge_type "
                    "FROM graph_edges"
                ).fetchall()
                if etype == "session_refs_issue"
            ]
        finally:
            conn.close()

        assert refs_issue_edges == [], (
            f"ref='pending' must not produce an edge, got: {refs_issue_edges}"
        )
