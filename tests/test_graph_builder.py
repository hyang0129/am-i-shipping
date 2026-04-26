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

        # Unit 1 closes — from pr_issues (Epic #93: issue→PR, inverted)
        assert (
            "issue:example/repo#201",
            "pr:example/repo#301",
            "issue_has_pr",
        ) in edges
        assert (
            "issue:example/repo#201",
            "pr:example/repo#302",
            "issue_has_pr",
        ) in edges
        # Unit 2 closes — from pr_issues
        assert (
            "issue:example/repo#202",
            "pr:example/repo#303",
            "issue_has_pr",
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

        # pr_has_session — from pr_sessions (Epic #93: PR→session, inverted)
        assert (
            "pr:example/repo#301",
            "session:00000000-0000-0000-0000-000000000101",
            "pr_has_session",
        ) in edges
        assert (
            "pr:example/repo#302",
            "session:00000000-0000-0000-0000-000000000102",
            "pr_has_session",
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

    def test_week_start_excludes_stale_issue_and_pr_nodes(self, tmp_path):
        """When ``week_start`` is provided, issue and PR nodes with no in-week
        activity are excluded from the graph (Issue #100 regression guard).

        The golden fixture contains:
          - issue 201: closed in-week (2025-01-08) → included
          - issue 202: open, updated 2024-12-11 (stale) → excluded
          - PR 301: created+merged 2025-01-06 (in-week) → included
          - PR 302: created+merged 2025-01-08 (in-week) → included
          - PR 303: created 2024-12-11, merged=NULL (stale, not in-week) → excluded
        """
        db = _fresh_fixture(tmp_path)
        build_graph(db, db, week_start="2025-01-06")

        conn = sqlite3.connect(str(db))
        try:
            counts = dict(
                conn.execute(
                    "SELECT node_type, COUNT(*) FROM graph_nodes "
                    "WHERE week_start = ? GROUP BY node_type",
                    ("2025-01-06",),
                ).fetchall()
            )
            node_ids = {
                row[0]
                for row in conn.execute(
                    "SELECT node_id FROM graph_nodes WHERE week_start = ?",
                    ("2025-01-06",),
                ).fetchall()
            }
        finally:
            conn.close()

        # Only the in-week issue should appear
        assert counts.get("issue") == 1, (
            f"Expected 1 issue node (only in-week), got {counts.get('issue')}; "
            f"nodes: {node_ids}"
        )
        assert "issue:example/repo#201" in node_ids, "In-week closed issue must be included"
        assert "issue:example/repo#202" not in node_ids, "Stale open issue must be excluded"

        # Only the two in-week PRs should appear
        assert counts.get("pr") == 2, (
            f"Expected 2 PR nodes (only in-week), got {counts.get('pr')}; "
            f"nodes: {node_ids}"
        )
        assert "pr:example/repo#301" in node_ids, "In-week PR 301 must be included"
        assert "pr:example/repo#302" in node_ids, "In-week PR 302 must be included"
        assert "pr:example/repo#303" not in node_ids, "Stale PR 303 must be excluded"

    def test_pr_to_issue_pull_in(self, tmp_path):
        """An issue with no direct in-week timestamps is pulled in when it is
        linked to an in-week PR via ``pr_issues`` (Issue #100 spec criterion 4).

        We create a stale closed issue (closed before the week) but link it
        to an in-week PR. The issue must appear as a graph node.
        """
        from am_i_shipping.db import init_github_db, init_sessions_db

        gh_db = tmp_path / "github.db"
        sess_db = tmp_path / "sessions.db"
        init_github_db(gh_db)
        init_sessions_db(sess_db)

        week = "2025-01-06"

        gh_conn = sqlite3.connect(str(gh_db))
        try:
            # Stale issue: closed before the week, not updated in-week
            gh_conn.execute(
                "INSERT INTO issues "
                "(repo, issue_number, title, type_label, state, body, comments_json, "
                " created_at, closed_at, updated_at) "
                "VALUES (?, ?, 'Old issue', 'bug', 'closed', '', '[]', "
                " '2024-12-01T10:00:00Z', '2024-12-05T10:00:00Z', '2024-12-05T10:00:00Z')",
                (REPO, 77),
            )
            # In-week PR
            gh_conn.execute(
                "INSERT INTO pull_requests "
                "(repo, pr_number, head_ref, title, body, comments_json, "
                " review_comments_json, review_comment_count, push_count, "
                " created_at, merged_at, updated_at) "
                "VALUES (?, ?, 'fix/old-issue', 'Fix old issue', '', '[]', '[]', 0, 0, "
                " '2025-01-07T09:00:00Z', '2025-01-07T10:00:00Z', '2025-01-07T10:00:00Z')",
                (REPO, 55),
            )
            # pr_issues linkage: in-week PR 55 closes stale issue 77
            gh_conn.execute(
                "INSERT INTO pr_issues (repo, pr_number, issue_number) VALUES (?, ?, ?)",
                (REPO, 55, 77),
            )
            gh_conn.commit()
        finally:
            gh_conn.close()

        build_graph(sess_db, gh_db, week_start=week)

        conn = sqlite3.connect(str(gh_db))
        try:
            node_ids = {
                row[0]
                for row in conn.execute(
                    "SELECT node_id FROM graph_nodes WHERE week_start = ?",
                    (week,),
                ).fetchall()
            }
        finally:
            conn.close()

        assert f"pr:{REPO}#55" in node_ids, "In-week PR must be present"
        assert f"issue:{REPO}#77" in node_ids, (
            "Stale issue linked to an in-week PR must be pulled into the graph"
        )

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


class TestIssueHasSessionEdge:
    """Epic #93 / Slice 2: build_graph emits issue_has_session /
    issue_refs_session edges (issue → session, traversal='own') from
    session_gh_events. The legacy ``session_refs_issue`` edge type is
    abandoned. Session→issue linkage is now first-class topology, and
    ``session_issue_attribution`` is a derived cache projected from these
    edges.
    """

    def test_issue_create_emits_issue_has_session_edge(self, tmp_path):
        """An issue_create event emits an issue_has_session edge (issue → session)."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_issue(gh_conn, REPO, 42)
            _seed_session_gh_event(
                gh_conn, SESSION_UUID, "issue_create", REPO, "42"
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
            rows = conn.execute(
                "SELECT src_node_id, dst_node_id, edge_type, traversal "
                "FROM graph_edges WHERE edge_type = 'issue_has_session'"
            ).fetchall()
        finally:
            conn.close()

        assert (f"issue:{REPO}#42", f"session:{SESSION_UUID}",
                "issue_has_session", "own") in rows, (
            f"Expected issue_has_session edge issue->session traversal=own; got {rows}"
        )

    def test_issue_comment_emits_issue_refs_session_edge(self, tmp_path):
        """An issue_comment event emits an issue_refs_session edge."""
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
            rows = conn.execute(
                "SELECT src_node_id, dst_node_id, edge_type, traversal "
                "FROM graph_edges WHERE edge_type = 'issue_refs_session'"
            ).fetchall()
        finally:
            conn.close()

        assert (f"issue:{REPO}#42", f"session:{SESSION_UUID}",
                "issue_refs_session", "own") in rows, (
            f"Expected issue_refs_session edge issue->session traversal=own; got {rows}"
        )

    def test_session_issue_attribution_row_still_written(self, tmp_path):
        """The derived attribution cache is still populated alongside the new edges."""
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
            rows = conn.execute(
                "SELECT session_uuid, repo, issue_number, fraction, phase "
                "FROM session_issue_attribution "
                "WHERE session_uuid = ? AND repo = ? AND issue_number = ?",
                (SESSION_UUID, REPO, 42),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected 1 session_issue_attribution row (derived from edges), "
            f"got {len(rows)}: {rows}"
        )

    def test_no_edge_when_issue_missing(self, tmp_path):
        """No edge is emitted when the target issue node does not exist
        (and the event is issue_comment, not issue_create which bootstraps)."""
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
            edges = conn.execute(
                "SELECT edge_type FROM graph_edges"
            ).fetchall()
            nodes = [
                nid for nid, in conn.execute(
                    "SELECT node_id FROM graph_nodes"
                ).fetchall()
            ]
        finally:
            conn.close()

        assert all(e[0] not in ("issue_has_session", "issue_refs_session")
                   for e in edges), (
            f"No issue→session edge should exist when issue node is missing; got {edges}"
        )
        assert f"issue:{REPO}#42" not in nodes

    def test_git_push_does_not_emit_edge(self, tmp_path):
        """git_push events do not produce any issue/PR-session edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
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
            edges = conn.execute(
                "SELECT edge_type FROM graph_edges"
            ).fetchall()
        finally:
            conn.close()

        bad_types = {"issue_has_session", "issue_refs_session",
                     "pr_has_session", "pr_refs_session"}
        assert not any(e[0] in bad_types for e in edges), (
            f"git_push must not emit any session-link edge, got: {edges}"
        )

    def test_pr_comment_emits_pr_refs_session_edge(self, tmp_path):
        """A pr_comment event creates a pr_refs_session edge (PR → session,
        inverted from legacy session_refs_pr per Epic #93)."""
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
                (src, dst, etype, trav)
                for src, dst, etype, trav in conn.execute(
                    "SELECT src_node_id, dst_node_id, edge_type, traversal "
                    "FROM graph_edges"
                ).fetchall()
            }
        finally:
            conn.close()

        assert (f"pr:{REPO}#99", f"session:{SESSION_UUID}",
                "pr_refs_session", "own") in edges, (
            f"Expected pr_refs_session edge pr->session traversal=own; got: {edges}"
        )

    def test_pending_ref_skipped(self, tmp_path):
        """A session_gh_events row with ref='pending' is not turned into an edge."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_issue(gh_conn, REPO, 1)
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
            edges = conn.execute(
                "SELECT edge_type FROM graph_edges"
            ).fetchall()
        finally:
            conn.close()

        bad_types = {"issue_has_session", "issue_refs_session"}
        assert not any(e[0] in bad_types for e in edges), (
            f"ref='pending' must not produce an issue→session edge, got: {edges}"
        )


# ---------------------------------------------------------------------------
# Issue #68 — AS-1: session_refs_issue rows absent from graph_edges (Shape A)
# ---------------------------------------------------------------------------


TWO_ISSUE_FIXTURE = Path(__file__).parent / "fixtures" / "two_issue_session.jsonl"
TWO_ISSUE_SESSION_UUID = "f2000000-0000-0000-0000-000000000002"
TWO_ISSUE_REPO = "hyang0129/video_agent_long"
TWO_ISSUE_WEEK = "2026-03-23"


def _ingest_two_issue_fixture(tmp_path: Path) -> tuple:
    """Parse and upsert the two-issue fixture; return (sess_db, gh_db).

    Both DBs land in *tmp_path* so that ``upsert_session`` finds
    ``github.db`` next to ``sessions.db`` automatically.
    """
    from am_i_shipping.db import init_github_db, init_sessions_db
    from collector.session_parser import parse_session
    from collector.store import upsert_session

    sess_db = tmp_path / "sessions.db"
    gh_db = tmp_path / "github.db"
    init_sessions_db(sess_db)
    init_github_db(gh_db)

    record = parse_session(TWO_ISSUE_FIXTURE)
    upsert_session(record, db_path=sess_db, data_dir=tmp_path, skip_health=True)
    return sess_db, gh_db


class TestEpic93IssueHasSessionInGraphEdges:
    """Epic #93 / Slice 2: session→issue linkage now lives in graph_edges
    as ``issue_has_session`` / ``issue_refs_session`` (issue → session,
    traversal='own'). PR bridging uses the inverted ``pr_has_session`` /
    ``pr_refs_session`` edges.
    """

    def test_issue_has_or_refs_session_rows_present(self, tmp_path):
        """After build_graph on the two-issue fixture, graph_edges has
        issue_has_session or issue_refs_session rows for the session."""
        sess_db, gh_db = _ingest_two_issue_fixture(tmp_path)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM graph_edges "
                "WHERE edge_type IN ('issue_has_session', 'issue_refs_session') "
                "AND traversal = 'own'",
            ).fetchone()[0]
        finally:
            conn.close()

        assert count > 0, (
            f"Expected issue_has_session/issue_refs_session rows in graph_edges "
            f"(Epic #93), got {count}"
        )

    def test_pr_session_rows_still_present(self, tmp_path):
        """pr_has_session / pr_refs_session edges still exist in graph_edges."""
        sess_db, gh_db = _ingest_two_issue_fixture(tmp_path)
        build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK)

        conn = sqlite3.connect(str(gh_db))
        try:
            pr_edge_count = conn.execute(
                "SELECT COUNT(*) FROM graph_edges "
                "WHERE edge_type IN ('pr_refs_session', 'pr_has_session')",
            ).fetchone()[0]
        finally:
            conn.close()

        assert pr_edge_count > 0, (
            "pr_refs_session / pr_has_session edges must exist in graph_edges "
            "for PR bridging (Epic #93)"
        )


class TestIssue68FractionDenominatorInflation:
    """Critical fix: a session that emits both issue_create AND issue_comment
    for the same (repo, issue) pair must count as ONE distinct issue, not two,
    so fraction=1.0 and phase='planning' (create wins over comment).
    """

    def test_create_and_comment_same_issue_fraction_one(self, tmp_path):
        """issue_create + issue_comment on the same issue → fraction=1.0 (not 0.5)
        and phase='planning' (create wins) in session_issue_attribution."""
        gh_db = _make_gh_db(tmp_path)
        sess_db = _make_sess_db(tmp_path)

        gh_conn = sqlite3.connect(str(gh_db))
        sess_conn = sqlite3.connect(str(sess_db))
        try:
            _seed_issue(gh_conn, REPO, 99)
            # Both issue_create AND issue_comment for the same issue in one session.
            _seed_session_gh_event(gh_conn, SESSION_UUID, "issue_create", REPO, "99")
            _seed_session_gh_event(gh_conn, SESSION_UUID, "issue_comment", REPO, "99")
            gh_conn.commit()

            _seed_session(sess_conn, SESSION_UUID, "2025-01-10T10:00:00Z")
            sess_conn.commit()
        finally:
            gh_conn.close()
            sess_conn.close()

        build_graph(sess_db, gh_db)

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT fraction, phase FROM session_issue_attribution "
                "WHERE session_uuid = ? AND repo = ? AND issue_number = ?",
                (SESSION_UUID, REPO, 99),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected exactly 1 attribution row for same (session, repo, issue) "
            f"regardless of how many event types were emitted; got {len(rows)}: {rows}"
        )
        fraction, phase = rows[0]
        assert abs(fraction - 1.0) < 1e-9, (
            f"Expected fraction=1.0 for single distinct issue (not inflated by "
            f"multiple event types), got fraction={fraction}"
        )
        assert phase == "planning", (
            f"issue_create + issue_comment → planning must win, got phase={phase!r}"
        )
