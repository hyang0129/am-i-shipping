"""Component tests: synthesis.coverage.backfill_full → collector.session_parser.parse_session
→ collector.store.upsert_session → pr_sessions in github.db.

Boundary: the pr_link ingestion path through the full backfill pipeline.

When ``backfill_full`` re-ingests a JSONL containing ``pr-link`` entries the
wiring must produce a row in ``github.db::pr_sessions``.  No earlier test
exercises all three modules cooperating across this exact path:

  1. ``synthesis.coverage.backfill_full`` discovers the JSONL and calls
     ``parse_session`` on it.
  2. ``parse_session`` extracts the ``pr_link`` gh_event via
     ``_extract_pr_link_event`` and returns a ``SessionRecord``.
  3. ``upsert_session`` writes the ``pr_sessions`` row alongside the
     ``session_gh_events`` audit row.

These are two or more real modules cooperating across one named boundary with
only the filesystem and SQLite as infrastructure — no mocks of any module
under test.  The tests use the canonical ``pr_link_session.jsonl`` fixture
committed with the PR.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.db import init_github_db, init_sessions_db
from synthesis.coverage import backfill_full

FIXTURES = Path(__file__).parent / "fixtures"
PR_LINK_FIXTURE = FIXTURES / "pr_link_session.jsonl"


def _projects_dir_with_pr_link(tmp_path: Path) -> Path:
    """Create a minimal projects_path that contains the pr-link fixture JSONL.

    ``backfill_full`` uses ``_build_uuid_index`` which rglob-s for ``*.jsonl``
    files under ``projects_path`` (skipping ``subagents/`` subdirectories).
    We mirror the real layout: one project directory containing the JSONL.
    """
    project_dir = tmp_path / "projects" / "test-project"
    project_dir.mkdir(parents=True)
    dest = project_dir / "test-pr-link-uuid.jsonl"
    shutil.copy(PR_LINK_FIXTURE, dest)
    return tmp_path / "projects"


def _seed_empty_sessions_row(db_path: Path, session_uuid: str) -> None:
    """Insert a sessions row with NULL raw_content_json to simulate an
    un-backfilled row that backfill_full should overwrite.

    ``backfill_full`` re-ingests *every* JSONL in projects_path, not just
    those with NULL raw_content_json, so the seed row is not strictly
    required — but it makes the test more realistic and the assertion about
    the sessions table more meaningful.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO sessions (
                session_uuid, turn_count, tool_call_count,
                tool_failure_count, reprompt_count, bail_out,
                session_duration_seconds, working_directory,
                git_branch, raw_content_json
            ) VALUES (?, 0, 0, 0, 0, 0, 0.0, NULL, NULL, NULL)
            """,
            (session_uuid,),
        )
        conn.commit()
    finally:
        conn.close()


class TestBackfillFullPopulatesPrSessions:
    """backfill_full → parse_session → upsert_session → pr_sessions contract."""

    SESSION_UUID = "test-pr-link-uuid"

    def test_pr_sessions_row_present_after_backfill(self, tmp_path: Path) -> None:
        """[Arrange] Empty databases + JSONL with pr-link entries on disk.
        [Act] Call backfill_full.
        [Assert] pr_sessions contains exactly 1 row for the session with the
        correct repo and pr_number.
        """
        sess_db = tmp_path / "sessions.db"
        gh_db = tmp_path / "github.db"
        init_sessions_db(sess_db)
        init_github_db(gh_db)
        _seed_empty_sessions_row(sess_db, self.SESSION_UUID)
        projects_path = _projects_dir_with_pr_link(tmp_path)

        # [Act]
        summary = backfill_full(sess_db, projects_path, data_dir=tmp_path)

        # [Assert]
        assert summary.reingested >= 1, (
            f"backfill_full must report at least 1 reingested session, got {summary.reingested}"
        )

        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT repo, pr_number, session_uuid FROM pr_sessions "
                "WHERE session_uuid = ?",
                (self.SESSION_UUID,),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected exactly 1 pr_sessions row after backfill_full, got {len(rows)}: {rows}"
        )
        repo, pr_number, session_uuid = rows[0]
        assert repo == "hyang0129/am-i-shipping"
        assert pr_number == 130
        assert session_uuid == self.SESSION_UUID

    def test_pr_sessions_idempotent_across_two_backfills(self, tmp_path: Path) -> None:
        """Running backfill_full twice must not duplicate pr_sessions rows.

        ``upsert_session``'s ``INSERT OR IGNORE INTO pr_sessions`` clause
        guarantees idempotency at the store level; this test verifies the
        contract survives the full pipeline invocation.
        """
        sess_db = tmp_path / "sessions.db"
        gh_db = tmp_path / "github.db"
        init_sessions_db(sess_db)
        init_github_db(gh_db)
        projects_path = _projects_dir_with_pr_link(tmp_path)

        # [Act] — two consecutive full backfills
        backfill_full(sess_db, projects_path, data_dir=tmp_path)
        backfill_full(sess_db, projects_path, data_dir=tmp_path)

        # [Assert]
        conn = sqlite3.connect(str(gh_db))
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM pr_sessions WHERE session_uuid = ?",
                (self.SESSION_UUID,),
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 1, (
            f"Expected exactly 1 pr_sessions row after two backfills, got {count} "
            "(upsert_session INSERT OR IGNORE idempotency failed across the pipeline)"
        )

    def test_session_gh_events_audit_row_present_after_backfill(self, tmp_path: Path) -> None:
        """backfill_full must also write the session_gh_events audit row for the pr_link event.

        pr_sessions captures graph-linkage; session_gh_events captures the audit
        trail.  Both must be written; this test verifies the audit boundary.
        """
        sess_db = tmp_path / "sessions.db"
        gh_db = tmp_path / "github.db"
        init_sessions_db(sess_db)
        init_github_db(gh_db)
        projects_path = _projects_dir_with_pr_link(tmp_path)

        # [Act]
        backfill_full(sess_db, projects_path, data_dir=tmp_path)

        # [Assert]
        conn = sqlite3.connect(str(gh_db))
        try:
            rows = conn.execute(
                "SELECT event_type, repo, ref FROM session_gh_events "
                "WHERE session_uuid = ? AND event_type = 'pr_link'",
                (self.SESSION_UUID,),
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, (
            f"Expected 1 session_gh_events row with event_type='pr_link', got {len(rows)}: {rows}"
        )
        event_type, repo, ref = rows[0]
        assert event_type == "pr_link"
        assert repo == "hyang0129/am-i-shipping"
        assert ref == "130"

    def test_three_repeated_pr_links_produce_single_pr_sessions_row(
        self, tmp_path: Path
    ) -> None:
        """The pr_link_session.jsonl fixture has 3 identical pr-link entries.

        After deduplication in parse_session and INSERT OR IGNORE in
        upsert_session, exactly 1 pr_sessions row must appear.  This verifies
        that the dedup contract in parse_session carries through the pipeline.
        """
        sess_db = tmp_path / "sessions.db"
        gh_db = tmp_path / "github.db"
        init_sessions_db(sess_db)
        init_github_db(gh_db)
        projects_path = _projects_dir_with_pr_link(tmp_path)

        # [Act]
        backfill_full(sess_db, projects_path, data_dir=tmp_path)

        # [Assert] — pr_sessions must have exactly 1 row
        conn = sqlite3.connect(str(gh_db))
        try:
            pr_count = conn.execute(
                "SELECT COUNT(*) FROM pr_sessions WHERE session_uuid = ?",
                (self.SESSION_UUID,),
            ).fetchone()[0]
            # Also verify the session_gh_events side: dedup in parse_session collapses
            # the 3 repeated pr_link entries to 1 gh_event before upsert_session.
            gh_event_count = conn.execute(
                "SELECT COUNT(*) FROM session_gh_events "
                "WHERE session_uuid = ? AND event_type = 'pr_link'",
                (self.SESSION_UUID,),
            ).fetchone()[0]
        finally:
            conn.close()

        assert pr_count == 1, (
            f"Expected 1 pr_sessions row for 3 repeated pr_link entries in JSONL, got {pr_count}"
        )
        assert gh_event_count == 1, (
            f"Expected 1 session_gh_events pr_link row (deduped), got {gh_event_count}"
        )
