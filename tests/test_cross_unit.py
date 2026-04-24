"""Tests for ``synthesis/cross_unit.py`` (Epic #17 — Issue #38).

These tests use in-memory-style SQLite fixtures built from scratch per
test (via ``tmp_path``) — no dependency on the committed
``golden.sqlite`` is needed because the cross-unit pass only reads the
``units`` + ``graph_nodes`` + ``graph_edges`` tables, all of which we
populate directly here.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from am_i_shipping.db import init_github_db
from synthesis.cross_unit import compute_flags


WEEK_START = "2025-04-07"
# Pinned "now" lets the abandonment tests pick their own cutoffs
# without depending on wall-clock time.
PINNED_NOW = datetime(2025, 4, 14, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path) -> Path:
    """Create a fresh github.db with all schema + migrations applied."""
    db_path = tmp_path / "github.db"
    init_github_db(db_path)
    return db_path


def _insert_unit(
    conn: sqlite3.Connection,
    *,
    unit_id: str,
    root_node_id: str,
    elapsed_days: float | None = 1.0,
    dark_time_pct: float | None = 0.0,
    total_reprompts: int | None = 0,
    review_cycles: int | None = 0,
    status: str = "closed",
) -> None:
    """Insert one units row with sensible defaults."""
    conn.execute(
        "INSERT INTO units "
        "(week_start, unit_id, root_node_type, root_node_id, "
        " elapsed_days, dark_time_pct, total_reprompts, "
        " review_cycles, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            WEEK_START,
            unit_id,
            "issue",
            root_node_id,
            elapsed_days,
            dark_time_pct,
            total_reprompts,
            review_cycles,
            status,
        ),
    )


def _insert_node(
    conn: sqlite3.Connection,
    *,
    node_id: str,
    created_at: str,
) -> None:
    """Insert one graph_nodes row linked to WEEK_START."""
    conn.execute(
        "INSERT INTO graph_nodes "
        "(week_start, node_id, node_type, node_ref, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (WEEK_START, node_id, "issue", None, created_at),
    )


# ---------------------------------------------------------------------------
# Empty-week behaviour
# ---------------------------------------------------------------------------


class TestEmptyWeek:
    def test_no_units_is_noop(self, tmp_path: Path) -> None:
        """Running against a week with no units must not raise."""
        db_path = _make_db(tmp_path)
        result = compute_flags(
            str(db_path), WEEK_START, now=PINNED_NOW
        )
        assert result == 0

    def test_week_with_only_null_metrics(self, tmp_path: Path) -> None:
        """All-NULL metrics flag nothing as an outlier (thresholds skip)."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn,
                unit_id="u-null-1",
                root_node_id="n-null-1",
                elapsed_days=None,
                dark_time_pct=None,
                total_reprompts=None,
                review_cycles=None,
            )
            _insert_node(
                conn,
                node_id="n-null-1",
                created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            conn.commit()
        finally:
            conn.close()

        updated = compute_flags(
            str(db_path), WEEK_START, now=PINNED_NOW
        )
        assert updated == 1

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT outlier_flags, abandonment_flag "
                "FROM units WHERE unit_id = ?",
                ("u-null-1",),
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == "[]"  # no metrics evaluable, so no flags
        assert row[1] == 0     # timestamp is exactly "now", recent


# ---------------------------------------------------------------------------
# Outlier threshold boundaries (parametrized)
# ---------------------------------------------------------------------------


# Population for the boundary tests: 9 units with elapsed_days = 1..9,
# plus the target unit under test. ``compute_flags`` computes median +
# stdev over ALL units for the week (including the target itself), so
# the "at cutoff" and "just above" values are pre-computed against the
# full 10-unit population rather than the 9-unit base. Keeping this as
# hard-coded numbers (not a helper-derived expression) makes the
# expected thresholds explicit to anyone reading the test.
_POPULATION = [float(x) for x in range(1, 10)]  # 1..9


# Thresholds pre-computed for target in {11.922889, 11.924} against the
# full 10-unit population (pop9 + [target]). ``11.922888752...`` is the
# exact fixed point where ``target == median(pop+[target]) + 2σ``; a
# hair above trips the flag.
_BOUNDARY_EXACT = 11.922888752197984
_BOUNDARY_JUST_ABOVE = 11.93  # comfortably above the fixed point


@pytest.mark.parametrize(
    "target_value,expected_flagged",
    [
        # Well below the 2σ cutoff — not flagged.
        pytest.param(10.0, False, id="just_below_2sigma"),
        # Comfortably above — flagged.
        pytest.param(50.0, True, id="far_above_2sigma"),
        # At the fixed-point cutoff — not flagged (comparison is strict >).
        pytest.param(
            _BOUNDARY_EXACT,
            False,
            id="exactly_at_2sigma_cutoff",
        ),
        # A hair above the fixed point — flagged.
        pytest.param(
            _BOUNDARY_JUST_ABOVE,
            True,
            id="just_above_2sigma_cutoff",
        ),
    ],
)
def test_outlier_boundary(
    tmp_path: Path, target_value: float, expected_flagged: bool
) -> None:
    """Unit with ``elapsed_days == target_value`` — is it flagged?"""
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    try:
        # Seed the 9-unit population.
        for i, val in enumerate(_POPULATION, start=1):
            _insert_unit(
                conn,
                unit_id=f"u-pop-{i}",
                root_node_id=f"n-pop-{i}",
                elapsed_days=val,
            )
            _insert_node(
                conn,
                node_id=f"n-pop-{i}",
                created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        # Target unit under test.
        _insert_unit(
            conn,
            unit_id="u-target",
            root_node_id="n-target",
            elapsed_days=target_value,
        )
        _insert_node(
            conn,
            node_id="n-target",
            created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        conn.commit()
    finally:
        conn.close()

    compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT outlier_flags FROM units WHERE unit_id = ?",
            ("u-target",),
        ).fetchone()
    finally:
        conn.close()
    flags = json.loads(row[0])
    if expected_flagged:
        assert "elapsed_days" in flags, (
            f"expected 'elapsed_days' in outlier_flags, got {flags!r} "
            f"for target_value={target_value}"
        )
    else:
        assert "elapsed_days" not in flags, (
            f"did not expect 'elapsed_days' in outlier_flags, got {flags!r} "
            f"for target_value={target_value}"
        )


class TestOutlierMultiMetric:
    def test_flags_multiple_metrics(self, tmp_path: Path) -> None:
        """One unit can breach on more than one metric at once."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            # Population of 9 small units.
            for i in range(1, 10):
                _insert_unit(
                    conn,
                    unit_id=f"u-pop-{i}",
                    root_node_id=f"n-pop-{i}",
                    elapsed_days=1.0,
                    total_reprompts=1,
                )
                _insert_node(
                    conn,
                    node_id=f"n-pop-{i}",
                    created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
            # One outlier on both elapsed_days and total_reprompts.
            _insert_unit(
                conn,
                unit_id="u-big",
                root_node_id="n-big",
                elapsed_days=100.0,
                total_reprompts=100,
            )
            _insert_node(
                conn,
                node_id="n-big",
                created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT outlier_flags FROM units WHERE unit_id = ?",
                ("u-big",),
            ).fetchone()
        finally:
            conn.close()
        flags = set(json.loads(row[0]))
        assert flags == {"elapsed_days", "total_reprompts"}

    def test_single_unit_never_flags_itself(self, tmp_path: Path) -> None:
        """Population stdev is 0 with one unit — never breach."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn,
                unit_id="u-solo",
                root_node_id="n-solo",
                elapsed_days=999.0,
                total_reprompts=999,
            )
            _insert_node(
                conn,
                node_id="n-solo",
                created_at=PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT outlier_flags FROM units WHERE unit_id = ?",
                ("u-solo",),
            ).fetchone()
        finally:
            conn.close()
        assert json.loads(row[0]) == []


# ---------------------------------------------------------------------------
# Abandonment flag — injected ``now``
# ---------------------------------------------------------------------------


class TestAbandonmentFlag:
    def test_recent_activity_is_active(self, tmp_path: Path) -> None:
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-active", root_node_id="n-active"
            )
            # Event 1 day before pinned now — well within 14-day window.
            recent_ts = (PINNED_NOW - timedelta(days=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _insert_node(conn, node_id="n-active", created_at=recent_ts)
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-active",),
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == 0

    def test_old_activity_is_abandoned(self, tmp_path: Path) -> None:
        """Issue #98: compute_flags no longer writes abandonment_flag=1.

        The abandoned signal is now ``status == 'abandoned'`` from
        ``_summarise_unit``. ``abandonment_flag`` is kept for backward
        compatibility but is always written as 0 by compute_flags.
        """
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-stale", root_node_id="n-stale"
            )
            # 30 days ago — well outside 14-day window.
            stale_ts = (PINNED_NOW - timedelta(days=30)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _insert_node(conn, node_id="n-stale", created_at=stale_ts)
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-stale",),
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        # Stale/abandoned detection uses status == "abandoned" from _summarise_unit.
        assert row[0] == 0

    @pytest.mark.parametrize(
        "days_ago,expected_flag",
        [
            # Issue #98: abandonment_flag is always 0 from compute_flags.
            # All boundary cases now expect 0 — the retired flag is no longer
            # set to 1 regardless of activity age.
            pytest.param(0, 0, id="today"),
            pytest.param(13, 0, id="13_days_ago_still_active"),
            pytest.param(14, 0, id="exactly_14_days_ago_still_active"),
            pytest.param(15, 0, id="15_days_ago_flag_retired"),
            pytest.param(100, 0, id="100_days_ago_flag_retired"),
        ],
    )
    def test_abandonment_boundary(
        self, tmp_path: Path, days_ago: int, expected_flag: int
    ) -> None:
        """Check the exact 14-day boundary using an injected ``now``.

        Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        """
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-edge", root_node_id="n-edge"
            )
            ts = (PINNED_NOW - timedelta(days=days_ago)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _insert_node(conn, node_id="n-edge", created_at=ts)
            conn.commit()
        finally:
            conn.close()

        compute_flags(
            str(db_path),
            WEEK_START,
            abandonment_days=14,
            now=PINNED_NOW,
        )

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-edge",),
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == expected_flag

    def test_no_nodes_is_abandoned(self, tmp_path: Path) -> None:
        """Unit whose root node is missing from graph_nodes — abandonment_flag retired.

        Issue #98: compute_flags no longer writes abandonment_flag=1 even
        for units with missing nodes. The flag is always 0.
        """
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-orphan", root_node_id="n-missing"
            )
            # intentionally NOT inserting n-missing
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-orphan",),
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        assert row[0] == 0

    @pytest.mark.parametrize(
        "bad_ts",
        [
            pytest.param(None, id="null_created_at"),
            pytest.param("", id="empty_created_at"),
            pytest.param("not-a-date", id="garbage_created_at"),
        ],
    )
    def test_node_with_unparseable_ts_is_abandoned(
        self, tmp_path: Path, bad_ts
    ) -> None:
        """Node exists but its ``created_at`` can't be parsed — abandoned.

        Pins the docstring promise: "Units with no parseable timestamp
        on any node count as abandoned — the absence of a dated signal
        is itself a signal". Complements ``test_no_nodes_is_abandoned``
        (which covers the missing-node branch) by exercising the
        present-node-but-unparseable-ts branch.
        """
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-bad-ts", root_node_id="n-bad-ts"
            )
            _insert_node(conn, node_id="n-bad-ts", created_at=bad_ts)
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-bad-ts",),
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        assert row[0] == 0

    def test_issue_closed_recently_is_not_abandoned(self, tmp_path: Path) -> None:
        """Issue created 30 days ago, closed 5 days ago → abandonment_flag=0."""
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-closed", root_node_id="n-closed-issue")
            # graph_nodes row: created_at is 30 days ago (old)
            old_ts = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            # but node_ref points to an issue closed 5 days ago
            recent_ts = (now - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-closed-issue", "issue", "testrepo/repo#1", old_ts),
            )
            conn.execute(
                "INSERT INTO issues (repo, issue_number, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("testrepo/repo", 1, old_ts, recent_ts, recent_ts),
            )
            conn.commit()
        finally:
            conn.close()
        compute_flags(str(db_path), WEEK_START, now=now)
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?", ("u-closed",)
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == 0  # closed recently → not abandoned

    def test_issue_old_no_update_is_abandoned(self, tmp_path: Path) -> None:
        """Issue created 30 days ago, never updated, never closed → abandonment_flag=1."""
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-old-open", root_node_id="n-old-issue")
            old_ts = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-old-issue", "issue", "testrepo/repo#2", old_ts),
            )
            conn.execute(
                "INSERT INTO issues (repo, issue_number, created_at, closed_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("testrepo/repo", 2, old_ts, None, None),
            )
            conn.commit()
        finally:
            conn.close()
        compute_flags(str(db_path), WEEK_START, now=now)
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?", ("u-old-open",)
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        assert row[0] == 0  # flag retired; stale detection via status=="abandoned"

    def test_pr_merged_recently_is_not_abandoned(self, tmp_path: Path) -> None:
        """PR created 30 days ago, merged 5 days ago → abandonment_flag=0."""
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-merged-pr", root_node_id="n-merged-pr")
            old_ts = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            recent_ts = (now - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-merged-pr", "pr", "testrepo/repo#10", old_ts),
            )
            conn.execute(
                "INSERT INTO pull_requests (repo, pr_number, created_at, merged_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("testrepo/repo", 10, old_ts, recent_ts, recent_ts),
            )
            conn.commit()
        finally:
            conn.close()
        compute_flags(str(db_path), WEEK_START, now=now)
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?", ("u-merged-pr",)
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == 0  # merged recently → not abandoned

    def test_pr_old_no_activity_is_abandoned(self, tmp_path: Path) -> None:
        """PR created 30 days ago, never updated, never merged → abandonment_flag=1."""
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-old-pr", root_node_id="n-old-pr")
            old_ts = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-old-pr", "pr", "testrepo/repo#11", old_ts),
            )
            conn.execute(
                "INSERT INTO pull_requests (repo, pr_number, created_at, merged_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                ("testrepo/repo", 11, old_ts, None, None),
            )
            conn.commit()
        finally:
            conn.close()
        compute_flags(str(db_path), WEEK_START, now=now)
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?", ("u-old-pr",)
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        assert row[0] == 0  # flag retired; stale detection via status=="abandoned"

    def test_issue_with_unparseable_noderef_uses_graph_nodes_ts(
        self, tmp_path: Path
    ) -> None:
        """node_ref with non-numeric issue number falls back to graph_nodes.created_at.

        When the ``#<number>`` part of a node_ref cannot be parsed as an
        integer, the issue-activity lookup is skipped (``continue``).
        The nodes map therefore stores ``graph_nodes.created_at`` for that
        node.  A recent ``created_at`` must yield ``abandonment_flag=0``.
        """
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-badref", root_node_id="n-badref")
            recent_ts = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            # node_ref has a non-numeric issue number — cannot be parsed as int
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-badref", "issue", "testrepo/repo#not-a-number", recent_ts),
            )
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=now)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-badref",),
            ).fetchone()
        finally:
            conn.close()
        # Fallback to graph_nodes.created_at (recent) → not abandoned
        assert row[0] == 0

    def test_issue_with_no_source_row_uses_graph_nodes_ts(
        self, tmp_path: Path
    ) -> None:
        """node_ref pointing to a non-existent issue falls back to graph_nodes.created_at.

        When the issue number parses successfully but no matching row
        exists in the ``issues`` table, ``issue_activity`` has no entry
        for that ref.  The nodes map therefore stores
        ``graph_nodes.created_at`` for that node.  A recent
        ``created_at`` must yield ``abandonment_flag=0``.
        """
        db_path = _make_db(tmp_path)
        now = PINNED_NOW
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(conn, unit_id="u-norow", root_node_id="n-norow")
            recent_ts = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            # node_ref points to issue #99999 which does not exist in issues table
            conn.execute(
                "INSERT INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "n-norow", "issue", "owner/repo#99999", recent_ts),
            )
            # Intentionally NOT inserting a row into issues for owner/repo#99999
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=now)

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-norow",),
            ).fetchone()
        finally:
            conn.close()
        # Fallback to graph_nodes.created_at (recent) → not abandoned
        assert row[0] == 0

    def test_abandonment_days_override(self, tmp_path: Path) -> None:
        """abandonment_days param is accepted; abandonment_flag is always 0 (retired).

        Issue #98: the abandonment_days parameter still governs
        _summarise_unit behavior, but compute_flags no longer writes
        abandonment_flag=1.
        """
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-strict", root_node_id="n-strict"
            )
            ts = (PINNED_NOW - timedelta(days=10)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _insert_node(conn, node_id="n-strict", created_at=ts)
            conn.commit()
        finally:
            conn.close()

        compute_flags(
            str(db_path),
            WEEK_START,
            abandonment_days=7,
            now=PINNED_NOW,
        )

        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT abandonment_flag FROM units WHERE unit_id = ?",
                ("u-strict",),
            ).fetchone()
        finally:
            conn.close()
        # Issue #98: abandonment_flag is retired — always 0 from compute_flags.
        assert row[0] == 0


# ---------------------------------------------------------------------------
# Idempotency / re-run
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_rerun_overwrites_previous_flags(
        self, tmp_path: Path
    ) -> None:
        """Second call with different data reflects the new state."""
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            _insert_unit(
                conn, unit_id="u-a", root_node_id="n-a",
                elapsed_days=1.0,
            )
            ts_recent = PINNED_NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
            _insert_node(conn, node_id="n-a", created_at=ts_recent)
            conn.commit()
        finally:
            conn.close()

        # First pass — 1 unit, no flags.
        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        # Add a second, larger unit; rerun and expect the small one
        # still unflagged and the cluster re-evaluated correctly.
        conn = sqlite3.connect(str(db_path))
        try:
            for i in range(2, 11):
                _insert_unit(
                    conn,
                    unit_id=f"u-pop-{i}",
                    root_node_id=f"n-pop-{i}",
                    elapsed_days=1.0,
                )
                _insert_node(
                    conn,
                    node_id=f"n-pop-{i}",
                    created_at=ts_recent,
                )
            conn.commit()
        finally:
            conn.close()

        compute_flags(str(db_path), WEEK_START, now=PINNED_NOW)

        conn = sqlite3.connect(str(db_path))
        try:
            rows = dict(
                conn.execute(
                    "SELECT unit_id, outlier_flags FROM units "
                    "WHERE week_start = ?",
                    (WEEK_START,),
                ).fetchall()
            )
        finally:
            conn.close()
        assert json.loads(rows["u-a"]) == []
        # The population is all 1.0 — nobody breaches.
        for k, v in rows.items():
            assert json.loads(v) == []


# ---------------------------------------------------------------------------
# Schema — migration added the new columns
# ---------------------------------------------------------------------------


class TestSchema:
    def test_units_has_flag_columns(self, tmp_path: Path) -> None:
        db_path = _make_db(tmp_path)
        conn = sqlite3.connect(str(db_path))
        try:
            cols = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(units)"
                ).fetchall()
            }
        finally:
            conn.close()
        assert "outlier_flags" in cols
        assert "abandonment_flag" in cols
