"""Tests for ``synthesis/prepare.py`` — the ``am-prepare-week`` CLI (Issue #52).

These tests exercise the end-to-end orchestration: starting from a DB that
has collector-side data (sessions, issues, PRs, commits, timeline) but no
derived graph/unit rows for the target week, invoking :func:`main` must
populate ``graph_nodes``, ``graph_edges``, and ``units`` and leave the
derived flag columns filled in.

The fixture used here is ``tests/fixtures/synthesis/golden.sqlite``. It
ships pre-built graph/unit rows for ``WEEK_START``; every test clears
those rows first so the CLI has something to create rather than just
insert-or-ignoring against a pre-populated table. That way we can tell
the difference between "CLI did the work" and "row was already there".
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from textwrap import dedent

import pytest

from synthesis import prepare


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"
WEEK_START = "2025-01-06"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _clear_derived(db_path: Path) -> None:
    """Drop the CLI's output rows so we can observe the re-creation.

    The golden fixture ships with graph_nodes/graph_edges/units already
    populated for WEEK_START (8/5/3 rows). To verify that ``main`` is
    doing the work — rather than ``INSERT OR IGNORE`` silently skipping
    against an already-complete table — we wipe those three tables for
    the week first and then look at the post-CLI row counts.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        for table in ("graph_nodes", "graph_edges", "units"):
            conn.execute(f"DELETE FROM {table} WHERE week_start = ?", (WEEK_START,))
        conn.commit()
    finally:
        conn.close()


def _write_config(tmp_path: Path, data_dir: Path) -> Path:
    """Write a minimal config.yaml whose data_dir points at *data_dir*.

    ``load_config`` resolves ``data_dir`` relative to the directory of
    the config file, so we pin ``data_dir`` to an absolute path to keep
    the test robust against future changes to that anchoring rule.
    """
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        dedent(
            f"""
            session:
              projects_path: "{tmp_path}/projects"
            github:
              repos:
                - "hyang0129/fixture-repo"
            data:
              data_dir: "{data_dir}"
            synthesis:
              anthropic_api_key_env: "ANTHROPIC_API_KEY"
              model: "claude-sonnet-4-6"
              output_dir: "{tmp_path}/retrospectives"
              week_start: "monday"
              abandonment_days: 14
              outlier_sigma: 2.0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    return cfg


@pytest.fixture
def prepared_env(tmp_path: Path) -> tuple[Path, Path]:
    """Build a data dir with ``github.db`` = ``sessions.db`` = the fixture.

    Returns ``(config_path, data_dir)``. The golden fixture packs both
    the github and sessions schemas into one SQLite file — ``build_graph``
    detects the overlap — so we copy it under both expected filenames
    inside the data_dir. Then we clear the derived rows so the CLI has
    work to do.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    gh_db = data_dir / "github.db"
    sess_db = data_dir / "sessions.db"
    shutil.copy(FIXTURE_SRC, gh_db)
    shutil.copy(FIXTURE_SRC, sess_db)
    _clear_derived(gh_db)
    # sessions.db has no graph/units schema, so only github.db needs
    # the clear. But we copied the full fixture into both paths for
    # convenience; identify_units only reads sessions from sessions_db
    # so the duplicated graph tables in sessions.db are harmless.

    cfg_path = _write_config(tmp_path, data_dir)
    return cfg_path, data_dir


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def _counts(db_path: Path, week: str) -> dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE week_start = ?", (week,)
            ).fetchone()[0]
            for table in ("graph_nodes", "graph_edges", "units")
        }
    finally:
        conn.close()


class TestMain:
    def test_first_run_populates_graph_units_and_flags(
        self, prepared_env: tuple[Path, Path]
    ) -> None:
        """The three derived tables go from zero rows to non-zero rows."""
        cfg_path, data_dir = prepared_env
        gh_db = data_dir / "github.db"
        assert _counts(gh_db, WEEK_START) == {
            "graph_nodes": 0,
            "graph_edges": 0,
            "units": 0,
        }

        rc = prepare.main(["--week", WEEK_START, "--config", str(cfg_path)])

        assert rc == 0
        after = _counts(gh_db, WEEK_START)
        assert after["graph_nodes"] > 0
        assert after["graph_edges"] > 0
        assert after["units"] > 0

    def test_compute_flags_ran_over_units(
        self, prepared_env: tuple[Path, Path]
    ) -> None:
        """Every new units row must have ``outlier_flags`` populated.

        ``outlier_flags`` defaults to NULL in the schema; ``compute_flags``
        writes at minimum the empty JSON list ``"[]"`` for every unit in
        the week. So if compute_flags actually ran, no units row for the
        week has NULL in that column.
        """
        cfg_path, data_dir = prepared_env
        gh_db = data_dir / "github.db"

        rc = prepare.main(["--week", WEEK_START, "--config", str(cfg_path)])
        assert rc == 0

        conn = sqlite3.connect(str(gh_db))
        try:
            null_count = conn.execute(
                "SELECT COUNT(*) FROM units "
                "WHERE week_start = ? AND outlier_flags IS NULL",
                (WEEK_START,),
            ).fetchone()[0]
        finally:
            conn.close()

        assert null_count == 0, (
            "compute_flags left outlier_flags NULL on some units — "
            "the third stage did not run"
        )

    def test_idempotent_second_run_same_row_counts(
        self, prepared_env: tuple[Path, Path]
    ) -> None:
        """Running twice must not change the row counts for the week."""
        cfg_path, data_dir = prepared_env
        gh_db = data_dir / "github.db"

        rc1 = prepare.main(["--week", WEEK_START, "--config", str(cfg_path)])
        assert rc1 == 0
        after_first = _counts(gh_db, WEEK_START)

        rc2 = prepare.main(["--week", WEEK_START, "--config", str(cfg_path)])
        assert rc2 == 0
        after_second = _counts(gh_db, WEEK_START)

        assert after_first == after_second, (
            "row counts changed on re-run — am-prepare-week is not idempotent"
        )

    def test_empty_week_returns_zero(
        self, prepared_env: tuple[Path, Path]
    ) -> None:
        """A week with no matching graph data is still a success (exit 0).

        ``identify_units`` returns 0 when the graph has no rows for the
        target week; that is a valid "nothing to do" state, not a
        failure. build_graph always writes something when sessions exist,
        but using a week far in the future bypasses its session filter
        and leaves graph_nodes empty for that week.
        """
        cfg_path, _data_dir = prepared_env
        far_future = "2099-01-05"

        rc = prepare.main(["--week", far_future, "--config", str(cfg_path)])

        assert rc == 0


class TestArgParsing:
    def test_missing_week_errors(self, prepared_env: tuple[Path, Path]) -> None:
        """argparse exits non-zero when --week is omitted."""
        cfg_path, _data_dir = prepared_env
        with pytest.raises(SystemExit) as exc_info:
            prepare.main(["--config", str(cfg_path)])
        # argparse raises SystemExit(2) on a missing required arg.
        assert exc_info.value.code != 0

    def test_builds_parser_without_crash(self) -> None:
        """Sanity: the parser assembles and exposes the expected flags.

        Cheap check that catches accidental typos in the ``add_argument``
        calls (e.g. misspelt ``required=`` kwarg) without needing to
        invoke the full pipeline.
        """
        parser = prepare._build_parser()
        args = parser.parse_args(["--week", "2026-04-13"])
        assert args.week == "2026-04-13"
        assert args.config is None
