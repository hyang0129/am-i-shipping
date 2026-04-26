"""Tests for ``synthesis/rebuild.py`` — the ``am-rebuild-history`` CLI (Issue #104).

Covers Scenarios 1–4 and 6 from the slice spec:

* Scenario 1 — single command iterates every distinct ``week_start``.
* Scenario 2 — partial-failure restore from SQLite backup.
* Scenario 3 — destroyed ``expectation_corrections`` row count is
  surfaced loudly before the drop.
* Scenario 4 — idempotency: re-running produces identical state.
* Scenario 6 — schema migration is applied (legacy DBs without the
  ``traversal`` column come out with it).

The fixture mirrors ``tests/test_prepare.py`` — it reuses
``tests/fixtures/synthesis/golden.sqlite`` for the github + sessions
DBs and writes a synthetic ``expectations.db`` so we can verify the
expectations-side tables are dropped and recreated.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import sqlite3
from pathlib import Path
from textwrap import dedent

import pytest

from am_i_shipping.db import init_expectations_db
from synthesis import rebuild


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"
WEEK_START = "2025-01-06"


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_config(tmp_path: Path, data_dir: Path) -> Path:
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


def _seed_correction(expectations_db: Path, week: str = WEEK_START) -> None:
    """Insert one expectation_corrections row so we can observe destruction."""
    init_expectations_db(expectations_db)
    conn = sqlite3.connect(str(expectations_db))
    try:
        conn.execute(
            "INSERT INTO expectation_corrections "
            "(week_start, unit_id, facet, original_value, corrected_value, "
            "correction_note, corrected_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (week, "abc123def456", "scope", "old", "new", "note", "user"),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def env(tmp_path: Path) -> dict:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    gh_db = data_dir / "github.db"
    sess_db = data_dir / "sessions.db"
    exp_db = data_dir / "expectations.db"
    shutil.copy(FIXTURE_SRC, gh_db)
    shutil.copy(FIXTURE_SRC, sess_db)
    cfg_path = _write_config(tmp_path, data_dir)
    return {
        "cfg": cfg_path,
        "data_dir": data_dir,
        "github_db": gh_db,
        "sessions_db": sess_db,
        "expectations_db": exp_db,
    }


def _hash_table(db_path: Path, table: str) -> str:
    """Stable hash of a table's contents for idempotency assertions."""
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update(repr(row).encode("utf-8"))
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Scenario 1 — single-command rebuild iterates every past week_start
# ---------------------------------------------------------------------------


class TestScenario1IteratesAllWeeks:
    def test_main_rebuilds_known_week(self, env: dict) -> None:
        rc = rebuild.main(["--config", str(env["cfg"])])
        assert rc == 0
        # Post-rebuild: graph_nodes and units exist for the fixture's week
        with sqlite3.connect(str(env["github_db"])) as conn:
            n_nodes = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes WHERE week_start = ?",
                (WEEK_START,),
            ).fetchone()[0]
            n_units = conn.execute(
                "SELECT COUNT(*) FROM units WHERE week_start = ?",
                (WEEK_START,),
            ).fetchone()[0]
        assert n_nodes > 0
        assert n_units > 0

    def test_iterates_every_distinct_week(self, env: dict) -> None:
        # Inject a second synthetic week_start into graph_nodes so the
        # rebuild has more than one to iterate over.
        extra_week = "2025-01-13"
        with sqlite3.connect(str(env["github_db"])) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (extra_week, "synthetic-extra", "issue", "fake/repo#1", "2025-01-13T00:00:00Z"),
            )
            conn.commit()

        summary = rebuild.rebuild_history(
            env["github_db"],
            env["sessions_db"],
            env["expectations_db"],
            abandonment_days=14,
            outlier_sigma=2.0,
        )
        assert WEEK_START in summary["weeks"]
        assert extra_week in summary["weeks"]


# ---------------------------------------------------------------------------
# Scenario 2 — partial-failure restore
# ---------------------------------------------------------------------------


class TestScenario2RestoreOnFailure:
    def test_restore_from_backup_on_pipeline_failure(
        self, env: dict, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Capture pre-rebuild content of every dropped table
        pre_state = {
            t: _hash_table(env["github_db"], t)
            for t in ("graph_nodes", "graph_edges", "units", "unit_summaries")
        }
        # Pre-rebuild graph_nodes must be non-empty so a successful
        # restore is observable (a wiped table also "matches" an empty
        # pre-state by accident).
        with sqlite3.connect(str(env["github_db"])) as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM graph_nodes"
            ).fetchone()[0] > 0

        # Force build_graph to blow up after the drop has already run
        def boom(*_args, **_kwargs):
            raise RuntimeError("simulated pipeline failure")

        monkeypatch.setattr(rebuild, "build_graph", boom)

        with pytest.raises(RuntimeError, match="simulated pipeline failure"):
            rebuild.rebuild_history(
                env["github_db"],
                env["sessions_db"],
                env["expectations_db"],
                abandonment_days=14,
                outlier_sigma=2.0,
            )

        # All four dropped tables exist and contain their pre-rebuild
        # content. SQLite's backup API is page-by-page so raw byte
        # comparison can drift even on identical content; compare by
        # row contents instead.
        post_state = {
            t: _hash_table(env["github_db"], t)
            for t in ("graph_nodes", "graph_edges", "units", "unit_summaries")
        }
        assert post_state == pre_state, (
            "github.db tables were not restored from backup after failure"
        )


# ---------------------------------------------------------------------------
# Scenario 3 — destroyed corrections count is surfaced loudly
# ---------------------------------------------------------------------------


class TestScenario3DestroyedCorrectionsLoud:
    def test_warning_logged_with_count(
        self, env: dict, caplog: pytest.LogCaptureFixture
    ) -> None:
        _seed_correction(env["expectations_db"])
        caplog.set_level(logging.WARNING, logger="synthesis.rebuild")

        rc = rebuild.main(["--config", str(env["cfg"])])
        assert rc == 0

        # The warning must mention the row count and the table name and
        # appear BEFORE the drop (we cannot enforce order via caplog
        # easily, but its presence is the load-bearing assertion).
        warned = [
            r for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any(
            "expectation_corrections" in r.getMessage()
            and "1" in r.getMessage()
            for r in warned
        ), (
            "Expected a WARNING naming `expectation_corrections` and the "
            f"row count (1). Got: {[r.getMessage() for r in warned]}"
        )

    def test_no_corrections_logs_info_not_warning(
        self, env: dict, caplog: pytest.LogCaptureFixture
    ) -> None:
        # No expectation_corrections rows -> only INFO, no WARNING.
        caplog.set_level(logging.INFO, logger="synthesis.rebuild")
        rc = rebuild.main(["--config", str(env["cfg"])])
        assert rc == 0
        warned = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and "expectation_corrections" in r.getMessage()
        ]
        assert warned == []


# ---------------------------------------------------------------------------
# Scenario 4 — idempotency
# ---------------------------------------------------------------------------


class TestScenario4Idempotent:
    def test_second_run_produces_identical_units_table(self, env: dict) -> None:
        rc1 = rebuild.main(["--config", str(env["cfg"])])
        assert rc1 == 0
        h1_units = _hash_table(env["github_db"], "units")
        h1_nodes = _hash_table(env["github_db"], "graph_nodes")

        rc2 = rebuild.main(["--config", str(env["cfg"])])
        assert rc2 == 0
        h2_units = _hash_table(env["github_db"], "units")
        h2_nodes = _hash_table(env["github_db"], "graph_nodes")

        assert h1_units == h2_units, "units table contents diverged on re-run"
        assert h1_nodes == h2_nodes, "graph_nodes contents diverged on re-run"


# ---------------------------------------------------------------------------
# Scenario 6 — schema migration applies
# ---------------------------------------------------------------------------


class TestScenario6SchemaMigration:
    def test_traversal_column_exists_after_rebuild(self, env: dict) -> None:
        rc = rebuild.main(["--config", str(env["cfg"])])
        assert rc == 0
        with sqlite3.connect(str(env["github_db"])) as conn:
            cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(graph_edges)").fetchall()
            }
        assert "traversal" in cols, (
            "Slice 1 traversal column missing on graph_edges after rebuild"
        )


# ---------------------------------------------------------------------------
# CLI plumbing sanity
# ---------------------------------------------------------------------------


class TestCli:
    def test_parser_accepts_optional_week(self) -> None:
        parser = rebuild._build_parser()
        args = parser.parse_args(["--week", "2025-01-06", "--week", "2025-01-13"])
        assert args.week == ["2025-01-06", "2025-01-13"]

    def test_parser_accepts_no_args(self) -> None:
        parser = rebuild._build_parser()
        args = parser.parse_args([])
        assert args.week is None
        assert args.config is None

    def test_explicit_week_filter_only_rebuilds_listed(self, env: dict) -> None:
        # Inject an extra week, then ask the CLI to rebuild only the
        # original week — the extra week's nodes should disappear (the
        # drop happened) but no rebuild ran for it.
        extra_week = "2025-01-13"
        with sqlite3.connect(str(env["github_db"])) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (extra_week, "synthetic-extra", "issue", "fake/repo#1", "2025-01-13T00:00:00Z"),
            )
            conn.commit()

        rc = rebuild.main(
            ["--config", str(env["cfg"]), "--week", WEEK_START]
        )
        assert rc == 0

        with sqlite3.connect(str(env["github_db"])) as conn:
            n_extra = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes WHERE week_start = ?",
                (extra_week,),
            ).fetchone()[0]
            n_main = conn.execute(
                "SELECT COUNT(*) FROM graph_nodes WHERE week_start = ?",
                (WEEK_START,),
            ).fetchone()[0]
        assert n_extra == 0, "extra week was rebuilt despite --week filter"
        assert n_main > 0, "filtered week was not rebuilt"
