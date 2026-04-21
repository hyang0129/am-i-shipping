"""Integration tests: X-1 through X-5 pipeline on real session/GitHub data.

Gated by AMIS_INTEGRATION=1. Uses real data from data/ but writes only to
tmp_path copies — live DBs are never modified.

    AMIS_INTEGRATION=1 pytest tests/test_integration_real_data.py -v

Three units with the highest reprompt counts from week 2026-04-12 are used
so X-3 revision detection has meaningful signal to process.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import init_expectations_db
from synthesis.expectations import run_extraction
from synthesis.weekly import run_synthesis

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
WEEK_START = "2026-04-12"

# Two session units that have both unit_summaries rows (required by run_synthesis)
# and reprompts (00455a1cc5fe59f1 has 7 — gives X-3 revision detection real signal).
TARGET_UNITS = (
    "00455a1cc5fe59f1",
    "00187167a76ccf0c",
)

pytestmark = pytest.mark.skipif(
    os.environ.get("AMIS_INTEGRATION") != "1",
    reason="Set AMIS_INTEGRATION=1 to run integration tests",
)


@pytest.fixture(autouse=True)
def _no_live_llm(monkeypatch):
    """Force FakeAnthropicClient unless AMIS_FORCE_LIVE=1 is set."""
    if os.environ.get("AMIS_FORCE_LIVE") != "1":
        monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


@pytest.fixture()
def dbs(tmp_path: Path) -> dict[str, Path]:
    """Copy live DBs into tmp_path and prune to TARGET_UNITS only."""
    github_dst = tmp_path / "github.db"
    sessions_dst = tmp_path / "sessions.db"
    expectations_dst = tmp_path / "expectations.db"

    shutil.copy2(DATA_DIR / "github.db", github_dst)
    shutil.copy2(DATA_DIR / "sessions.db", sessions_dst)
    init_expectations_db(expectations_dst)

    placeholders = ",".join("?" * len(TARGET_UNITS))
    with sqlite3.connect(str(github_dst)) as con:
        con.execute(
            f"DELETE FROM units WHERE unit_id NOT IN ({placeholders})",
            TARGET_UNITS,
        )
        con.commit()

    return {
        "github_db": github_dst,
        "sessions_db": sessions_dst,
        "expectations_db": expectations_dst,
    }


def _make_config(tmp_path: Path) -> SynthesisConfig:
    return SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-6",
        output_dir=str(tmp_path / "retros"),
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )


def _row_count(db: Path, table: str, week: str = WEEK_START) -> int:
    with sqlite3.connect(str(db)) as con:
        return con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE week_start = ?", (week,)
        ).fetchone()[0]


class TestExpectationPipelineRealData:
    def test_x1_extraction_populates_expectations(self, dbs, tmp_path):
        """X-1 writes at least one expectations row for the three real units."""
        cfg = _make_config(tmp_path)
        rc = run_extraction(
            cfg,
            github_db=str(dbs["github_db"]),
            sessions_db=str(dbs["sessions_db"]),
            expectations_db=str(dbs["expectations_db"]),
            week_start=WEEK_START,
        )
        assert rc == 0
        assert _row_count(dbs["expectations_db"], "expectations") > 0

    def test_x1_is_idempotent(self, dbs, tmp_path):
        """Running X-1 twice on the same week does not duplicate rows."""
        cfg = _make_config(tmp_path)
        kwargs = dict(
            github_db=str(dbs["github_db"]),
            sessions_db=str(dbs["sessions_db"]),
            expectations_db=str(dbs["expectations_db"]),
            week_start=WEEK_START,
        )
        run_extraction(cfg, **kwargs)
        count_first = _row_count(dbs["expectations_db"], "expectations")

        run_extraction(cfg, **kwargs)
        count_second = _row_count(dbs["expectations_db"], "expectations")

        assert count_first == count_second

    def test_full_pipeline_x1_through_x5(self, dbs, tmp_path):
        """Full X-1→X-5 pipeline completes without error and all tables exist."""
        cfg = _make_config(tmp_path)

        # X-1
        rc = run_extraction(
            cfg,
            github_db=str(dbs["github_db"]),
            sessions_db=str(dbs["sessions_db"]),
            expectations_db=str(dbs["expectations_db"]),
            week_start=WEEK_START,
        )
        assert rc == 0

        # X-2 through X-5 via run_synthesis; dry_run skips LLM retrospective write
        run_synthesis(
            cfg,
            github_db=dbs["github_db"],
            sessions_db=dbs["sessions_db"],
            week_start=WEEK_START,
            dry_run=True,
            expectations_db=dbs["expectations_db"],
        )

        edb = dbs["expectations_db"]
        assert _row_count(edb, "expectations") > 0, "X-1 table empty"
        # X-2, X-3 rows depend on gap/revision signal; just assert tables are queryable
        with sqlite3.connect(str(edb)) as con:
            for table in (
                "expectation_gaps",
                "expectation_revisions",
                "expectation_corrections",
                "expectation_calibration_trends",
            ):
                con.execute(f"SELECT 1 FROM {table} LIMIT 1")
