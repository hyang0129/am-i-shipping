"""Tests for ``synthesis/weekly.py`` (Epic #17 — Issue #39)."""

from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.weekly import (
    TRANSCRIPT_BUDGET_BYTES,
    run_synthesis,
    water_fill_truncate,
)


FIXTURE_SRC = Path(__file__).parent / "fixtures" / "synthesis" / "golden.sqlite"
EXPECTED_MD = (
    Path(__file__).parent
    / "fixtures"
    / "synthesis"
    / "expected_retrospective.md"
)
WEEK_START = "2025-01-06"


# ---------------------------------------------------------------------------
# Water-fill unit tests (ADR Decision 4)
# ---------------------------------------------------------------------------


class TestWaterFillTruncate:
    """The algorithm is the unit of test, per the epic acceptance list."""

    def test_budget_smaller_than_all_contents_equal_share(self):
        # budget=10, sessions=[8,9] → each fits share=5; both truncate.
        assert water_fill_truncate(["a" * 8, "b" * 9], 10) == [
            "a" * 5,
            "b" * 5,
        ]

    def test_small_session_fits_whole_large_truncated_to_share(self):
        # budget=10, sessions=[5,15] → small fits share=5, large truncates
        # to share=5 (small's savings are exactly zero, all budget goes
        # to equal share on the first pass).
        assert water_fill_truncate(["a" * 5, "b" * 15], 10) == [
            "a" * 5,
            "b" * 5,
        ]

    def test_savings_cascade_to_larger_sessions(self):
        # budget=12, sessions=[5,15] → first pass share=6; small session
        # consumes 5 (remaining budget=7, count=1); large takes share=7.
        assert water_fill_truncate(["a" * 5, "b" * 15], 12) == [
            "a" * 5,
            "b" * 7,
        ]

    def test_preserves_input_order(self):
        # Out-of-order input: large first, small second.
        result = water_fill_truncate(["b" * 15, "a" * 5], 12)
        # Same budget as the savings-cascade case; result aligns with
        # the input positional order.
        assert result == ["b" * 7, "a" * 5]

    def test_empty_contents_returns_empty_list(self):
        assert water_fill_truncate([], 1000) == []

    def test_zero_budget_returns_empty_strings(self):
        assert water_fill_truncate(["abc", "def"], 0) == ["", ""]

    def test_negative_budget_raises(self):
        with pytest.raises(ValueError):
            water_fill_truncate(["a"], -1)

    def test_generous_budget_returns_originals(self):
        contents = ["a" * 5, "b" * 10]
        # Budget is larger than total demand — nobody is truncated.
        assert water_fill_truncate(contents, 1000) == contents

    def test_total_bytes_never_exceed_budget(self):
        # Property-style sanity check on a mixed batch. Two small, two
        # large; equal-share walk should keep the total at or under
        # budget.
        contents = ["a" * 3, "b" * 100, "c" * 50, "d" * 7]
        result = water_fill_truncate(contents, 40)
        assert sum(len(s) for s in result) <= 40

    def test_single_session_gets_full_budget(self):
        assert water_fill_truncate(["a" * 100], 30) == ["a" * 30]


# ---------------------------------------------------------------------------
# run_synthesis happy path + idempotency + dry-run
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path, output_dir: Path) -> SynthesisConfig:
    """Build a SynthesisConfig pinned to *output_dir*."""
    return SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-5",
        output_dir=str(output_dir),
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )


def _fixture_copy(tmp_path: Path) -> Path:
    dst = tmp_path / "golden.sqlite"
    shutil.copy(FIXTURE_SRC, dst)
    return dst


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    """Guarantee offline mode across every test in this module.

    Some environments may have AMIS_SYNTHESIS_LIVE or ANTHROPIC_API_KEY
    exported. Scrub both so no test accidentally tries a real API call.
    """
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


class TestRunSynthesisOffline:
    def test_writes_retrospective_using_fake_client(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START, dry_run=False)

        assert result is not None
        assert result == out / f"{WEEK_START}.md"
        assert result.exists()
        # Sanity: file has content.
        assert result.read_text(encoding="utf-8").startswith("# Weekly Retrospective")

    def test_snapshot_matches_committed_fixture(self, tmp_path: Path):
        """Fake-client output byte-matches the committed snapshot."""
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None

        actual = result.read_text(encoding="utf-8")
        expected = EXPECTED_MD.read_text(encoding="utf-8")
        assert actual == expected

    def test_rendered_markdown_has_required_sections(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)
        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None
        md = result.read_text(encoding="utf-8")
        for heading in (
            "Velocity Trend",
            "Unit Summary Table",
            "Outlier Units",
            "Abandoned Units",
            "Dark Time",
            "Clarifying Questions",
        ):
            assert f"## {heading}" in md, f"missing heading: {heading}"

    def test_at_most_two_clarifying_questions(self, tmp_path: Path):
        import re

        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)
        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None
        md = result.read_text(encoding="utf-8")
        # Grab the ``## Clarifying Questions`` section only.
        _, _, tail = md.partition("## Clarifying Questions")
        # Stop at the next ``## `` heading if any.
        section = tail.split("\n## ", 1)[0]
        numbered = re.findall(r"^\d+\.", section, flags=re.MULTILINE)
        assert len(numbered) <= 2, numbered

    def test_no_recommendations_section(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)
        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None
        md = result.read_text(encoding="utf-8").lower()
        assert "## recommendations" not in md
        assert "# recommendations" not in md


class TestRunSynthesisIdempotency:
    def test_second_run_returns_none(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        first = run_synthesis(cfg, db, db, WEEK_START)
        assert first is not None
        second = run_synthesis(cfg, db, db, WEEK_START)
        assert second is None

    def test_second_run_does_not_overwrite_file(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None
        # Stash the bytes we wrote.
        before = result.read_bytes()
        # Stomp the file with sentinel content, then run again — the
        # second run must refuse to overwrite our sentinel.
        sentinel = b"USER HAND-EDITED NOTE\n"
        result.write_bytes(sentinel)
        _ = run_synthesis(cfg, db, db, WEEK_START)
        assert result.read_bytes() == sentinel, (
            "idempotent run overwrote user-edited file"
        )
        # And to round-trip, check that the first run wrote something
        # other than the sentinel (so the test is meaningful).
        assert before != sentinel


class TestDryRun:
    def test_dry_run_writes_prompt_only(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START, dry_run=True)

        assert result is not None
        assert result.name == f"{WEEK_START}.prompt.txt"
        assert result.parent.name == ".dry-run"
        assert result.exists()
        # Dry-run must NOT produce the final retrospective file.
        assert not (out / f"{WEEK_START}.md").exists()

    def test_dry_run_prompt_contains_system_and_user_halves(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START, dry_run=True)
        assert result is not None
        content = result.read_text(encoding="utf-8")
        assert "=== SYSTEM" in content
        assert "=== USER" in content
        # The per-unit block should render something recognisable from
        # the fixture — at least one of the unit IDs.
        assert "unit-0001-multi" in content or "unit-0003-singleton" in content


class TestNoUnitsForWeek:
    def test_missing_week_returns_none(self, tmp_path: Path):
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)
        # 1999-01-04 is a Monday with no units.
        result = run_synthesis(cfg, db, db, "1999-01-04")
        assert result is None
        # And no retrospective file was written.
        assert not (out / "1999-01-04.md").exists()


class TestWaterFillBudgetConstant:
    def test_budget_is_512kb(self):
        # Pin the constant so a future refactor cannot silently shrink
        # or grow the budget without a test diff.
        assert TRANSCRIPT_BUDGET_BYTES == 524288
