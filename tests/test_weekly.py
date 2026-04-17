"""Tests for ``synthesis/weekly.py`` (Epic #17 — Issue #39)."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.weekly import (
    TRANSCRIPT_BUDGET_BYTES,
    _format_unit_block,
    _load_units,
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


# ---------------------------------------------------------------------------
# Health-write isolation (F-2 in the PR-48 review-fix cycle)
# ---------------------------------------------------------------------------
# Issue #40 added ``write_health("synthesis", len(units))`` inside
# ``run_synthesis``. ``write_health`` with no ``data_dir`` defaults to the
# real repo's ``data/`` directory, which means every invocation of
# ``run_synthesis`` from this test module was stomping the developer's
# production ``data/health.json``. The fixture below redirects the call
# site so the default-path health writer writes into a tmp_path-scoped
# data dir and never touches the repo.

@pytest.fixture(autouse=True)
def _isolate_write_health(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Redirect ``synthesis.weekly.write_health``'s default data_dir to tmp_path.

    Yields the redirected health-data directory. Tests that want to assert
    "a synthesis entry landed in health.json" can read from
    ``tmp_path / "data" / "health.json"`` — or request the ``health_data_dir``
    fixture.
    """
    import synthesis.weekly as weekly_module

    health_data_dir = tmp_path / "health_data"
    real_write_health = weekly_module.write_health

    def _redirecting_write_health(collector_name, record_count, data_dir=None):
        target = data_dir if data_dir is not None else health_data_dir
        return real_write_health(collector_name, record_count, data_dir=target)

    monkeypatch.setattr(
        weekly_module, "write_health", _redirecting_write_health
    )
    yield health_data_dir


@pytest.fixture
def health_data_dir(_isolate_write_health) -> Path:
    """Convenience alias for the redirected health-data directory."""
    return _isolate_write_health


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


# ---------------------------------------------------------------------------
# Health-write integration (issue #40 / F-3 in PR-48 review-fix cycle)
# ---------------------------------------------------------------------------
# These tests lock down the invariant that the PR claims to establish:
# ``run_synthesis`` must call ``write_health("synthesis", ...)`` on the
# successful-write path, and must NOT call it on the refuse-to-overwrite
# path. Without these, a future refactor could silently delete the
# ``write_health`` call and the health-check goes-red-after-one-week
# invariant regresses without CI signal.


class TestRunSynthesisHealthWiring:
    def test_successful_run_writes_synthesis_health_entry(
        self, tmp_path: Path, health_data_dir: Path
    ):
        """Happy path: the fake-client run lands a synthesis entry in health.json."""
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START)
        assert result is not None, "precondition: the happy-path write succeeded"

        health_path = health_data_dir / "health.json"
        assert health_path.exists(), (
            "run_synthesis did not call write_health on the success path"
        )

        data = json.loads(health_path.read_text(encoding="utf-8"))
        assert "synthesis" in data
        entry = data["synthesis"]
        assert "last_success" in entry
        assert "last_record_count" in entry
        # last_record_count is len(units); golden fixture has ≥ 1 unit
        # for WEEK_START, so we just assert it is a non-negative int.
        assert isinstance(entry["last_record_count"], int)
        assert entry["last_record_count"] >= 0
        # last_success is a recent ISO timestamp.
        ts = datetime.fromisoformat(entry["last_success"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        assert (datetime.now(timezone.utc) - ts).total_seconds() < 60, (
            f"last_success timestamp is not recent: {entry['last_success']!r}"
        )

    def test_refuse_to_overwrite_does_not_bump_health(
        self, tmp_path: Path, health_data_dir: Path
    ):
        """Decision-2 (refuse-to-overwrite) returns None → health NOT bumped.

        The weekly.py comment at the write_health call site is explicit:
        "that case is idempotent success, but it's not a new data point
        so we don't bump the health timestamp". This test nails that
        invariant down with a negative assertion.
        """
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        # First run writes the retrospective AND the health entry.
        first = run_synthesis(cfg, db, db, WEEK_START)
        assert first is not None

        health_path = health_data_dir / "health.json"
        first_ts = json.loads(health_path.read_text(encoding="utf-8"))[
            "synthesis"
        ]["last_success"]

        # Second run hits refuse-to-overwrite and returns None.
        second = run_synthesis(cfg, db, db, WEEK_START)
        assert second is None, "precondition: refuse-to-overwrite path taken"

        # The health timestamp MUST NOT have advanced.
        second_ts = json.loads(health_path.read_text(encoding="utf-8"))[
            "synthesis"
        ]["last_success"]
        assert first_ts == second_ts, (
            "refuse-to-overwrite run bumped the health timestamp; it must not"
        )

    def test_no_units_for_week_does_not_write_health(
        self, tmp_path: Path, health_data_dir: Path
    ):
        """Empty-week run returns None → no health entry created.

        An empty week is not a successful synthesis — the LLM was never
        called, no retrospective was written. It MUST NOT register as a
        fresh health datapoint, otherwise stale-synthesis detection
        would be masked by empty-week runs.
        """
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        # 1999-01-04 is a Monday with no units in the golden fixture.
        result = run_synthesis(cfg, db, db, "1999-01-04")
        assert result is None, "precondition: no-units path taken"

        health_path = health_data_dir / "health.json"
        # Either the file doesn't exist, or it exists without a synthesis
        # entry. Both are acceptable — what must NOT happen is a synthesis
        # entry appearing from an empty-week run.
        if health_path.exists():
            data = json.loads(health_path.read_text(encoding="utf-8"))
            assert "synthesis" not in data, (
                "empty-week run wrote a synthesis health entry; it must not"
            )

    def test_dry_run_does_not_write_health(
        self, tmp_path: Path, health_data_dir: Path
    ):
        """--dry-run emits a prompt artefact and MUST NOT write health."""
        db = _fixture_copy(tmp_path)
        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(cfg, db, db, WEEK_START, dry_run=True)
        assert result is not None
        assert result.name.endswith(".prompt.txt")

        health_path = health_data_dir / "health.json"
        if health_path.exists():
            data = json.loads(health_path.read_text(encoding="utf-8"))
            assert "synthesis" not in data, (
                "dry-run wrote a synthesis health entry; it must not"
            )


# ---------------------------------------------------------------------------
# Regression — Issue #51: root_node line must not double the namespace
# ---------------------------------------------------------------------------
# The golden fixture built by ``build_golden.py`` uses short fake node
# IDs (e.g. ``n-u1-issue``) that would never surface this bug. Production
# node IDs are already namespaced — ``session:<uuid>``, ``issue:<repo>#N``,
# ``pr:<repo>#N``, ``commit:<sha>`` — so a naive ``{root_node_type}:{root_node_id}``
# render emits ``session:session:<uuid>``. This test seeds a handcrafted
# DB (pattern borrowed from ``test_cross_unit.py``) with real-shape node
# IDs and asserts the rendered block never contains the doubled prefix.
# Do NOT extend ``build_golden.py`` to cover this — that would couple the
# bug's regression test to the expensive golden-snapshot pipeline.


def test_format_unit_block_no_double_namespace(tmp_path: Path) -> None:
    """``_format_unit_block`` must not emit ``<type>:<type>:<id>``.

    Regression for issue #51. The formatter previously prefixed the
    already-namespaced ``root_node_id`` with ``root_node_type:``, so a
    unit rooted at ``session:<uuid>`` came out as ``session:session:<uuid>``
    in the prompt. This test asserts the double-namespace pattern is
    absent for every real-shape root node type (session / issue / pr /
    commit) via a handcrafted inline DB — no reliance on the committed
    ``golden.sqlite`` or ``expected_retrospective.md`` fixtures.
    """
    import re

    from am_i_shipping.db import init_github_db

    week_start = "2025-04-07"
    # Production-shape, already-namespaced node IDs — each one would
    # collide with its ``root_node_type`` if the formatter re-prefixed.
    cases = [
        ("unit-session", "session", "session:3f2a9e14-1b6d-4a0e-9a2c-1234567890ab"),
        ("unit-issue",   "issue",   "issue:example/repo#201"),
        ("unit-pr",      "pr",      "pr:example/repo#42"),
        ("unit-commit",  "commit",  "commit:deadbeefcafef00d1234567890abcdef01234567"),
    ]

    db_path = tmp_path / "github.db"
    init_github_db(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        for unit_id, node_type, node_id in cases:
            conn.execute(
                "INSERT INTO units "
                "(week_start, unit_id, root_node_type, root_node_id, "
                " elapsed_days, dark_time_pct, total_reprompts, "
                " review_cycles, status, outlier_flags, abandonment_flag) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    week_start, unit_id, node_type, node_id,
                    1.0, 0.0, 0, 0, "closed", "[]", 0,
                ),
            )
        conn.commit()
        units = _load_units(conn, week_start)
    finally:
        conn.close()

    assert len(units) == len(cases), (
        f"expected {len(cases)} units, got {len(units)}"
    )

    double_ns = re.compile(
        r"(?:session|issue|pr|commit):(?:session|issue|pr|commit):"
    )

    for unit in units:
        block = _format_unit_block(unit, transcript="")
        assert double_ns.search(block) is None, (
            f"double-namespace pattern leaked into block for unit "
            f"{unit['unit_id']!r} (root_node_id={unit['root_node_id']!r}, "
            f"root_node_type={unit['root_node_type']!r}):\n{block}"
        )
        # And the raw root_node_id must still appear verbatim — the fix
        # drops the prefix, it does not drop the identifier itself.
        assert unit["root_node_id"] in block, (
            f"root_node_id {unit['root_node_id']!r} missing from block:\n{block}"
        )


class TestWaterFillBudgetConstant:
    def test_budget_is_512kb(self):
        # Pin the constant so a future refactor cannot silently shrink
        # or grow the budget without a test diff. The number comes from
        # Epic #17 ADR Decision 4 ("cumulative transcript budget of
        # 512 KB per synthesis run"); changing it requires an ADR
        # update, not just a code change.
        assert TRANSCRIPT_BUDGET_BYTES == 524288, (
            "TRANSCRIPT_BUDGET_BYTES is pinned by Epic #17 ADR Decision 4 "
            "(512 KB = 524288 bytes). Update the ADR before changing it."
        )
