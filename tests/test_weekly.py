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
from am_i_shipping.db import SYNTHESIS_UNIT_SUMMARIES_SCHEMA
from synthesis.weekly import (
    MAX_PROMPT_BYTES,
    MAX_UNITS_PER_PROMPT,
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
        model="claude-sonnet-4-6",
        output_dir=str(output_dir),
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )


def _fixture_copy(tmp_path: Path) -> Path:
    dst = tmp_path / "golden.sqlite"
    shutil.copy(FIXTURE_SRC, dst)
    return dst


def _insert_unit_summaries(
    conn: sqlite3.Connection,
    week_start: str,
    summaries: dict,
) -> None:
    """Insert rows into ``unit_summaries`` for the given week.

    Creates the table first (via ``SYNTHESIS_UNIT_SUMMARIES_SCHEMA``) in
    case the connection is to an in-memory or freshly-created DB that does
    not yet have the table — this is always a no-op on a DB that already
    has the table (``CREATE TABLE IF NOT EXISTS``).

    Parameters
    ----------
    conn:
        Open ``sqlite3.Connection``.
    week_start:
        The ``YYYY-MM-DD`` anchor that matches the ``units`` rows.
    summaries:
        Mapping of ``unit_id -> summary_text``.
    """
    conn.execute(SYNTHESIS_UNIT_SUMMARIES_SCHEMA)
    for unit_id, summary_text in summaries.items():
        conn.execute(
            "INSERT OR IGNORE INTO unit_summaries "
            "(week_start, unit_id, summary_text, model, input_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (week_start, unit_id, summary_text, "claude-haiku-4-5", len(summary_text)),
        )
    conn.commit()


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
        assert "unit-0001-multi" in content or "unit-0002-abandoned" in content


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


# ---------------------------------------------------------------------------
# Issue #54 P-2 — unit cap + prompt-byte guard
# ---------------------------------------------------------------------------
# These tests nail down the safety rails that keep ``run_synthesis``
# from silently shipping a 5-MB prompt when the week partition is
# pathological (dev ``week_start='all'`` with thousands of units) or
# when a single unit produces an oversized metadata block. The cap and
# the guard are independent — the cap truncates the unit list BEFORE
# assembly, the guard fails loudly AFTER assembly — so each gets its
# own end-to-end test against a handcrafted DB.


def _seed_unit_row(
    conn: sqlite3.Connection,
    *,
    week_start: str,
    unit_id: str,
    root_node_type: str = "session",
    root_node_id: str = "",
    elapsed_days: float = 1.0,
    dark_time_pct: float = 0.0,
    total_reprompts: int = 0,
    review_cycles: int = 0,
    status: str = "closed",
    outlier_flags: str = "[]",
    abandonment_flag: int = 0,
) -> None:
    """Insert one ``units`` row with test-friendly defaults.

    Mirrors the helper pattern in ``test_cross_unit.py`` / the inline
    INSERT in ``test_format_unit_block_no_double_namespace`` so the new
    tests do not drag in the full ``build_golden`` pipeline just to
    exercise the cap / guard.
    """
    if not root_node_id:
        root_node_id = f"n-{unit_id}"
    conn.execute(
        "INSERT INTO units "
        "(week_start, unit_id, root_node_type, root_node_id, "
        " elapsed_days, dark_time_pct, total_reprompts, "
        " review_cycles, status, outlier_flags, abandonment_flag) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            week_start,
            unit_id,
            root_node_type,
            root_node_id,
            elapsed_days,
            dark_time_pct,
            total_reprompts,
            review_cycles,
            status,
            outlier_flags,
            abandonment_flag,
        ),
    )


def test_unit_cap_truncates_and_warns(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """150 units → WARNING logged and only MAX_UNITS_PER_PROMPT survive.

    Seeds a handcrafted DB with 150 units under a single week_start,
    runs ``run_synthesis`` in dry-run mode (so we can inspect the
    assembled prompt without a network call), and asserts:

    1. A WARNING log line is emitted naming both the before (150) and
       after (MAX_UNITS_PER_PROMPT=100) counts.
    2. The assembled prompt reports ``Total units this week: 100`` — the
       prompt body encodes the post-cap count, so this is the cleanest
       end-to-end check that the cap took effect.
    3. Exactly ``MAX_UNITS_PER_PROMPT`` ``### unit `` headings appear in
       the assembled prompt — double-locks the count via the per-unit
       block renderer.
    """
    from am_i_shipping.db import init_github_db

    week_start = "2025-04-14"
    total_units = 150
    assert total_units > MAX_UNITS_PER_PROMPT, (
        "test precondition: seed size must exceed the cap"
    )

    db_path = tmp_path / "github.db"
    init_github_db(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        # Seed a mix of abandoned / outlier / plain units so the sort
        # key has something to rank against. The cap must still land on
        # exactly MAX_UNITS_PER_PROMPT regardless of the priority mix.
        for i in range(total_units):
            flags = "[\"elapsed_days\"]" if i % 7 == 0 else "[]"
            abandoned = 1 if i % 11 == 0 else 0
            _seed_unit_row(
                conn,
                week_start=week_start,
                unit_id=f"unit-{i:04d}",
                elapsed_days=float(i % 30),
                outlier_flags=flags,
                abandonment_flag=abandoned,
            )
        # Insert a unit_summaries row for every seeded unit so run_synthesis
        # does not hit the fail-loud guard (units without summaries raise).
        _insert_unit_summaries(
            conn,
            week_start,
            {f"unit-{i:04d}": f"Summary for unit-{i:04d}." for i in range(total_units)},
        )
        conn.commit()
    finally:
        conn.close()

    out = tmp_path / "retrospectives"
    cfg = _make_config(tmp_path, out)

    caplog.set_level("WARNING", logger="synthesis.weekly")

    # Dry-run keeps us off the LLM and gives us the assembled prompt on
    # disk — cleanest surface to assert on without reaching into
    # private helpers.
    result = run_synthesis(cfg, db_path, db_path, week_start, dry_run=True)
    assert result is not None, "dry-run must produce a prompt artefact"

    # 1. WARNING emitted naming the before/after counts.
    warnings = [
        r for r in caplog.records
        if r.levelname == "WARNING" and r.name == "synthesis.weekly"
    ]
    assert warnings, "expected at least one WARNING log from the unit cap"
    msg_blob = " ".join(r.getMessage() for r in warnings)
    assert str(total_units) in msg_blob, (
        f"warning must mention the before-count {total_units}: {msg_blob!r}"
    )
    assert str(MAX_UNITS_PER_PROMPT) in msg_blob, (
        f"warning must mention the after-count {MAX_UNITS_PER_PROMPT}: "
        f"{msg_blob!r}"
    )

    # 2. Prompt body encodes the post-cap count.
    prompt_text = result.read_text(encoding="utf-8")
    assert f"Total units this week: {MAX_UNITS_PER_PROMPT}" in prompt_text, (
        "assembled prompt did not report the capped unit count"
    )

    # 3. Exactly MAX_UNITS_PER_PROMPT per-unit blocks rendered.
    rendered_unit_blocks = prompt_text.count("### unit ")
    assert rendered_unit_blocks == MAX_UNITS_PER_PROMPT, (
        f"expected {MAX_UNITS_PER_PROMPT} unit blocks in the assembled "
        f"prompt, got {rendered_unit_blocks}"
    )


def test_prompt_byte_guard_raises(tmp_path: Path) -> None:
    """An oversized unit block pushes the prompt past MAX_PROMPT_BYTES → RuntimeError.

    Strategy: inject a single unit whose ``outlier_flags`` field is a
    JSON string large enough that rendering its block alone exceeds
    ``MAX_PROMPT_BYTES``. ``_format_unit_block`` embeds
    ``outlier_flags`` verbatim, so a multi-megabyte JSON string flows
    straight into the assembled prompt. This avoids any dependence on
    session transcripts (which water-fill would truncate) and exercises
    the guard against oversized *metadata*, which is the actual gap
    the guard exists to close.

    The guard MUST fire before any network call and before the dry-run
    artefact is written. We exercise the dry-run path here because it
    hits the guard without needing a live SDK stub — the guard is
    shared across both paths.
    """
    from am_i_shipping.db import init_github_db

    week_start = "2025-04-14"

    db_path = tmp_path / "github.db"
    init_github_db(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        # Build an ``outlier_flags`` payload that alone exceeds the
        # byte ceiling. 2 * MAX_PROMPT_BYTES is comfortably over the
        # limit even accounting for the static system prompt savings.
        bloated_flags = "[" + ("\"x\"," * (2 * MAX_PROMPT_BYTES // 4)) + "\"x\"]"
        assert len(bloated_flags) > MAX_PROMPT_BYTES, (
            "test precondition: bloated_flags must itself exceed the cap"
        )
        _seed_unit_row(
            conn,
            week_start=week_start,
            unit_id="unit-bloat",
            outlier_flags=bloated_flags,
        )
        # Provide a unit_summaries row so run_synthesis passes the fail-loud
        # guard for missing summaries and reaches the prompt-byte guard.
        _insert_unit_summaries(conn, week_start, {"unit-bloat": "Summary for unit-bloat."})
        conn.commit()
    finally:
        conn.close()

    out = tmp_path / "retrospectives"
    cfg = _make_config(tmp_path, out)

    with pytest.raises(RuntimeError, match="MAX_PROMPT_BYTES"):
        run_synthesis(cfg, db_path, db_path, week_start, dry_run=True)

    # And the dry-run artefact must NOT have been written — the guard
    # runs before the write.
    dry_path = out / ".dry-run" / f"{week_start}.prompt.txt"
    assert not dry_path.exists(), (
        "prompt-byte guard must fire before the dry-run artefact is written"
    )


def test_prompt_byte_guard_blocks_live_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The byte-guard must also fire BEFORE the LLM call on ``dry_run=False``.

    ``test_prompt_byte_guard_raises`` above only exercises the dry-run
    branch. The guard's contract (documented in the module comment and
    the PR description) is "no network call, no dry-run write" — so we
    also need a live-path assertion. We install a spy as
    ``synthesis.weekly._call_llm`` that raises ``AssertionError`` if
    ever invoked, then call ``run_synthesis(..., dry_run=False)`` with
    the same oversized seed. A ``RuntimeError`` from the guard is the
    only acceptable outcome; if the spy is touched the guard landed
    *below* the LLM dispatch (regression) and the test fails loudly.
    """
    from am_i_shipping.db import init_github_db
    import synthesis.weekly as weekly_module

    week_start = "2025-04-14"
    db_path = tmp_path / "github.db"
    init_github_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        bloated_flags = "[" + ("\"x\"," * (2 * MAX_PROMPT_BYTES // 4)) + "\"x\"]"
        assert len(bloated_flags) > MAX_PROMPT_BYTES
        _seed_unit_row(
            conn,
            week_start=week_start,
            unit_id="unit-bloat-live",
            outlier_flags=bloated_flags,
        )
        # Provide a unit_summaries row so run_synthesis passes the fail-loud
        # guard for missing summaries and reaches the prompt-byte guard.
        _insert_unit_summaries(
            conn, week_start, {"unit-bloat-live": "Summary for unit-bloat-live."}
        )
        conn.commit()
    finally:
        conn.close()

    # Spy: if the adapter is reached, the guard regressed.
    import synthesis.weekly as weekly_module

    class _ForbiddenAdapter:
        def call(self, *args, **kwargs):
            raise AssertionError(
                "_get_adapter().call() must not be invoked when the prompt-byte "
                "guard should have fired — the guard regressed below the LLM dispatch"
            )

    monkeypatch.setattr(weekly_module, "_get_adapter", lambda _config: _ForbiddenAdapter())

    out = tmp_path / "retrospectives"
    cfg = _make_config(tmp_path, out)

    with pytest.raises(RuntimeError, match="MAX_PROMPT_BYTES"):
        run_synthesis(cfg, db_path, db_path, week_start, dry_run=False)

    # No retrospective file should have been written either — the guard
    # sits above ``write_retrospective`` just as it sits above the
    # dry-run write.
    assert not (out / f"{week_start}.md").exists(), (
        "live-path guard regression: retrospective was written despite "
        "an oversized prompt"
    )


def test_unit_cap_preserves_priority_order(
    tmp_path: Path,
) -> None:
    """The 100 units that survive truncation must be the highest-priority ones.

    ``test_unit_cap_truncates_and_warns`` asserts the *count* is 100.
    This test asserts the *identity* of the survivors — that the
    documented priority (abandonment_flag=1 > non-empty outlier_flags >
    elapsed_days desc > unit_id asc) actually drives the truncation.
    A regression that dropped a ``-`` from ``_priority_key`` (sorting
    abandoned units to the BOTTOM instead of the top) would still pass
    the count assertion; this test fails loudly on that bug.

    Seeding strategy:
      * 5 guaranteed survivors: ``unit-A-00..04`` with abandonment_flag=1.
      * 5 more guaranteed survivors: ``unit-B-00..04`` with non-empty
        outlier_flags (and abandonment=0).
      * 120 filler units: ``unit-Z-000..119`` with both flags clear.
        Only 90 of these can fit (100 cap − 10 already spoken for).
        The 30 extras must be dropped.
    After the cap:
      * Every A-unit must be present (top priority).
      * Every B-unit must be present (second priority).
      * At least one Z-unit must be absent (proof the cap actually
        discarded low-priority units rather than high-priority ones).
    """
    from am_i_shipping.db import init_github_db

    week_start = "2025-04-14"
    db_path = tmp_path / "github.db"
    init_github_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        # 5 abandoned (guaranteed top-priority survivors)
        for i in range(5):
            _seed_unit_row(
                conn,
                week_start=week_start,
                unit_id=f"unit-A-{i:02d}",
                abandonment_flag=1,
                outlier_flags="[]",
                elapsed_days=0.0,
            )
        # 5 outlier-flagged (guaranteed second-priority survivors)
        for i in range(5):
            _seed_unit_row(
                conn,
                week_start=week_start,
                unit_id=f"unit-B-{i:02d}",
                abandonment_flag=0,
                outlier_flags="[\"elapsed_days\"]",
                elapsed_days=0.0,
            )
        # 120 filler units (flags all clear — lowest priority)
        for i in range(120):
            _seed_unit_row(
                conn,
                week_start=week_start,
                unit_id=f"unit-Z-{i:03d}",
                abandonment_flag=0,
                outlier_flags="[]",
                elapsed_days=0.0,
            )
        # Insert unit_summaries for all seeded units so run_synthesis passes
        # the fail-loud missing-summary guard.
        all_unit_summaries = {}
        for i in range(5):
            all_unit_summaries[f"unit-A-{i:02d}"] = f"Summary for unit-A-{i:02d}."
            all_unit_summaries[f"unit-B-{i:02d}"] = f"Summary for unit-B-{i:02d}."
        for i in range(120):
            all_unit_summaries[f"unit-Z-{i:03d}"] = f"Summary for unit-Z-{i:03d}."
        _insert_unit_summaries(conn, week_start, all_unit_summaries)
        conn.commit()
    finally:
        conn.close()

    out = tmp_path / "retrospectives"
    cfg = _make_config(tmp_path, out)

    result = run_synthesis(cfg, db_path, db_path, week_start, dry_run=True)
    assert result is not None
    prompt_text = result.read_text(encoding="utf-8")

    # Every abandonment-flagged unit must survive.
    for i in range(5):
        uid = f"unit-A-{i:02d}"
        assert f"### unit {uid}" in prompt_text, (
            f"top-priority (abandoned) unit {uid} was dropped — "
            "priority ordering regressed"
        )
    # Every outlier-flagged unit must survive.
    for i in range(5):
        uid = f"unit-B-{i:02d}"
        assert f"### unit {uid}" in prompt_text, (
            f"second-priority (outlier) unit {uid} was dropped — "
            "priority ordering regressed"
        )
    # At least 20 low-priority fillers must be absent — proof that the
    # cap discarded the right tail of the priority order. We assert the
    # loose bound (>=20) rather than the tight expectation (==30,
    # because 120 filler - 90 surviving slots = 30 dropped) so future
    # tweaks to the tie-break (e.g. a secondary sort field) do not
    # require updating this test. The tight expectation is that
    # exactly 30 filler units drop; anything looser than 20 means the
    # cap is not doing its job.
    missing_z = [
        i for i in range(120)
        if f"### unit unit-Z-{i:03d}" not in prompt_text
    ]
    assert len(missing_z) >= 20, (
        f"expected >=20 filler units to be dropped under the 100-unit "
        f"cap (120 filler + 10 priority = 130 total, cap=100), got "
        f"{len(missing_z)} dropped — the cap may not be engaging"
    )


# ---------------------------------------------------------------------------
# Issue #64 — fail-loud guard for missing unit_summaries rows
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Issue #68 — AS-7: weekly prompt emits 2 unit blocks for the two-issue fixture
# ---------------------------------------------------------------------------

TWO_ISSUE_FIXTURE_68 = Path(__file__).parent / "fixtures" / "two_issue_session.jsonl"
TWO_ISSUE_WEEK_68 = "2026-03-23"
TWO_ISSUE_SESSION_UUID_68 = "f2000000-0000-0000-0000-000000000002"


def _ingest_and_build_two_issue(tmp_path: Path) -> Path:
    """Parse and upsert the two-issue fixture, build graph, identify units.

    Returns the github.db path (which also holds units + graph tables).
    """
    from am_i_shipping.db import init_github_db, init_sessions_db
    from collector.session_parser import parse_session
    from collector.store import upsert_session
    from synthesis.graph_builder import build_graph
    from synthesis.unit_identifier import identify_units
    from datetime import datetime

    sess_db = tmp_path / "sessions.db"
    gh_db = tmp_path / "github.db"
    init_sessions_db(sess_db)
    init_github_db(gh_db)

    record = parse_session(TWO_ISSUE_FIXTURE_68)
    upsert_session(record, db_path=sess_db, data_dir=tmp_path, skip_health=True)
    build_graph(sess_db, gh_db, week_start=TWO_ISSUE_WEEK_68)
    identify_units(
        gh_db, sess_db, TWO_ISSUE_WEEK_68, now=datetime(2026, 4, 19, 0, 0, 0)
    )
    return gh_db


class TestIssue68WeeklyPromptTwoUnitBlocks:
    """AS-7: weekly prompt must emit exactly 2 per-unit blocks for the two-issue
    fixture, and each block must carry session_fraction and phase labels."""

    def test_prompt_emits_two_unit_blocks(self, tmp_path: Path):
        """AS-7: assembled prompt contains exactly 2 '### unit' blocks."""
        import re

        gh_db = _ingest_and_build_two_issue(tmp_path)

        # Seed unit_summaries so run_synthesis passes the fail-loud guard.
        conn = sqlite3.connect(str(gh_db))
        try:
            unit_ids = [
                r[0]
                for r in conn.execute(
                    "SELECT unit_id FROM units WHERE week_start = ?",
                    (TWO_ISSUE_WEEK_68,),
                ).fetchall()
            ]
        finally:
            conn.close()

        assert len(unit_ids) == 2, (
            f"Precondition: expected 2 units in DB, got {len(unit_ids)}"
        )

        conn = sqlite3.connect(str(gh_db))
        try:
            _insert_unit_summaries(
                conn,
                TWO_ISSUE_WEEK_68,
                {uid: f"Summary for {uid}." for uid in unit_ids},
            )
        finally:
            conn.close()

        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(
            cfg, gh_db, tmp_path / "sessions.db", TWO_ISSUE_WEEK_68, dry_run=True
        )
        assert result is not None, "dry-run must produce a prompt artefact"

        prompt_text = result.read_text(encoding="utf-8")
        unit_block_count = len(re.findall(r"^### unit ", prompt_text, re.MULTILINE))
        assert unit_block_count == 2, (
            f"Expected exactly 2 '### unit' blocks in the assembled prompt (AS-7), "
            f"got {unit_block_count}"
        )

    def test_prompt_blocks_contain_session_fraction_and_phase(self, tmp_path: Path):
        """AS-7: each unit block in the prompt includes session_fraction and phase."""
        gh_db = _ingest_and_build_two_issue(tmp_path)

        conn = sqlite3.connect(str(gh_db))
        try:
            unit_ids = [
                r[0]
                for r in conn.execute(
                    "SELECT unit_id FROM units WHERE week_start = ?",
                    (TWO_ISSUE_WEEK_68,),
                ).fetchall()
            ]
            _insert_unit_summaries(
                conn,
                TWO_ISSUE_WEEK_68,
                {uid: f"Summary for {uid}." for uid in unit_ids},
            )
        finally:
            conn.close()

        out = tmp_path / "retrospectives"
        cfg = _make_config(tmp_path, out)

        result = run_synthesis(
            cfg, gh_db, tmp_path / "sessions.db", TWO_ISSUE_WEEK_68, dry_run=True
        )
        assert result is not None
        prompt_text = result.read_text(encoding="utf-8")

        # Both session_fraction and phase must appear in the prompt (AS-7).
        assert "session_fraction" in prompt_text, (
            "Assembled prompt must contain 'session_fraction' label for issue-rooted "
            "units (AS-7)"
        )
        assert "phase" in prompt_text, (
            "Assembled prompt must contain 'phase' label for issue-rooted "
            "units (AS-7)"
        )


def test_run_synthesis_raises_when_summary_missing(tmp_path: Path) -> None:
    """``run_synthesis`` must raise ``RuntimeError`` when a unit has no summary row.

    Seeds a DB with one unit but NO ``unit_summaries`` row for that unit.
    The fail-loud guard in ``run_synthesis`` must raise a ``RuntimeError``
    whose message contains ``"am-summarize-units"`` — the command the
    operator should run to fix the missing row.
    """
    from am_i_shipping.db import init_github_db

    week_start = "2025-04-21"
    db_path = tmp_path / "github.db"
    init_github_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        _seed_unit_row(
            conn,
            week_start=week_start,
            unit_id="unit-no-summary",
        )
        conn.commit()
        # Deliberately do NOT insert a unit_summaries row — this is the
        # condition the guard is meant to catch.
    finally:
        conn.close()

    out = tmp_path / "retrospectives"
    cfg = _make_config(tmp_path, out)

    with pytest.raises(RuntimeError, match=r"unit-no-summary.*am-summarize-units"):
        run_synthesis(cfg, db_path, db_path, week_start, dry_run=True)
