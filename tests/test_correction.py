"""Tests for ``synthesis/correction.py`` (Epic #27 — X-4, Issue #75)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    EXPECTED_EXPECTATIONS_TABLES,
    assert_schema,
    init_expectations_db,
    init_github_db,
)
from synthesis import correction
from synthesis.correction import (
    AUTO_CONFIRM_DAYS,
    FACETS,
    auto_confirm_sweep,
    run_correction_session,
)


WEEK_START = "2026-04-14"


def _make_config(**overrides) -> SynthesisConfig:
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-6",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


def _init_dbs(tmp_path: Path) -> tuple[Path, Path]:
    gh = tmp_path / "github.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_expectations_db(exp)
    return gh, exp


def _seed_expectation(
    exp_db: Path,
    *,
    unit_id: str,
    week_start: str = WEEK_START,
    expected_scope: str = "fix X",
    expected_effort: str = "one session",
    expected_outcome: str = "tests pass",
    commitment_point: str = "turn 3",
) -> None:
    conn = sqlite3.connect(str(exp_db))
    try:
        conn.execute(
            "INSERT INTO expectations "
            "(week_start, unit_id, commitment_point, expected_scope, "
            " expected_effort, expected_outcome, confidence, model, "
            " input_bytes, skip_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                week_start,
                unit_id,
                commitment_point,
                expected_scope,
                expected_effort,
                expected_outcome,
                0.8,
                "claude-sonnet-4-6",
                100,
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_gap(
    exp_db: Path,
    *,
    unit_id: str,
    severity: str = "major",
    direction: str = "over",
    failure_precondition: str = "step_4_plan",
    week_start: str = WEEK_START,
    commitment_point: str = "turn 3",
    computed_at: str | None = None,
) -> None:
    conn = sqlite3.connect(str(exp_db))
    try:
        if computed_at is None:
            conn.execute(
                "INSERT INTO expectation_gaps "
                "(week_start, unit_id, commitment_point, scope_gap, "
                " effort_gap, outcome_gap, severity, direction, "
                " failure_precondition, auto_confirmed) "
                "VALUES (?, ?, ?, 'scope deviated', 'effort deviated', "
                " 'outcome deviated', ?, ?, ?, 0)",
                (week_start, unit_id, commitment_point, severity, direction,
                 failure_precondition),
            )
        else:
            conn.execute(
                "INSERT INTO expectation_gaps "
                "(week_start, unit_id, commitment_point, scope_gap, "
                " effort_gap, outcome_gap, severity, direction, "
                " failure_precondition, computed_at, auto_confirmed) "
                "VALUES (?, ?, ?, 'scope deviated', 'effort deviated', "
                " 'outcome deviated', ?, ?, ?, ?, 0)",
                (week_start, unit_id, commitment_point, severity, direction,
                 failure_precondition, computed_at),
            )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AMIS_SYNTHESIS_OFFLINE", "1")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ---------------------------------------------------------------------------
# AS-1: schema exists
# ---------------------------------------------------------------------------


class TestExpectationCorrectionsSchema:
    def test_init_creates_expectation_corrections_table(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)

        conn = sqlite3.connect(str(db))
        try:
            cols = {
                r[1]
                for r in conn.execute(
                    "PRAGMA table_info(expectation_corrections)"
                ).fetchall()
            }
        finally:
            conn.close()

        required = {
            "week_start",
            "unit_id",
            "facet",
            "original_value",
            "corrected_value",
            "correction_note",
            "corrected_by",
            "corrected_at",
        }
        missing = required - cols
        assert not missing, (
            f"expectation_corrections missing columns: {missing}"
        )

    def test_primary_key_is_week_unit_facet(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        conn = sqlite3.connect(str(db))
        try:
            # Insert twice on the same PK — INSERT OR IGNORE should no-op,
            # and a bare INSERT should raise IntegrityError.
            conn.execute(
                "INSERT INTO expectation_corrections "
                "(week_start, unit_id, facet, original_value, "
                " corrected_value, correction_note, corrected_by) "
                "VALUES ('2026-04-14', 'u1', 'scope', 'a', 'b', '', 'user')"
            )
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO expectation_corrections "
                    "(week_start, unit_id, facet, original_value, "
                    " corrected_value, correction_note, corrected_by) "
                    "VALUES ('2026-04-14', 'u1', 'scope', 'x', 'y', '', 'user')"
                )
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# AS-2: subcommand wired
# ---------------------------------------------------------------------------


class TestCliSubcommand:
    def test_correct_subcommand_help(self, capsys: pytest.CaptureFixture):
        from synthesis.cli import _build_parser

        parser = _build_parser()
        # Help of the correct subcommand should mention --week and --unit.
        with pytest.raises(SystemExit):
            parser.parse_args(["correct", "--help"])
        captured = capsys.readouterr()
        assert "--week" in captured.out
        assert "--unit" in captured.out


# ---------------------------------------------------------------------------
# AS-3, AS-4, AS-5, AS-9: agentic loop with fake adapter
# ---------------------------------------------------------------------------


class _RecordingAdapter:
    """Test adapter that returns pre-scripted JSON responses in order.

    Mirrors :class:`synthesis.llm_adapter._FakeAdapter`'s call surface.
    """

    def __init__(self, responses: List[str]) -> None:
        self._responses = list(responses)
        self.calls: List[dict] = []

    def call(self, system: str, user: str, model: str, max_tokens: int):
        from synthesis.llm_adapter import LLMResult

        self.calls.append(
            {"system": system, "user": user, "model": model}
        )
        if not self._responses:
            # Exhausted — confirm no change.
            text = json.dumps(
                {"action": "confirm", "question": "",
                 "corrected_value": "", "correction_note": ""}
            )
        else:
            text = self._responses.pop(0)
        return LLMResult(text=text, cost_usd=0.0)


def _confirm_json() -> str:
    return json.dumps(
        {"action": "confirm", "question": "",
         "corrected_value": "", "correction_note": ""}
    )


def _correct_json(value: str, note: str = "") -> str:
    return json.dumps(
        {"action": "correct", "question": "",
         "corrected_value": value, "correction_note": note}
    )


class TestCorrectionSession:
    def test_confirm_path_writes_one_row_per_facet(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1")
        _seed_gap(exp, unit_id="u1", severity="major")

        # Every facet confirms.
        adapter = _RecordingAdapter([_confirm_json()] * len(FACETS))
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )

        written = run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        assert written == len(FACETS)

        conn = sqlite3.connect(str(exp))
        try:
            rows = conn.execute(
                "SELECT facet, original_value, corrected_value, corrected_by "
                "FROM expectation_corrections WHERE unit_id='u1' "
                "ORDER BY facet"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == len(FACETS)
        # AS-5: confirm-no-change → original_value == corrected_value, user.
        for facet, original, corrected, by in rows:
            assert by == "user"
            assert original == corrected, (
                f"confirm should preserve value for facet={facet}"
            )

    def test_correct_path_distinct_from_original(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1", expected_scope="fix X")
        _seed_gap(exp, unit_id="u1", severity="major")

        # First facet is commitment_point (confirm), then scope gets
        # corrected, rest confirm.
        responses = [
            _confirm_json(),                        # commitment_point
            _correct_json("fix X and also Y", "user refined scope"),  # scope
        ] + [_confirm_json()] * (len(FACETS) - 2)
        adapter = _RecordingAdapter(responses)
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )

        run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        conn = sqlite3.connect(str(exp))
        try:
            row = conn.execute(
                "SELECT original_value, corrected_value, correction_note "
                "FROM expectation_corrections "
                "WHERE unit_id='u1' AND facet='scope'"
            ).fetchone()
            gap_auto = conn.execute(
                "SELECT auto_confirmed FROM expectation_gaps "
                "WHERE unit_id='u1'"
            ).fetchone()[0]
        finally:
            conn.close()

        # AS-9: original preserved from the expectations row.
        assert row[0] == "fix X"
        assert row[1] == "fix X and also Y"
        assert row[2] == "user refined scope"
        # AS-4: at least one correction → auto_confirmed flipped to 0.
        assert gap_auto == 0

    def test_confirm_only_path_flips_auto_confirmed_to_1(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1")
        _seed_gap(exp, unit_id="u1", severity="critical")

        adapter = _RecordingAdapter([_confirm_json()] * len(FACETS))
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )
        run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        conn = sqlite3.connect(str(exp))
        try:
            auto = conn.execute(
                "SELECT auto_confirmed FROM expectation_gaps "
                "WHERE unit_id='u1'"
            ).fetchone()[0]
        finally:
            conn.close()
        # Every facet confirmed with no value change → 1 (resolved).
        assert auto == 1

    def test_gap_row_not_mutated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """AS-9 — expectation_gaps row is NOT mutated by corrections."""
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1")
        _seed_gap(exp, unit_id="u1", severity="major",
                  failure_precondition="step_4_plan")

        responses = (
            [_confirm_json()]
            + [_correct_json("step_1_intent", "actually it was intent")]
            + [_confirm_json()] * (len(FACETS) - 2)
        )
        # Put the correct_json for failure_precondition. FACETS order is
        # commitment_point, scope, effort, outcome, severity,
        # failure_precondition — so place it at index 5.
        responses = [_confirm_json()] * len(FACETS)
        responses[5] = _correct_json("step_1_intent", "root cause was intent")
        adapter = _RecordingAdapter(responses)
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )

        run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        conn = sqlite3.connect(str(exp))
        try:
            gap_fp = conn.execute(
                "SELECT failure_precondition FROM expectation_gaps "
                "WHERE unit_id='u1'"
            ).fetchone()[0]
            corr = conn.execute(
                "SELECT original_value, corrected_value "
                "FROM expectation_corrections "
                "WHERE unit_id='u1' AND facet='failure_precondition'"
            ).fetchone()
        finally:
            conn.close()
        # Gap row still holds the original value.
        assert gap_fp == "step_4_plan"
        # Correction row captures the before/after delta.
        assert corr[0] == "step_4_plan"
        assert corr[1] == "step_1_intent"

    def test_skips_only_major_and_critical(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u_minor")
        _seed_expectation(exp, unit_id="u_major")
        _seed_gap(exp, unit_id="u_minor", severity="minor")
        _seed_gap(exp, unit_id="u_major", severity="major")

        adapter = _RecordingAdapter([_confirm_json()] * 100)
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )
        run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        conn = sqlite3.connect(str(exp))
        try:
            unit_ids = {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT unit_id FROM expectation_corrections"
                ).fetchall()
            }
        finally:
            conn.close()
        assert unit_ids == {"u_major"}


# ---------------------------------------------------------------------------
# AS-8: re-entrancy — resume on remaining facets
# ---------------------------------------------------------------------------


class TestReentrancy:
    def test_already_corrected_facets_are_skipped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1")
        _seed_gap(exp, unit_id="u1", severity="major")

        # Pre-seed two facets as already corrected.
        conn = sqlite3.connect(str(exp))
        try:
            for facet in ("commitment_point", "scope"):
                conn.execute(
                    "INSERT INTO expectation_corrections "
                    "(week_start, unit_id, facet, original_value, "
                    " corrected_value, correction_note, corrected_by) "
                    "VALUES (?, ?, ?, 'x', 'x', '', 'user')",
                    (WEEK_START, "u1", facet),
                )
            conn.commit()
        finally:
            conn.close()

        adapter = _RecordingAdapter([_confirm_json()] * 100)
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )
        written = run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        # Only the 4 remaining facets should have been processed.
        assert written == len(FACETS) - 2


# ---------------------------------------------------------------------------
# AS-6: auto-confirm sweep after 14 days
# ---------------------------------------------------------------------------


class TestAutoConfirmSweep:
    def test_stale_gap_gets_correction_rows(self, tmp_path: Path):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u_old")
        old_ts = (
            datetime.now(timezone.utc) - timedelta(days=15)
        ).strftime("%Y-%m-%d %H:%M:%S")
        _seed_gap(exp, unit_id="u_old", severity="major",
                  computed_at=old_ts)

        written = auto_confirm_sweep(str(exp))
        assert written == len(FACETS)

        conn = sqlite3.connect(str(exp))
        try:
            rows = conn.execute(
                "SELECT facet, corrected_by, original_value, corrected_value "
                "FROM expectation_corrections WHERE unit_id='u_old'"
            ).fetchall()
            auto = conn.execute(
                "SELECT auto_confirmed FROM expectation_gaps "
                "WHERE unit_id='u_old'"
            ).fetchone()[0]
        finally:
            conn.close()

        assert len(rows) == len(FACETS)
        for _facet, by, original, corrected in rows:
            assert by == "auto_confirm"
            assert original == corrected
        # AS-6: gap row's auto_confirmed flag flipped to 1.
        assert auto == 1

    def test_young_gap_untouched(self, tmp_path: Path):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u_young")
        # computed_at defaults to now.
        _seed_gap(exp, unit_id="u_young", severity="major")

        written = auto_confirm_sweep(str(exp))
        assert written == 0
        conn = sqlite3.connect(str(exp))
        try:
            auto = conn.execute(
                "SELECT auto_confirmed FROM expectation_gaps "
                "WHERE unit_id='u_young'"
            ).fetchone()[0]
        finally:
            conn.close()
        assert auto == 0

    def test_sweep_respects_existing_user_corrections(self, tmp_path: Path):
        """Stale gap whose facets are already user-corrected: no auto row written for those facets."""
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u_old")
        old_ts = (
            datetime.now(timezone.utc) - timedelta(days=20)
        ).strftime("%Y-%m-%d %H:%M:%S")
        _seed_gap(exp, unit_id="u_old", severity="major",
                  computed_at=old_ts)

        # Pre-seed a user correction on 'scope'.
        conn = sqlite3.connect(str(exp))
        try:
            conn.execute(
                "INSERT INTO expectation_corrections "
                "(week_start, unit_id, facet, original_value, "
                " corrected_value, correction_note, corrected_by) "
                "VALUES (?, ?, 'scope', 'fix X', 'fix X and Y', 'note', 'user')",
                (WEEK_START, "u_old"),
            )
            conn.commit()
        finally:
            conn.close()

        written = auto_confirm_sweep(str(exp))
        # All facets except 'scope' (pre-existing).
        assert written == len(FACETS) - 1

        conn = sqlite3.connect(str(exp))
        try:
            scope_by = conn.execute(
                "SELECT corrected_by FROM expectation_corrections "
                "WHERE unit_id='u_old' AND facet='scope'"
            ).fetchone()[0]
        finally:
            conn.close()
        # User correction preserved — sweep did NOT overwrite it.
        assert scope_by == "user"

    def test_sweep_is_idempotent(self, tmp_path: Path):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u_old")
        old_ts = (
            datetime.now(timezone.utc) - timedelta(days=20)
        ).strftime("%Y-%m-%d %H:%M:%S")
        _seed_gap(exp, unit_id="u_old", severity="major",
                  computed_at=old_ts)

        first = auto_confirm_sweep(str(exp))
        second = auto_confirm_sweep(str(exp))
        assert first == len(FACETS)
        assert second == 0  # PK-conflicted rows are no-ops.

    def test_sweep_noop_when_expectation_gaps_missing(self, tmp_path: Path):
        """Fresh DB, no tables at all — sweep returns 0 without crashing."""
        empty_db = tmp_path / "empty.db"
        # Create the file but leave it empty.
        sqlite3.connect(str(empty_db)).close()
        assert auto_confirm_sweep(str(empty_db)) == 0


# ---------------------------------------------------------------------------
# AS-7: retrospective .md is never rewritten
# ---------------------------------------------------------------------------


class TestRetrospectiveIdempotency:
    def test_correction_does_not_touch_output_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        _, exp = _init_dbs(tmp_path)
        _seed_expectation(exp, unit_id="u1")
        _seed_gap(exp, unit_id="u1", severity="major")

        retro_dir = tmp_path / "retrospectives"
        retro_dir.mkdir()
        retro_path = retro_dir / f"{WEEK_START}.md"
        retro_path.write_text("# existing retrospective (must not change)\n")
        mtime_before = retro_path.stat().st_mtime_ns

        adapter = _RecordingAdapter(
            [_correct_json("new scope")] + [_confirm_json()] * (len(FACETS) - 1)
        )
        monkeypatch.setattr(
            correction, "_get_adapter", lambda cfg: adapter
        )
        run_correction_session(
            WEEK_START,
            expectations_db=str(exp),
            config=_make_config(output_dir=str(retro_dir)),
            input_fn=lambda _p: "",
            output_fn=lambda _m: None,
        )
        # .md was not modified.
        assert retro_path.stat().st_mtime_ns == mtime_before
        assert retro_path.read_text() == (
            "# existing retrospective (must not change)\n"
        )
