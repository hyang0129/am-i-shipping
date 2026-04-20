"""Tests for ``synthesis/revision_detector.py`` (Epic #27 — X-3, Issue #74)."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    EXPECTED_EXPECTATIONS_TABLES,
    assert_schema,
    init_expectations_db,
    init_github_db,
    init_sessions_db,
)
from synthesis import revision_detector
from synthesis.revision_detector import (
    FACET_ENUM,
    REVISION_TRIGGER_ENUM,
    SESSION_BREAK_THRESHOLD_SECONDS,
    classify_revision,
    detect_structural_triggers,
    load_revision_rows,
)


WEEK_START = "2026-04-14"


def _make_config(**overrides) -> SynthesisConfig:
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-5",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


def _turn(role: str, kind: str, text: str = "") -> dict:
    return {"role": role, "kind": kind, "text": text, "index": 0}


def _raw_content_from_turns(turns: list[dict]) -> str:
    """Build a raw_content_json that _extract_turns can parse back."""
    messages = []
    # Group sequential turns into messages by role.
    for t in turns:
        if t["kind"] == "text":
            messages.append({"role": t["role"], "content": t["text"]})
        elif t["kind"] == "tool_use":
            messages.append(
                {
                    "role": t["role"],
                    "content": [{"type": "tool_use", "name": "x", "input": {}}],
                }
            )
    return json.dumps(messages)


# ---------------------------------------------------------------------------
# AS-1 — schema
# ---------------------------------------------------------------------------


class TestExpectationRevisionsSchema:
    def test_init_creates_expectation_revisions_table(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)

        conn = sqlite3.connect(str(db))
        try:
            cols = {
                r[1]
                for r in conn.execute(
                    "PRAGMA table_info(expectation_revisions)"
                ).fetchall()
            }
        finally:
            conn.close()

        required = {
            "week_start",
            "unit_id",
            "revision_index",
            "revision_turn",
            "revision_trigger",
            "facet",
            "before_text",
            "after_text",
            "confidence",
            "detected_at",
        }
        missing = required - cols
        assert not missing, f"missing columns: {missing}"

    def test_idempotent_init(self, tmp_path: Path):
        db = tmp_path / "expectations.db"
        init_expectations_db(db)
        init_expectations_db(db)
        assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)


# ---------------------------------------------------------------------------
# AS-2 / AS-3 / AS-4 — pure-function structural trigger walker
# ---------------------------------------------------------------------------


class TestDetectStructuralTriggers:
    def test_no_triggers_for_clean_unit(self):
        turns = [
            _turn("user", "text", "please fix X"),
            _turn("assistant", "text", "ok"),
            _turn("assistant", "tool_use"),
        ]
        records = detect_structural_triggers(
            turns, commitment_turn_idx=0, reprompt_count=0
        )
        assert records == []

    def test_reprompt_anchored_after_assistant(self):
        # user (commitment) -> assistant -> user (reprompt #1)
        turns = [
            _turn("user", "text", "plan: do X"),
            _turn("assistant", "text", "ok, starting"),
            _turn("user", "text", "wait, also do Y"),
        ]
        records = detect_structural_triggers(
            turns, commitment_turn_idx=0, reprompt_count=1
        )
        # At least one reprompt trigger at turn 2.
        reprompts = [r for r in records if r["trigger"] == "reprompt"]
        assert len(reprompts) == 1
        assert reprompts[0]["turn_idx"] == 2

    def test_scope_change_cue_triggers(self):
        turns = [
            _turn("user", "text", "fix X"),
            _turn("assistant", "text", "ok"),
            _turn("user", "text", "actually, rewrite the whole module"),
        ]
        records = detect_structural_triggers(
            turns, commitment_turn_idx=0, reprompt_count=0
        )
        scope_changes = [r for r in records if r["trigger"] == "scope_change_turn"]
        assert len(scope_changes) == 1
        assert scope_changes[0]["turn_idx"] == 2

    def test_session_break_anchors_to_resumption(self):
        turns = [
            _turn("user", "text", "start"),
            _turn("assistant", "text", "ok"),
            _turn("user", "text", "resumed"),
        ]
        prev_end = datetime(2026, 4, 10, 10, 0, 0, tzinfo=timezone.utc)
        this_start = prev_end + timedelta(hours=48)
        records = detect_structural_triggers(
            turns,
            commitment_turn_idx=0,
            reprompt_count=0,
            session_boundaries=[(2, prev_end, this_start)],
        )
        breaks = [r for r in records if r["trigger"] == "session_break"]
        assert len(breaks) == 1
        assert breaks[0]["turn_idx"] == 2

    def test_session_break_under_threshold_ignored(self):
        turns = [
            _turn("user", "text", "start"),
            _turn("user", "text", "quick resume"),
        ]
        prev_end = datetime(2026, 4, 10, 10, 0, 0, tzinfo=timezone.utc)
        this_start = prev_end + timedelta(hours=2)  # < 24h
        records = detect_structural_triggers(
            turns,
            commitment_turn_idx=0,
            reprompt_count=0,
            session_boundaries=[(1, prev_end, this_start)],
        )
        breaks = [r for r in records if r["trigger"] == "session_break"]
        assert breaks == []

    def test_only_triggers_after_commitment(self):
        # reprompt before commitment is ignored.
        turns = [
            _turn("user", "text", "vague idea"),
            _turn("assistant", "text", "what exactly"),
            _turn("user", "text", "actually this"),  # scope cue pre-commit
            _turn("assistant", "text", "ok"),
            _turn("user", "text", "final plan"),  # commitment
            _turn("assistant", "text", "going"),
            _turn("user", "text", "also handle Z"),  # post-commit
        ]
        records = detect_structural_triggers(
            turns, commitment_turn_idx=4, reprompt_count=2
        )
        for r in records:
            assert r["turn_idx"] > 4, (
                f"trigger at {r['turn_idx']} is not after commitment 4"
            )


# ---------------------------------------------------------------------------
# LLM classifier (fake adapter path)
# ---------------------------------------------------------------------------


class TestClassifyRevision:
    def test_fake_adapter_returns_low_confidence_record(self):
        from synthesis.llm_adapter import _get_adapter

        adapter = _get_adapter(_make_config())
        record = {
            "trigger": "reprompt",
            "turn_idx": 5,
            "text": "actually, also do Y",
            "context": "turn 5 [ANCHOR]: actually, also do Y",
        }
        result = classify_revision(record, adapter=adapter, model="claude-sonnet-4-5")
        assert result["facet"] in FACET_ENUM
        assert isinstance(result["before_text"], str)
        assert isinstance(result["after_text"], str)
        assert 0.0 <= result["confidence"] <= 1.0


# ---------------------------------------------------------------------------
# Pipeline integration (AS-2, AS-7, AS-8)
# ---------------------------------------------------------------------------


def _init_dbs(tmp_path: Path) -> tuple[Path, Path, Path]:
    gh = tmp_path / "github.db"
    sess = tmp_path / "sessions.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_sessions_db(sess)
    init_expectations_db(exp)
    return gh, sess, exp


def _seed_unit_and_graph(
    gh_db: Path,
    *,
    unit_id: str,
    session_uuids: list[str],
    week_start: str = WEEK_START,
) -> None:
    """Seed a units row + graph_nodes + graph_edges linking to sessions."""
    conn = sqlite3.connect(str(gh_db))
    try:
        root_node_id = f"unit:{unit_id}"
        conn.execute(
            "INSERT INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, review_cycles, "
            " status, outlier_flags, abandonment_flag) "
            "VALUES (?, ?, 'session', ?, 1.0, 0.0, 0, 0, 'closed', '[]', 0)",
            (week_start, unit_id, root_node_id),
        )
        conn.execute(
            "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref) "
            "VALUES (?, ?, 'unit', ?)",
            (week_start, root_node_id, unit_id),
        )
        for sid in session_uuids:
            conn.execute(
                "INSERT INTO graph_nodes (week_start, node_id, node_type, node_ref) "
                "VALUES (?, ?, 'session', ?)",
                (week_start, sid, sid),
            )
            conn.execute(
                "INSERT INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type) "
                "VALUES (?, ?, ?, 'unit_contains_session')",
                (week_start, root_node_id, sid),
            )
        conn.commit()
    finally:
        conn.close()


def _seed_session(
    sess_db: Path,
    *,
    session_uuid: str,
    turns: list[dict],
    reprompt_count: int = 0,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> None:
    conn = sqlite3.connect(str(sess_db))
    try:
        conn.execute(
            "INSERT INTO sessions "
            "(session_uuid, reprompt_count, raw_content_json, "
            " session_started_at, session_ended_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                session_uuid,
                reprompt_count,
                _raw_content_from_turns(turns),
                started_at,
                ended_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_expectation(
    exp_db: Path,
    *,
    unit_id: str,
    commitment_point: str | None = "turn 0",
    skip_reason: str | None = None,
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
                WEEK_START,
                unit_id,
                commitment_point,
                "fix X",
                "one session",
                "tests pass",
                0.8 if skip_reason is None else None,
                "claude-sonnet-4-5",
                100,
                skip_reason,
            ),
        )
        conn.commit()
    finally:
        conn.close()


class TestPipelineRun:
    def test_unit_with_reprompt_produces_row(self, tmp_path: Path):
        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(
            gh, unit_id="u1", session_uuids=["s1"]
        )
        _seed_session(
            sess,
            session_uuid="s1",
            reprompt_count=1,
            turns=[
                _turn("user", "text", "do X"),
                _turn("assistant", "text", "ok"),
                _turn("user", "text", "actually also do Y"),
            ],
        )
        _seed_expectation(exp, unit_id="u1", commitment_point="turn 0")

        written = revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
        )
        assert written >= 1
        rows = load_revision_rows(str(exp), WEEK_START)
        assert len(rows) >= 1
        assert rows[0]["unit_id"] == "u1"
        assert rows[0]["revision_trigger"] in REVISION_TRIGGER_ENUM
        assert rows[0]["facet"] in FACET_ENUM

    def test_unit_without_triggers_produces_no_rows(self, tmp_path: Path):
        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(gh, unit_id="u_clean", session_uuids=["s1"])
        _seed_session(
            sess,
            session_uuid="s1",
            reprompt_count=0,
            turns=[
                _turn("user", "text", "plan"),
                _turn("assistant", "text", "executing"),
            ],
        )
        _seed_expectation(exp, unit_id="u_clean", commitment_point="turn 0")

        written = revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
        )
        assert written == 0
        assert load_revision_rows(str(exp), WEEK_START) == []

    def test_session_break_produces_row(self, tmp_path: Path):
        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(
            gh, unit_id="u_break", session_uuids=["s_a", "s_b"]
        )
        _seed_session(
            sess,
            session_uuid="s_a",
            reprompt_count=0,
            turns=[
                _turn("user", "text", "start"),
                _turn("assistant", "text", "ok"),
            ],
            started_at="2026-04-10T10:00:00+00:00",
            ended_at="2026-04-10T11:00:00+00:00",
        )
        _seed_session(
            sess,
            session_uuid="s_b",
            reprompt_count=0,
            turns=[
                _turn("user", "text", "back now"),
                _turn("assistant", "text", "continuing"),
            ],
            # 48h later → exceeds threshold
            started_at="2026-04-12T11:00:00+00:00",
            ended_at="2026-04-12T12:00:00+00:00",
        )
        _seed_expectation(exp, unit_id="u_break", commitment_point="turn 0")

        revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
        )
        rows = load_revision_rows(str(exp), WEEK_START)
        triggers = {r["revision_trigger"] for r in rows}
        assert "session_break" in triggers


# ---------------------------------------------------------------------------
# AS-7 — idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_rerun_without_rebuild_preserves_rows(self, tmp_path: Path):
        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(gh, unit_id="u1", session_uuids=["s1"])
        _seed_session(
            sess,
            session_uuid="s1",
            reprompt_count=1,
            turns=[
                _turn("user", "text", "do X"),
                _turn("assistant", "text", "ok"),
                _turn("user", "text", "actually also do Y"),
            ],
        )
        _seed_expectation(exp, unit_id="u1", commitment_point="turn 0")

        revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
        )
        conn = sqlite3.connect(str(exp))
        try:
            before = conn.execute(
                "SELECT revision_index, detected_at FROM expectation_revisions "
                "WHERE unit_id='u1' ORDER BY revision_index"
            ).fetchall()
            count_before = len(before)
        finally:
            conn.close()
        assert count_before >= 1

        # Re-run without rebuild — same row count, same detected_at values.
        revision_detector.run(
            WEEK_START,
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            config=_make_config(),
        )
        conn = sqlite3.connect(str(exp))
        try:
            after = conn.execute(
                "SELECT revision_index, detected_at FROM expectation_revisions "
                "WHERE unit_id='u1' ORDER BY revision_index"
            ).fetchall()
        finally:
            conn.close()
        assert len(after) == count_before
        assert after == before, "detected_at must be preserved on idempotent re-run"


# ---------------------------------------------------------------------------
# AS-8 — integration into run_synthesis + AS-5/AS-6 rendering
# ---------------------------------------------------------------------------


class TestWeeklyIntegration:
    def test_revision_pass_runs_inside_run_synthesis(self, tmp_path: Path):
        from synthesis.weekly import run_synthesis

        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(gh, unit_id="u1", session_uuids=["s1"])
        _seed_session(
            sess,
            session_uuid="s1",
            reprompt_count=1,
            turns=[
                _turn("user", "text", "do X"),
                _turn("assistant", "text", "ok"),
                _turn("user", "text", "actually also do Y"),
            ],
        )
        _seed_expectation(exp, unit_id="u1", commitment_point="turn 0")

        # unit_summaries row required by run_synthesis.
        conn = sqlite3.connect(str(gh))
        try:
            conn.execute(
                "INSERT INTO unit_summaries "
                "(week_start, unit_id, summary_text, model, input_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "u1", "summary", "fake", 7),
            )
            conn.commit()
        finally:
            conn.close()

        out_dir = tmp_path / "retrospectives"
        cfg = replace(_make_config(), output_dir=str(out_dir))

        result = run_synthesis(
            cfg, gh, sess, WEEK_START, dry_run=False, expectations_db=exp,
        )
        assert result is not None

        rows = load_revision_rows(str(exp), WEEK_START)
        assert len(rows) >= 1

    def test_section_order_and_low_confidence_marker_in_prompt(
        self, tmp_path: Path
    ):
        """Verify dry-run prompt contains Gaps BEFORE Revisions, with low-conf marker."""
        from synthesis.weekly import run_synthesis

        gh, sess, exp = _init_dbs(tmp_path)
        _seed_unit_and_graph(gh, unit_id="u1", session_uuids=["s1"])
        _seed_session(
            sess,
            session_uuid="s1",
            reprompt_count=1,
            turns=[
                _turn("user", "text", "do X"),
                _turn("assistant", "text", "ok"),
                _turn("user", "text", "actually also do Y"),
            ],
        )
        _seed_expectation(exp, unit_id="u1", commitment_point="turn 0")

        conn = sqlite3.connect(str(gh))
        try:
            conn.execute(
                "INSERT INTO unit_summaries "
                "(week_start, unit_id, summary_text, model, input_bytes) "
                "VALUES (?, ?, ?, ?, ?)",
                (WEEK_START, "u1", "summary", "fake", 7),
            )
            conn.commit()
        finally:
            conn.close()

        out_dir = tmp_path / "retrospectives"
        cfg = replace(_make_config(), output_dir=str(out_dir))

        # dry_run writes the assembled prompt to disk — inspect it.
        dry_path = run_synthesis(
            cfg, gh, sess, WEEK_START, dry_run=True, expectations_db=exp,
        )
        assert dry_path is not None
        text = Path(dry_path).read_text(encoding="utf-8")

        # The user message must include the Revisions block heading.
        assert "## Expectation Revisions" in text

        # The system prompt must list Expectation Gaps BEFORE Expectation
        # Revisions. Revisions must come BEFORE Clarifying Questions.
        gaps_idx = text.index("`## Expectation Gaps`")
        revisions_idx = text.index("`## Expectation Revisions`")
        clarifying_idx = text.index("`## Clarifying Questions`")
        assert gaps_idx < revisions_idx < clarifying_idx

        # Low-confidence marker — the fake adapter yields confidence=0.3.
        assert "[low confidence]" in text
