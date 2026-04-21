"""Tests for ``synthesis/expectations.py`` (Epic #27 — X-1, Issue #72)."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
import unittest.mock as mock
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import (
    EXPECTED_EXPECTATIONS_TABLES,
    assert_schema,
    init_expectations_db,
    init_github_db,
    init_sessions_db,
)
from synthesis.expectations import (
    _build_unit_input,
    _extract_turns,
    _parse_llm_response,
    _surrounding_user_text,
    detect_structural_commitment_point,
    run_extraction,
)


FIXTURES = Path(__file__).parent / "fixtures" / "synthesis"


WEEK_START = "2026-04-14"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthesis_config(**overrides) -> SynthesisConfig:
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-6",
        summary_model="claude-haiku-4-5",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


def _init_dbs(tmp_path: Path) -> tuple[Path, Path, Path]:
    gh = tmp_path / "github.db"
    sess = tmp_path / "sessions.db"
    exp = tmp_path / "expectations.db"
    init_github_db(gh)
    init_sessions_db(sess)
    init_expectations_db(exp)
    return gh, sess, exp


def _seed_unit(
    db_path: Path,
    *,
    week_start: str = WEEK_START,
    unit_id: str,
    root_node_id: str = "",
    root_node_type: str = "session",
) -> None:
    if not root_node_id:
        root_node_id = f"n-{unit_id}"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO units "
            "(week_start, unit_id, root_node_type, root_node_id, "
            " elapsed_days, dark_time_pct, total_reprompts, "
            " review_cycles, status, outlier_flags, abandonment_flag) "
            f"VALUES (?, ?, '{root_node_type}', ?, 1.0, 0.0, 0, 0, 'closed', '[]', 0)",
            (week_start, unit_id, root_node_id),
        )
        conn.commit()
    finally:
        conn.close()


def _seed_session_and_node(
    gh_db: Path,
    sess_db: Path,
    *,
    week_start: str = WEEK_START,
    unit_root_id: str,
    session_uuid: str,
    raw_content_json: Any,
) -> None:
    """Seed a graph_node referencing a session, and the session row itself."""
    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO graph_nodes "
            "(week_start, node_id, node_type, node_ref) "
            "VALUES (?, ?, 'session', ?)",
            (week_start, unit_root_id, session_uuid),
        )
        conn.commit()
    finally:
        conn.close()

    payload = (
        raw_content_json
        if isinstance(raw_content_json, str) or raw_content_json is None
        else json.dumps(raw_content_json)
    )
    conn = sqlite3.connect(str(sess_db))
    try:
        conn.execute(
            "INSERT INTO sessions (session_uuid, raw_content_json) "
            "VALUES (?, ?)",
            (session_uuid, payload),
        )
        conn.commit()
    finally:
        conn.close()


def _expectation_rows(exp_db: Path, week_start: str = WEEK_START) -> list[dict]:
    conn = sqlite3.connect(str(exp_db))
    try:
        rows = conn.execute(
            "SELECT week_start, unit_id, commitment_point, expected_scope, "
            "       expected_effort, expected_outcome, confidence, model, "
            "       input_bytes, extracted_at, skip_reason "
            "FROM expectations WHERE week_start = ? ORDER BY unit_id",
            (week_start,),
        ).fetchall()
        return [
            {
                "week_start": r[0],
                "unit_id": r[1],
                "commitment_point": r[2],
                "expected_scope": r[3],
                "expected_effort": r[4],
                "expected_outcome": r[5],
                "confidence": r[6],
                "model": r[7],
                "input_bytes": r[8],
                "extracted_at": r[9],
                "skip_reason": r[10],
            }
            for r in rows
        ]
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ---------------------------------------------------------------------------
# Schema (AS-1)
# ---------------------------------------------------------------------------


def test_init_expectations_db_creates_schema(tmp_path: Path):
    """init_expectations_db creates the expectations table with all columns."""
    db = tmp_path / "expectations.db"
    init_expectations_db(db)

    assert db.exists()
    assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)


def test_init_expectations_db_idempotent(tmp_path: Path):
    """Re-initializing against an existing DB is a no-op."""
    db = tmp_path / "expectations.db"
    init_expectations_db(db)
    init_expectations_db(db)
    assert_schema(db, EXPECTED_EXPECTATIONS_TABLES)


def test_init_all_creates_expectations_db(tmp_path: Path):
    """init_all wires expectations.db in alongside the other three DBs."""
    from am_i_shipping.config_loader import (
        AppSwitchConfig,
        Config,
        DataConfig,
        GitHubConfig,
        SessionConfig,
    )
    from am_i_shipping.db import init_all

    config = Config(
        session=SessionConfig(projects_path="/fake"),
        github=GitHubConfig(repos=["a/b"]),
        appswitch=AppSwitchConfig(),
        data=DataConfig(data_dir=str(tmp_path / "data")),
    )
    init_all(config)
    assert (tmp_path / "data" / "expectations.db").exists()
    assert_schema(
        tmp_path / "data" / "expectations.db", EXPECTED_EXPECTATIONS_TABLES
    )


# ---------------------------------------------------------------------------
# Pure structural detector
# ---------------------------------------------------------------------------


def test_structural_detector_selects_last_user_text_before_tool_use():
    """Last user text turn before first tool-use is the candidate."""
    turns = [
        {"role": "user", "kind": "text", "text": "hi", "index": 0},
        {"role": "assistant", "kind": "text", "text": "what?", "index": 1},
        {"role": "user", "kind": "text", "text": "go ahead", "index": 2},
        {"role": "assistant", "kind": "tool_use", "text": "", "index": 3},
        {"role": "user", "kind": "text", "text": "later", "index": 4},
    ]
    idx = detect_structural_commitment_point(turns)
    assert idx == 2


def test_structural_detector_falls_back_to_last_user_text_if_no_tool_use():
    turns = [
        {"role": "user", "kind": "text", "text": "first", "index": 0},
        {"role": "assistant", "kind": "text", "text": "ok", "index": 1},
        {"role": "user", "kind": "text", "text": "final", "index": 2},
    ]
    idx = detect_structural_commitment_point(turns)
    assert idx == 2


def test_structural_detector_returns_none_for_empty_turns():
    assert detect_structural_commitment_point([]) is None


def test_structural_detector_returns_none_when_no_user_text():
    turns = [
        {"role": "assistant", "kind": "text", "text": "hi", "index": 0},
        {"role": "assistant", "kind": "tool_use", "text": "", "index": 1},
    ]
    assert detect_structural_commitment_point(turns) is None


# ---------------------------------------------------------------------------
# Turn extraction + context window
# ---------------------------------------------------------------------------


def test_extract_turns_handles_plain_string_content():
    blob = json.dumps(
        [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
    )
    turns = _extract_turns(blob)
    assert len(turns) == 2
    assert turns[0]["role"] == "user" and turns[0]["text"] == "hello"


def test_extract_turns_handles_structured_content_list():
    blob = json.dumps(
        [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "thinking"},
                    {"type": "tool_use", "name": "Read"},
                ],
            }
        ]
    )
    turns = _extract_turns(blob)
    assert [t["kind"] for t in turns] == ["text", "tool_use"]


def test_extract_turns_handles_malformed_json():
    assert _extract_turns("{not json") == []
    assert _extract_turns("") == []


def test_surrounding_user_text_window():
    turns = [
        {"role": "user", "kind": "text", "text": "a", "index": 0},
        {"role": "user", "kind": "text", "text": "b", "index": 1},
        {"role": "user", "kind": "text", "text": "c", "index": 2},
        {"role": "user", "kind": "text", "text": "d", "index": 3},
        {"role": "user", "kind": "text", "text": "e", "index": 4},
    ]
    ctx = _surrounding_user_text(turns, anchor_idx=2, window=2)
    assert [text for _i, text in ctx] == ["a", "b", "c", "d", "e"]
    ctx = _surrounding_user_text(turns, anchor_idx=0, window=2)
    assert [text for _i, text in ctx] == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# LLM response parsing
# ---------------------------------------------------------------------------


def test_parse_llm_response_parses_bare_json():
    text = (
        '{"commitment_point": "turn 2", "expected_scope": "foo.py", '
        '"expected_effort": "1 session", "expected_outcome": "tests pass", '
        '"confidence": 0.8}'
    )
    obj = _parse_llm_response(text)
    assert obj is not None
    assert obj["commitment_point"] == "turn 2"
    assert obj["confidence"] == 0.8


def test_parse_llm_response_strips_fences():
    text = "```json\n{\"commitment_point\": \"t\", \"confidence\": 0.3}\n```"
    obj = _parse_llm_response(text)
    assert obj is not None
    assert obj["commitment_point"] == "t"


def test_parse_llm_response_returns_none_on_junk():
    assert _parse_llm_response("no json here") is None
    assert _parse_llm_response("") is None


# ---------------------------------------------------------------------------
# End-to-end (AS-2, AS-3, AS-5, AS-6)
# ---------------------------------------------------------------------------


def _stub_adapter_json(expected_scope: str = "foo.py"):
    """Return an object with a `.call` method that returns a JSON response."""

    class _Result:
        def __init__(self, text: str):
            self.text = text
            self.cost_usd = 0.0
            self.input_chars = 0
            self.output_chars = len(text)

    class _Adapter:
        def __init__(self):
            self.calls: list[tuple[str, str, str, int]] = []

        def call(self, system, user, model, max_tokens):
            self.calls.append((system, user, model, max_tokens))
            body = {
                "commitment_point": "turn 0",
                "expected_scope": expected_scope,
                "expected_effort": "one session",
                "expected_outcome": "tests pass",
                "confidence": 0.75,
            }
            return _Result(json.dumps(body))

    return _Adapter()


def test_run_extraction_happy_path(tmp_path: Path):
    """One unit with transcript data → one populated expectations row."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_unit(gh, unit_id="unit-1", root_node_id="n-unit-1")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-unit-1",
        session_uuid="sess-1",
        raw_content_json=[
            {"role": "user", "content": "please refactor foo"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Edit"}],
            },
        ],
    )

    adapter = _stub_adapter_json()
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        rc = run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )

    assert rc == 0
    rows = _expectation_rows(exp)
    assert len(rows) == 1
    row = rows[0]
    assert row["unit_id"] == "unit-1"
    assert row["skip_reason"] is None
    assert row["commitment_point"] == "turn 0"
    assert row["expected_scope"] == "foo.py"
    assert row["confidence"] == 0.75
    assert row["input_bytes"] > 0
    assert len(adapter.calls) == 1


def test_run_extraction_skip_reason_for_empty_transcript(tmp_path: Path):
    """A unit whose only session has empty raw_content_json → skip row."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_unit(gh, unit_id="unit-empty", root_node_id="n-unit-empty")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-unit-empty",
        session_uuid="sess-empty",
        raw_content_json=None,
    )

    adapter = _stub_adapter_json()
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        rc = run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )

    assert rc == 0
    rows = _expectation_rows(exp)
    assert len(rows) == 1
    assert rows[0]["skip_reason"] == "raw_content_json_empty"
    assert rows[0]["commitment_point"] is None
    # LLM was not called for the skipped unit.
    assert len(adapter.calls) == 0


def test_run_extraction_covers_every_unit(tmp_path: Path):
    """3 units with varying data shapes — all 3 get rows in expectations."""
    gh, sess, exp = _init_dbs(tmp_path)
    # Unit A — has transcript.
    _seed_unit(gh, unit_id="unit-a", root_node_id="n-a")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-a",
        session_uuid="sess-a",
        raw_content_json=[
            {"role": "user", "content": "plan accepted"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Edit"}],
            },
        ],
    )
    # Unit B — session exists but transcript is empty.
    _seed_unit(gh, unit_id="unit-b", root_node_id="n-b")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-b",
        session_uuid="sess-b",
        raw_content_json=None,
    )
    # Unit C — no graph_nodes at all (unit_root_id isn't referenced).
    _seed_unit(gh, unit_id="unit-c", root_node_id="n-c-missing")

    adapter = _stub_adapter_json()
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        rc = run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )

    assert rc == 0
    rows = _expectation_rows(exp)
    got_unit_ids = {r["unit_id"] for r in rows}
    assert got_unit_ids == {"unit-a", "unit-b", "unit-c"}, (
        f"expected a row for every unit, got {got_unit_ids}"
    )

    by_id = {r["unit_id"]: r for r in rows}
    assert by_id["unit-a"]["skip_reason"] is None
    assert by_id["unit-b"]["skip_reason"] == "raw_content_json_empty"
    assert by_id["unit-c"]["skip_reason"] == "raw_content_json_empty"


def test_run_extraction_is_idempotent(tmp_path: Path):
    """Re-running without --rebuild issues zero new LLM calls."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_unit(gh, unit_id="unit-idem", root_node_id="n-idem")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-idem",
        session_uuid="sess-idem",
        raw_content_json=[
            {"role": "user", "content": "go"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Bash"}],
            },
        ],
    )

    adapter = _stub_adapter_json()
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )
        first_call_count = len(adapter.calls)

        # Second run — no --rebuild flag, should issue zero new calls.
        run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )

    assert first_call_count == 1
    assert len(adapter.calls) == 1, (
        f"idempotent re-run triggered extra calls: {len(adapter.calls)}"
    )

    rows = _expectation_rows(exp)
    assert len(rows) == 1


def test_run_extraction_rebuild_flag_wipes_and_rebuilds(tmp_path: Path):
    """--rebuild deletes the week's rows and re-extracts."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_unit(gh, unit_id="unit-rb", root_node_id="n-rb")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-rb",
        session_uuid="sess-rb",
        raw_content_json=[
            {"role": "user", "content": "go"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Bash"}],
            },
        ],
    )

    adapter = _stub_adapter_json(expected_scope="first.py")
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )
    first_rows = _expectation_rows(exp)
    assert first_rows[0]["expected_scope"] == "first.py"

    adapter2 = _stub_adapter_json(expected_scope="second.py")
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter2
    ):
        run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
            rebuild=True,
        )

    second_rows = _expectation_rows(exp)
    assert len(second_rows) == 1
    assert second_rows[0]["expected_scope"] == "second.py"
    assert len(adapter2.calls) == 1


# ---------------------------------------------------------------------------
# Diagnostic log (AS-8)
# ---------------------------------------------------------------------------


def test_run_extraction_logs_input_bytes_and_agreement_rate(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_unit(gh, unit_id="unit-log", root_node_id="n-log")
    _seed_session_and_node(
        gh,
        sess,
        unit_root_id="n-log",
        session_uuid="sess-log",
        raw_content_json=[
            {"role": "user", "content": "plan"},
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "name": "Edit"}],
            },
        ],
    )

    adapter = _stub_adapter_json()
    with caplog.at_level(logging.INFO, logger="synthesis.expectations"):
        with mock.patch(
            "synthesis.expectations._get_adapter", return_value=adapter
        ):
            run_extraction(
                _make_synthesis_config(),
                github_db=str(gh),
                sessions_db=str(sess),
                expectations_db=str(exp),
                week_start=WEEK_START,
            )

    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "input_bytes=" in joined, (
        f"expected per-unit input_bytes log line, got: {joined}"
    )
    assert "agreement_rate=" in joined, (
        f"expected run-level agreement_rate summary, got: {joined}"
    )


# ---------------------------------------------------------------------------
# Performance budget (AS-7)
# ---------------------------------------------------------------------------


def test_run_extraction_50_units_under_120s(tmp_path: Path):
    """50 units with offline adapter completes in <120s."""
    gh, sess, exp = _init_dbs(tmp_path)

    # Seed 50 units. Half have transcripts (LLM path), half don't (skip).
    for i in range(50):
        unit_id = f"unit-{i:02d}"
        root = f"n-{i:02d}"
        _seed_unit(gh, unit_id=unit_id, root_node_id=root)
        if i % 2 == 0:
            _seed_session_and_node(
                gh,
                sess,
                unit_root_id=root,
                session_uuid=f"sess-{i:02d}",
                raw_content_json=[
                    {"role": "user", "content": f"plan {i}"},
                    {
                        "role": "assistant",
                        "content": [{"type": "tool_use", "name": "Edit"}],
                    },
                ],
            )

    adapter = _stub_adapter_json()
    start = time.time()
    with mock.patch(
        "synthesis.expectations._get_adapter", return_value=adapter
    ):
        rc = run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )
    elapsed = time.time() - start

    assert rc == 0
    assert elapsed < 120.0, f"50-unit extraction took {elapsed:.1f}s (>120)"

    rows = _expectation_rows(exp)
    assert len(rows) == 50


# ---------------------------------------------------------------------------
# Empty week is a no-op
# ---------------------------------------------------------------------------


def test_run_extraction_empty_week_is_noop(tmp_path: Path):
    gh, sess, exp = _init_dbs(tmp_path)
    rc = run_extraction(
        _make_synthesis_config(),
        github_db=str(gh),
        sessions_db=str(sess),
        expectations_db=str(exp),
        week_start="2099-01-01",
    )
    assert rc == 0
    assert _expectation_rows(exp, "2099-01-01") == []


# ---------------------------------------------------------------------------
# Real-session fixtures: issue #139 and #163
# These cover the session_issue_attribution fallback and the all-user-turns
# context fix for refine-issue sessions (issue #233).
# ---------------------------------------------------------------------------


def _seed_issue_unit(
    gh_db: Path,
    sess_db: Path,
    *,
    week_start: str = WEEK_START,
    unit_id: str,
    repo: str,
    issue_number: int,
    session_uuid: str,
    raw_content_json: Any,
) -> None:
    """Seed an issue-rooted unit with its session via session_issue_attribution."""
    root_node_id = f"issue:{repo}#{issue_number}"
    _seed_unit(
        gh_db,
        week_start=week_start,
        unit_id=unit_id,
        root_node_id=root_node_id,
        root_node_type="issue",
    )
    payload = (
        raw_content_json
        if isinstance(raw_content_json, str) or raw_content_json is None
        else json.dumps(raw_content_json)
    )
    conn = sqlite3.connect(str(sess_db))
    try:
        conn.execute(
            "INSERT INTO sessions (session_uuid, raw_content_json) VALUES (?, ?)",
            (session_uuid, payload),
        )
        conn.commit()
    finally:
        conn.close()

    conn = sqlite3.connect(str(gh_db))
    try:
        conn.execute(
            "INSERT INTO session_issue_attribution "
            "(week_start, session_uuid, repo, issue_number, phase, fraction) "
            "VALUES (?, ?, ?, ?, 'planning', 1.0)",
            (week_start, session_uuid, repo, issue_number),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def fixture_139() -> list:
    return json.loads((FIXTURES / "session_issue_139.json").read_text())


@pytest.fixture()
def fixture_163() -> list:
    return json.loads((FIXTURES / "session_issue_163.json").read_text())


def test_issue_163_refine_session_includes_confirmation_turn(
    tmp_path: Path, fixture_163: list
):
    """All user text turns are sent so the LLM can see 'yes this is good' (turn 15).

    Before the fix, only ±2 turns around the structural candidate were sent,
    which resolved to the /resolve-issue invocation and [Request interrupted by
    user] — no planning content at all.
    """
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_issue_unit(
        gh,
        sess,
        unit_id="46e3bb0455004d17",
        repo="hyang0129/supreme-claudemander",
        issue_number=163,
        session_uuid="ad86311e-7d67-4a68-bdaa-8a06963d9fc9",
        raw_content_json=fixture_163,
    )

    gh_conn = sqlite3.connect(str(gh))
    sess_conn = sqlite3.connect(str(sess))
    unit_input, input_bytes, candidate_idx, skip_reason = _build_unit_input(
        gh_conn, sess_conn, "46e3bb0455004d17", WEEK_START
    )
    gh_conn.close()
    sess_conn.close()

    assert skip_reason == "", f"unexpected skip: {skip_reason}"
    assert input_bytes > 0
    # The full context must include the user confirmation turn.
    assert "yes this is good" in unit_input, (
        "confirmation turn missing from LLM input — context window too narrow"
    )
    # The structural candidate (interrupt / slash command) should NOT be the
    # only user turn visible to the LLM.
    assert len(re.findall(r"^turn \d+[:\s]", unit_input, re.MULTILINE)) >= 4, (
        "fewer turns than expected — all-user-turns expansion not working"
    )


def test_issue_163_refine_session_extraction_produces_populated_row(
    tmp_path: Path, fixture_163: list
):
    """End-to-end: issue-rooted refine-session unit produces a non-skipped row."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_issue_unit(
        gh,
        sess,
        unit_id="46e3bb0455004d17",
        repo="hyang0129/supreme-claudemander",
        issue_number=163,
        session_uuid="ad86311e-7d67-4a68-bdaa-8a06963d9fc9",
        raw_content_json=fixture_163,
    )

    adapter = _stub_adapter_json(expected_scope="profiles/main")
    with mock.patch("synthesis.expectations._get_adapter", return_value=adapter):
        rc = run_extraction(
            _make_synthesis_config(),
            github_db=str(gh),
            sessions_db=str(sess),
            expectations_db=str(exp),
            week_start=WEEK_START,
        )

    assert rc == 0
    rows = _expectation_rows(exp)
    assert len(rows) == 1
    row = rows[0]
    assert row["skip_reason"] is None, f"expected populated row, got skip: {row['skip_reason']}"
    assert row["expected_scope"] == "profiles/main"
    assert len(adapter.calls) == 1
    # Verify the adapter received context with the confirmation turn.
    _, user_prompt, _, _ = adapter.calls[0]
    assert "yes this is good" in user_prompt


def test_issue_139_session_attribution_fallback(
    tmp_path: Path, fixture_139: list
):
    """Issue #139: session found via attribution (not graph_nodes) → non-empty input."""
    gh, sess, exp = _init_dbs(tmp_path)
    _seed_issue_unit(
        gh,
        sess,
        unit_id="45d36ccb02b9fb09",
        repo="hyang0129/supreme-claudemander",
        issue_number=139,
        session_uuid="0dc66653-a8ce-456e-9a84-3ff29e0d5153",
        raw_content_json=fixture_139,
    )

    gh_conn = sqlite3.connect(str(gh))
    sess_conn = sqlite3.connect(str(sess))
    unit_input, input_bytes, candidate_idx, skip_reason = _build_unit_input(
        gh_conn, sess_conn, "45d36ccb02b9fb09", WEEK_START
    )
    gh_conn.close()
    sess_conn.close()

    # The session is short (7 messages) but must be found via attribution fallback.
    assert skip_reason == "", f"unexpected skip: {skip_reason}"
    assert input_bytes > 0
