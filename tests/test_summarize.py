"""Tests for ``synthesis/summarize.py`` (Issue #64)."""

from __future__ import annotations

import sqlite3
import unittest.mock as mock
from dataclasses import replace
from pathlib import Path

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.db import init_github_db, init_sessions_db, SYNTHESIS_UNIT_SUMMARIES_SCHEMA
from synthesis.summarize import run_summarization


WEEK_START = "2026-04-14"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthesis_config(**overrides) -> SynthesisConfig:
    """Return a SynthesisConfig suitable for offline tests."""
    base = SynthesisConfig(
        anthropic_api_key_env="ANTHROPIC_API_KEY",
        model="claude-sonnet-4-5",
        summary_model="claude-haiku-4-5",
        output_dir="retrospectives",
        week_start="monday",
        abandonment_days=14,
        outlier_sigma=2.0,
    )
    if overrides:
        return replace(base, **overrides)
    return base


def _make_github_db(tmp_path: Path) -> Path:
    """Initialize a temporary github.db with full schema (including migrations)."""
    db_path = tmp_path / "github.db"
    init_github_db(db_path)
    return db_path


def _make_sessions_db(tmp_path: Path) -> Path:
    """Initialize a temporary sessions.db."""
    db_path = tmp_path / "sessions.db"
    init_sessions_db(db_path)
    return db_path


def _seed_unit(
    db_path: Path,
    *,
    week_start: str = WEEK_START,
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
    """Insert a minimal ``units`` row into the given DB."""
    if not root_node_id:
        root_node_id = f"n-{unit_id}"
    conn = sqlite3.connect(str(db_path))
    try:
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
        conn.commit()
    finally:
        conn.close()


def _seed_summary(
    db_path: Path,
    *,
    week_start: str = WEEK_START,
    unit_id: str,
    summary_text: str = "existing summary",
    model: str = "claude-haiku-4-5",
    input_bytes: int = 100,
) -> None:
    """Insert a ``unit_summaries`` row to simulate a prior run."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO unit_summaries "
            "(week_start, unit_id, summary_text, model, input_bytes) "
            "VALUES (?, ?, ?, ?, ?)",
            (week_start, unit_id, summary_text, model, input_bytes),
        )
        conn.commit()
    finally:
        conn.close()


def _get_summary_rows(db_path: Path, week_start: str = WEEK_START) -> list[dict]:
    """Fetch all ``unit_summaries`` rows for the given week."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT week_start, unit_id, summary_text, model, input_bytes, generated_at "
            "FROM unit_summaries WHERE week_start = ? ORDER BY unit_id",
            (week_start,),
        ).fetchall()
        return [
            {
                "week_start": r[0],
                "unit_id": r[1],
                "summary_text": r[2],
                "model": r[3],
                "input_bytes": r[4],
                "generated_at": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def _run(
    github_db: Path,
    sessions_db: Path,
    *,
    config: SynthesisConfig | None = None,
    week_start: str = WEEK_START,
    rebuild: bool = False,
) -> int:
    """Invoke ``run_summarization`` with file-based test DBs."""
    cfg = config or _make_synthesis_config()
    return run_summarization(
        cfg,
        github_db=str(github_db),
        sessions_db=str(sessions_db),
        week_start=week_start,
        rebuild=rebuild,
    )


# ---------------------------------------------------------------------------
# Module-level fixture: scrub live-mode env vars
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    """Guarantee offline mode (FakeAnthropicClient) for all tests here."""
    monkeypatch.delenv("AMIS_SYNTHESIS_LIVE", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ---------------------------------------------------------------------------
# Test 1: empty week is a no-op
# ---------------------------------------------------------------------------


def test_empty_week_is_noop(tmp_path: Path):
    """A week with no units in the ``units`` table → run returns 0, no summaries."""
    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    result = _run(github_db, sessions_db, week_start="2099-01-01")
    assert result == 0

    rows = _get_summary_rows(github_db, "2099-01-01")
    assert rows == [], f"expected no summary rows, got: {rows}"


# ---------------------------------------------------------------------------
# Test 2: happy-path — one unit → one summary row
# ---------------------------------------------------------------------------


def test_populates_unit_summaries(tmp_path: Path):
    """Seed one unit, run summarization, assert one ``unit_summaries`` row."""
    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    _seed_unit(github_db, unit_id="unit-001")

    result = _run(github_db, sessions_db)
    assert result == 0

    rows = _get_summary_rows(github_db)
    assert len(rows) == 1, f"expected 1 summary row, got {len(rows)}: {rows}"

    row = rows[0]
    assert row["unit_id"] == "unit-001"
    assert row["week_start"] == WEEK_START
    assert row["summary_text"], "summary_text must be non-empty"
    assert row["model"] == _make_synthesis_config().summary_model
    assert row["input_bytes"] >= 0
    assert row["generated_at"] is not None


# ---------------------------------------------------------------------------
# Test 3: skip when summary already exists
# ---------------------------------------------------------------------------


def test_skip_when_summary_exists(tmp_path: Path):
    """Unit with an existing summary is skipped; LLM is called 0 times."""
    import synthesis.summarize as summarize_module

    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    _seed_unit(github_db, unit_id="unit-skip")
    _seed_summary(github_db, unit_id="unit-skip", summary_text="already done")

    call_count = [0]
    real_summarize_unit = summarize_module._summarize_unit

    def _counting_summarize(config, unit_input):
        call_count[0] += 1
        return real_summarize_unit(config, unit_input)

    with mock.patch.object(summarize_module, "_summarize_unit", side_effect=_counting_summarize):
        result = _run(github_db, sessions_db)

    assert result == 0
    # The existing summary must NOT have triggered an LLM call.
    assert call_count[0] == 0, (
        f"LLM called {call_count[0]} time(s) for an already-summarized unit; "
        "expected 0"
    )

    # The original summary must still be present unchanged.
    rows = _get_summary_rows(github_db)
    assert len(rows) == 1
    assert rows[0]["summary_text"] == "already done"


# ---------------------------------------------------------------------------
# Test 4: --rebuild-summaries wipes and regenerates
# ---------------------------------------------------------------------------


def test_rebuild_summaries_wipes_and_regenerates(tmp_path: Path):
    """With rebuild=True, existing summary is replaced and LLM is called once."""
    import synthesis.summarize as summarize_module

    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    _seed_unit(github_db, unit_id="unit-rebuild")
    _seed_summary(github_db, unit_id="unit-rebuild", summary_text="old summary")

    old_rows = _get_summary_rows(github_db)
    assert len(old_rows) == 1

    call_count = [0]
    real_summarize_unit = summarize_module._summarize_unit

    def _counting_summarize(config, unit_input):
        call_count[0] += 1
        return real_summarize_unit(config, unit_input)

    with mock.patch.object(summarize_module, "_summarize_unit", side_effect=_counting_summarize):
        result = _run(github_db, sessions_db, rebuild=True)

    assert result == 0
    # The LLM must have been called exactly once (for the one unit).
    assert call_count[0] == 1, (
        f"expected LLM called once on rebuild, got {call_count[0]}"
    )

    # A new summary row should exist, replacing the old one.
    new_rows = _get_summary_rows(github_db)
    assert len(new_rows) == 1, f"expected 1 row after rebuild, got {len(new_rows)}"
    # The FakeAnthropicClient returns the canned retrospective Markdown
    # which differs from "old summary".
    assert new_rows[0]["summary_text"] != "old summary", (
        "rebuild must replace the old summary_text"
    )


# ---------------------------------------------------------------------------
# Test 5: summary_model from config is passed to the LLM
# ---------------------------------------------------------------------------


def test_uses_summary_model_config(tmp_path: Path):
    """The model arg passed to client.messages.create must match config.summary_model."""
    import synthesis.summarize as summarize_module

    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    _seed_unit(github_db, unit_id="unit-model-check")

    sentinel_model = "claude-haiku-test-sentinel"
    cfg = _make_synthesis_config(summary_model=sentinel_model)

    captured_models: list[str] = []
    real_summarize = summarize_module._summarize_unit

    def _capturing_summarize(config, unit_input):
        # Patch _get_adapter on the summarize module (where it was imported)
        # so calls from within _summarize_unit see the spy.
        import synthesis.summarize as summarize_module
        real_get_adapter = summarize_module._get_adapter

        def _spy_get_adapter(cfg):
            adapter = real_get_adapter(cfg)
            original_call = adapter.call

            def _spy_call(system, user, model, max_tokens):
                captured_models.append(model)
                return original_call(system, user, model, max_tokens)

            adapter.call = _spy_call
            return adapter

        with mock.patch.object(summarize_module, "_get_adapter", side_effect=_spy_get_adapter):
            return real_summarize(config, unit_input)

    with mock.patch.object(summarize_module, "_summarize_unit", side_effect=_capturing_summarize):
        result = _run(github_db, sessions_db, config=cfg)

    assert result == 0
    assert captured_models, "LLM was never called — no model to check"
    assert all(m == sentinel_model for m in captured_models), (
        f"expected all model calls to use {sentinel_model!r}, got: {captured_models}"
    )


# ---------------------------------------------------------------------------
# Test 6: unit with no session transcripts writes a summary row
# ---------------------------------------------------------------------------


def test_zero_transcript_unit_writes_placeholder(tmp_path: Path):
    """A unit with no session nodes in its graph still gets a ``unit_summaries`` row.

    The ``_build_unit_input`` function returns the placeholder string
    ``"(no session transcripts for this unit)"`` when no session transcripts
    exist. The summarizer should still call the LLM (with the placeholder
    as input) and store the resulting summary, so the ``unit_summaries``
    table is complete for the week.
    """
    github_db = _make_github_db(tmp_path)
    sessions_db = _make_sessions_db(tmp_path)

    # Seed a unit whose root_node_id does not appear in graph_nodes,
    # so _unit_nodes returns [] → no session UUIDs → no transcripts.
    _seed_unit(
        github_db,
        unit_id="unit-no-transcripts",
        root_node_id="n-no-transcripts",
    )
    # No graph_nodes, no sessions rows → _build_unit_input returns placeholder.

    result = _run(github_db, sessions_db)
    assert result == 0

    rows = _get_summary_rows(github_db)
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {rows}"
    row = rows[0]
    assert row["unit_id"] == "unit-no-transcripts"
    # The FakeAnthropicClient ignores the input and returns its canned
    # Markdown; the key invariant is that a non-empty summary_text was stored.
    assert row["summary_text"], "summary_text must be non-empty even with no transcripts"
    assert row["input_bytes"] >= 0


# ---------------------------------------------------------------------------
# Test 7: live-mode system prompt uses cache_control ephemeral block shape
# ---------------------------------------------------------------------------


def test_cache_control_block_shape_live(tmp_path: Path):
    """AnthropicAdapter wraps the system prompt with cache_control: ephemeral.

    Tests the adapter layer directly — no real Anthropic key required.
    The cache_control behaviour used to live in ``_summarize_unit``; it
    now lives in ``AnthropicAdapter.call()`` so that all live SDK paths
    share it automatically.
    """
    from synthesis.llm_adapter import AnthropicAdapter

    captured_kwargs: list[dict] = []

    class _FakeContent:
        text = "cached narrative"

    class _FakeUsage:
        input_tokens = 10
        output_tokens = 5

    class _FakeResponse:
        content = [_FakeContent()]
        usage = _FakeUsage()

    class _FakeMessages:
        def create(self, **kwargs):
            captured_kwargs.append(kwargs)
            return _FakeResponse()

    class _FakeSDKClient:
        messages = _FakeMessages()

    # Patch anthropic.Anthropic so AnthropicAdapter uses our capturing client.
    with mock.patch("synthesis.llm_adapter.AnthropicAdapter.call", autospec=True) as _mock:
        pass  # just verify import

    # Call AnthropicAdapter.call() directly with a patched anthropic module.
    import synthesis.llm_adapter as llm_adapter_module

    real_anthropic = None
    try:
        import anthropic as _real_anthropic
        real_anthropic = _real_anthropic
    except ImportError:
        pass

    class _FakeAnthropicModule:
        class Anthropic:
            def __init__(self, **kwargs):
                self.messages = _FakeMessages()

    with mock.patch.dict("sys.modules", {"anthropic": _FakeAnthropicModule}):
        adapter = AnthropicAdapter(api_key="test-key")
        result_obj = adapter.call(
            system="You are a summarizer.",
            user="## Unit: unit-live-test\n- status: closed",
            model="claude-haiku-4-5",
            max_tokens=1024,
        )

    assert result_obj.text == "cached narrative"
    assert len(captured_kwargs) == 1, "expected exactly one messages.create call"

    system_arg = captured_kwargs[0]["system"]
    assert isinstance(system_arg, list), (
        f"AnthropicAdapter system= must be a list, got {type(system_arg)}"
    )
    assert len(system_arg) == 1, f"expected one block, got {len(system_arg)}"
    block = system_arg[0]
    assert block.get("type") == "text", f"block type must be 'text', got {block.get('type')!r}"
    assert block.get("cache_control") == {"type": "ephemeral"}, (
        f"block must have cache_control={{'type': 'ephemeral'}}, got {block.get('cache_control')!r}"
    )
    assert "text" in block and block["text"], "block must have non-empty 'text'"
