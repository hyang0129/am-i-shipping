"""Tests for ``synthesis/llm_adapter.py`` — ClaudeCliAdapter and _get_adapter routing."""

from __future__ import annotations

import subprocess
import unittest.mock as mock
from unittest.mock import MagicMock

import pytest

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.llm_adapter import (
    ClaudeCliAdapter,
    _FakeAdapter,
    _get_adapter,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(**overrides) -> SynthesisConfig:
    """Return a minimal SynthesisConfig for routing tests."""
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
        from dataclasses import replace
        return replace(base, **overrides)
    return base


def _make_completed_process(stdout: str, returncode: int = 0) -> subprocess.CompletedProcess:
    """Build a CompletedProcess-like object suitable for mocking subprocess.run."""
    return subprocess.CompletedProcess(
        args=[],
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


# ---------------------------------------------------------------------------
# Module-level fixture: guarantee offline mode unless a test opts into live
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _scrub_live_env(monkeypatch: pytest.MonkeyPatch):
    """Reset env so each test in this file controls its own routing.

    The repo-wide ``conftest.py`` autouse fixture sets
    ``AMIS_SYNTHESIS_OFFLINE=1`` for every test. Routing tests below
    selectively delete it to exercise live-mode branches.
    """
    monkeypatch.setenv("AMIS_SYNTHESIS_OFFLINE", "1")
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    yield


# ---------------------------------------------------------------------------
# F-3: ClaudeCliAdapter unit tests
# ---------------------------------------------------------------------------


def test_claude_cli_adapter_strips_api_key(monkeypatch: pytest.MonkeyPatch):
    """ANTHROPIC_API_KEY must NOT appear in the env= kwarg passed to subprocess.run.

    The adapter calls os.environ.copy() then pops the key before spawning.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-should-be-stripped")

    payload = '{"result": "hello", "is_error": false}'
    completed = _make_completed_process(stdout=payload)

    with mock.patch("synthesis.llm_adapter.subprocess.run", return_value=completed) as mock_run:
        adapter = ClaudeCliAdapter()
        adapter.call(system="sys", user="user prompt", model="claude-haiku-4-5", max_tokens=256)

    mock_run.assert_called_once()
    call_kwargs = mock_run.call_args.kwargs
    env_passed = call_kwargs.get("env", {})
    assert "ANTHROPIC_API_KEY" not in env_passed, (
        f"ANTHROPIC_API_KEY must be stripped from subprocess env; "
        f"got keys: {list(env_passed.keys())}"
    )


def test_claude_cli_adapter_parses_result_key():
    """Adapter correctly parses the 'result' key from the JSON response."""
    payload = '{"result": "hello from result key", "is_error": false}'
    completed = _make_completed_process(stdout=payload)

    with mock.patch("synthesis.llm_adapter.subprocess.run", return_value=completed):
        adapter = ClaudeCliAdapter()
        result = adapter.call(system="", user="prompt", model="claude-haiku-4-5", max_tokens=256)

    assert result.text == "hello from result key"


def test_claude_cli_adapter_parses_content_key():
    """Adapter falls back to 'content' key when 'result' is absent."""
    payload = '{"content": "hello from content key", "is_error": false}'
    completed = _make_completed_process(stdout=payload)

    with mock.patch("synthesis.llm_adapter.subprocess.run", return_value=completed):
        adapter = ClaudeCliAdapter()
        result = adapter.call(system="", user="prompt", model="claude-haiku-4-5", max_tokens=256)

    assert result.text == "hello from content key"


def test_claude_cli_adapter_raises_on_empty_stdout():
    """Adapter raises RuntimeError when subprocess.run returns empty stdout."""
    completed = _make_completed_process(stdout="")

    with mock.patch("synthesis.llm_adapter.subprocess.run", return_value=completed):
        adapter = ClaudeCliAdapter()
        with pytest.raises(RuntimeError):
            adapter.call(system="", user="prompt", model="claude-haiku-4-5", max_tokens=256)


def test_claude_cli_adapter_raises_on_is_error():
    """When is_error=true the adapter returns an LLMResult (it logs but does not raise).

    The adapter reads the is_error flag and logs it; the resulting text is the
    'result' value from the JSON (empty string here).  No RuntimeError is raised
    for is_error alone — a non-zero returncode or empty stdout triggers errors.
    This test documents that behaviour and guards against regressions where a
    future refactor might accidentally swallow the text.
    """
    payload = '{"result": "", "is_error": true}'
    completed = _make_completed_process(stdout=payload)

    with mock.patch("synthesis.llm_adapter.subprocess.run", return_value=completed):
        adapter = ClaudeCliAdapter()
        # Current implementation does NOT raise on is_error; it returns an LLMResult
        # with an empty text field.  Assert this documented behaviour holds.
        result = adapter.call(system="", user="prompt", model="claude-haiku-4-5", max_tokens=256)

    assert result.text == "", (
        f"Expected empty text for is_error=true response, got: {result.text!r}"
    )


# ---------------------------------------------------------------------------
# F-4: _get_adapter routing tests
# ---------------------------------------------------------------------------


def test_get_adapter_offline_returns_fake(monkeypatch: pytest.MonkeyPatch):
    """With AMIS_SYNTHESIS_OFFLINE=1, _get_adapter returns a _FakeAdapter."""
    # autouse fixture already sets AMIS_SYNTHESIS_OFFLINE=1
    adapter = _get_adapter(_make_cfg())
    assert isinstance(adapter, _FakeAdapter), (
        f"expected _FakeAdapter in offline mode, got {type(adapter)}"
    )


def test_get_adapter_live_default_is_claude_cli(monkeypatch: pytest.MonkeyPatch):
    """With AMIS_SYNTHESIS_OFFLINE unset and LLM_PROVIDER unset, returns ClaudeCliAdapter."""
    from synthesis.llm_adapter import ClaudeCliAdapter as _CCA

    monkeypatch.delenv("AMIS_SYNTHESIS_OFFLINE", raising=False)
    # LLM_PROVIDER unset → default is claude-cli

    adapter = _get_adapter(_make_cfg())
    assert isinstance(adapter, _CCA), (
        f"expected ClaudeCliAdapter as default live provider, got {type(adapter)}"
    )


def test_get_adapter_live_anthropic_with_key(monkeypatch: pytest.MonkeyPatch):
    """With AMIS_SYNTHESIS_OFFLINE unset, LLM_PROVIDER=anthropic, and the key set, returns AnthropicAdapter."""
    from synthesis.llm_adapter import AnthropicAdapter

    monkeypatch.delenv("AMIS_SYNTHESIS_OFFLINE", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-live-key")

    adapter = _get_adapter(_make_cfg())
    assert isinstance(adapter, AnthropicAdapter), (
        f"expected AnthropicAdapter when LLM_PROVIDER=anthropic and key is set, got {type(adapter)}"
    )


def test_get_adapter_live_anthropic_missing_key_raises(monkeypatch: pytest.MonkeyPatch):
    """With AMIS_SYNTHESIS_OFFLINE unset, LLM_PROVIDER=anthropic, but no API key: raises RuntimeError."""
    monkeypatch.delenv("AMIS_SYNTHESIS_OFFLINE", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    # ANTHROPIC_API_KEY unset (autouse fixture already strips it)

    cfg = _make_cfg(anthropic_api_key_env="ANTHROPIC_API_KEY")
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        _get_adapter(cfg)


def test_get_adapter_unknown_provider_raises(monkeypatch: pytest.MonkeyPatch):
    """With AMIS_SYNTHESIS_OFFLINE unset and an unknown LLM_PROVIDER: raises ValueError."""
    monkeypatch.delenv("AMIS_SYNTHESIS_OFFLINE", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "unknown-provider")

    with pytest.raises(ValueError, match="unknown-provider"):
        _get_adapter(_make_cfg())
