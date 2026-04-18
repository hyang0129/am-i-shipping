"""LLM adapter layer for am-i-shipping synthesis (Issue #65).

Ported from ``video_agent_long/tools/llm_adapter.py``. Routes synthesis
LLM calls through one of three backends selected at runtime:

* **offline** (``AMIS_SYNTHESIS_LIVE`` unset / falsy) →
  :class:`_FakeAdapter` — wraps :class:`FakeAnthropicClient`; no API
  calls, deterministic output, no credentials required.

* **claude-cli** (``AMIS_SYNTHESIS_LIVE=1``, ``LLM_PROVIDER=claude-cli``,
  the default when live) → :class:`ClaudeCliAdapter` — shells out to the
  ``claude`` CLI via ``subprocess``. Uses the authenticated session inside
  the running Claude Code instance; does NOT require ``ANTHROPIC_API_KEY``
  in the subprocess environment (the key is stripped to prevent leakage).

* **anthropic** (``AMIS_SYNTHESIS_LIVE=1``, ``LLM_PROVIDER=anthropic``) →
  :class:`AnthropicAdapter` — calls the Anthropic SDK directly. The system
  prompt is wrapped in a ``cache_control: ephemeral`` block so repeated
  weekly synthesis runs pay a cache-read price on the static context.

Call-site interface is uniform across all three backends::

    adapter = _get_adapter(config)
    result  = adapter.call(system, user, model, max_tokens)
    text    = result.text

Environment variables
---------------------
``AMIS_SYNTHESIS_LIVE``
    Any truthy string enables live mode. Unset or empty → offline.
``LLM_PROVIDER``
    ``claude-cli`` (default) or ``anthropic``. Only read in live mode.
``LINUX_CLAUDE_CLI_PATH``
    Override path to the ``claude`` binary on Linux/WSL. Supports glob
    wildcards with version-aware sorting for versioned VS Code extension
    paths (e.g. ``/ext/anthropic.claude-code-*/node_modules/.bin/claude``).
``NODE_BINARY_DIR``
    Prepended to ``PATH`` in the subprocess environment so the ``claude``
    CLI can locate ``node``.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from am_i_shipping.config_loader import SynthesisConfig


@dataclass
class LLMResult:
    text: str
    cost_usd: float
    input_tokens: int = 0
    output_tokens: int = 0
    input_chars: int = 0
    output_chars: int = 0


class LLMAdapter(Protocol):
    def call(
        self,
        system: str,
        user: str,
        model: str,
        max_tokens: int,
    ) -> LLMResult: ...


# Pricing per million tokens: {model_key: (input_rate, output_rate)}
_ANTHROPIC_RATES: dict[str, tuple[float, float]] = {
    "haiku": (0.80, 4.00),
    "sonnet": (3.00, 15.00),
    "opus": (15.00, 75.00),
}


def _anthropic_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    model_lower = model.lower()
    if "haiku" in model_lower:
        rates = _ANTHROPIC_RATES["haiku"]
    elif "opus" in model_lower:
        rates = _ANTHROPIC_RATES["opus"]
    else:
        rates = _ANTHROPIC_RATES["sonnet"]
    return (input_tokens * rates[0] + output_tokens * rates[1]) / 1_000_000


class AnthropicAdapter:
    """Direct Anthropic SDK adapter.

    Wraps the system prompt in a ``cache_control: ephemeral`` block so
    the static synthesis context is cached across repeated weekly runs.
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def call(self, system: str, user: str, model: str, max_tokens: int) -> LLMResult:
        import anthropic

        client = anthropic.Anthropic(api_key=self._api_key)
        system_param: str | list = (
            [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}]
            if system
            else system
        )
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system_param,
            messages=[{"role": "user", "content": user}],
        )
        if not resp.content:
            raise RuntimeError(
                f"Anthropic API returned empty content list (model={model})"
            )
        block = resp.content[0]
        block_type = getattr(block, "type", "text")  # default "text" for mock objects without type
        if block_type != "text":
            raise RuntimeError(
                f"Anthropic API returned unexpected first content block type "
                f"(model={model}, type={block_type})"
            )
        text = block.text
        cost = _anthropic_cost(model, resp.usage.input_tokens, resp.usage.output_tokens)
        return LLMResult(
            text=text,
            cost_usd=cost,
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            input_chars=len(system) + len(user),
            output_chars=len(text),
        )


def _version_sort_key(path: str) -> tuple[int, ...]:
    """Numeric version sort for anthropic.claude-code-X.Y.Z extension paths."""
    m = re.search(r"anthropic\.claude-code-(\d+(?:\.\d+)*)", path)
    if m:
        return (1, *[int(x) for x in m.group(1).split(".")])
    return (0,)


def resolve_cli_path(raw: str) -> str:
    """Expand a glob pattern and return the highest-versioned match."""
    if any(c in raw for c in ("*", "?", "[")):
        matches = sorted(glob.glob(raw), key=_version_sort_key)
        if not matches:
            raise FileNotFoundError(f"Glob pattern '{raw}' matched no files")
        if not os.path.isfile(matches[-1]):
            raise FileNotFoundError(f"Resolved path '{matches[-1]}' is not a file")
        return matches[-1]
    return raw


def _claude_cmd() -> str:
    import sys

    if sys.platform != "win32":
        linux_explicit = os.environ.get("LINUX_CLAUDE_CLI_PATH")
        if linux_explicit:
            if os.path.isfile(linux_explicit):
                return linux_explicit
            if "anthropic.claude-code-" in linux_explicit:
                glob_pattern = re.sub(
                    r"anthropic\.claude-code-[\d.]+-",
                    "anthropic.claude-code-*-",
                    linux_explicit,
                )
                try:
                    resolved = resolve_cli_path(glob_pattern)
                    logger.warning(
                        "[WARN] LINUX_CLAUDE_CLI_PATH '%s' not found; "
                        "auto-resolved to '%s' via glob",
                        linux_explicit,
                        resolved,
                    )
                    return resolved
                except FileNotFoundError:
                    pass
                which_result = shutil.which("claude")
                if which_result:
                    logger.warning(
                        "[WARN] LINUX_CLAUDE_CLI_PATH '%s' not found and glob failed; "
                        "falling back to shutil.which: '%s'",
                        linux_explicit,
                        which_result,
                    )
                    return which_result
                raise FileNotFoundError(
                    f"LINUX_CLAUDE_CLI_PATH '{linux_explicit}' does not exist, "
                    f"glob pattern '{glob_pattern}' matched no files, "
                    "and 'claude' was not found on PATH."
                )
            return resolve_cli_path(linux_explicit)
        return shutil.which("claude") or "claude"

    explicit = os.environ.get("CLAUDE_CLI_PATH")
    if explicit:
        return explicit
    resolved = shutil.which("claude.cmd") or shutil.which("claude")
    if resolved:
        return resolved
    return "claude.cmd"


class ClaudeCliAdapter:
    """Subprocess adapter that shells out to the ``claude`` CLI.

    Uses the authenticated Claude Code session; strips ``ANTHROPIC_API_KEY``
    from the subprocess env so the key is never forwarded to the child.
    """

    def call(self, system: str, user: str, model: str, max_tokens: int) -> LLMResult:
        import sys
        import tempfile

        env = os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)
        node_dir = os.environ.get("NODE_BINARY_DIR")
        if node_dir:
            env["PATH"] = node_dir + os.pathsep + env.get("PATH", "")

        logger.debug(
            "[LLM] claude-cli call: model=%s max_tokens=%d system_len=%d user_len=%d",
            model, max_tokens, len(system), len(user),
        )

        tmp_files: list[str] = []
        try:
            cmd = [_claude_cmd(), "--output-format", "json", "-p"]

            if sys.platform == "win32":
                # Windows: cmd.exe 8191-char limit requires temp files for large prompts.
                if system:
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".txt", encoding="utf-8", delete=False
                    ) as f:
                        f.write(system)
                        tmp_files.append(f.name)
                    cmd += ["--system-prompt-file", tmp_files[-1]]

                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".txt", encoding="utf-8", delete=False
                ) as f:
                    f.write(user)
                    tmp_files.append(f.name)
                user_prompt_file = tmp_files[-1]
                stdin_input = (
                    f"Read the file at {user_prompt_file} and respond to it "
                    "as your direct instruction."
                )
                cmd += ["--allowedTools", "Read"]
            else:
                if system:
                    cmd += ["--system-prompt", system]
                stdin_input = user

            result = subprocess.run(
                cmd,
                input=stdin_input,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=600,
                env=env,
            )
        finally:
            for f in tmp_files:
                try:
                    os.unlink(f)
                except OSError:
                    pass

        if result.returncode != 0:
            raise RuntimeError(f"[ERROR] claude CLI failed: {result.stderr.strip()}")
        if not result.stdout.strip():
            raise RuntimeError(
                f"[ERROR] claude CLI returned empty stdout (stderr={result.stderr.strip()!r})"
            )
        data = json.loads(result.stdout)
        keys = list(data.keys())
        stop_reason = data.get("stop_reason", data.get("stopReason", "<absent>"))
        is_error = data.get("is_error", False)
        usage = data.get("usage", {})
        raw_input_tokens = usage.get("input_tokens", data.get("input_tokens", "<absent>"))
        raw_output_tokens = usage.get("output_tokens", data.get("output_tokens", "<absent>"))
        input_tokens = raw_input_tokens if isinstance(raw_input_tokens, int) else 0
        output_tokens = raw_output_tokens if isinstance(raw_output_tokens, int) else 0
        text = data.get("result", data.get("content", ""))
        cost = float(data.get("total_cost_usd", data.get("cost_usd", 0.0)))
        logger.debug(
            "[LLM] claude-cli response: keys=%s stop_reason=%r is_error=%s "
            "tokens=(%s in, %s out) text_len=%d text_preview=%r",
            keys, stop_reason, is_error,
            raw_input_tokens, raw_output_tokens,
            len(text), text[:200],
        )
        return LLMResult(
            text=text,
            cost_usd=cost,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            input_chars=len(system) + len(user),
            output_chars=len(text),
        )


class _FakeAdapter:
    """Offline adapter that wraps FakeAnthropicClient for tests and dry-runs."""

    def call(self, system: str, user: str, model: str, max_tokens: int) -> LLMResult:
        from synthesis.fake_client import FakeAnthropicClient

        resp = FakeAnthropicClient().messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = resp.content[0].text
        return LLMResult(text=text, cost_usd=0.0, input_chars=len(system) + len(user), output_chars=len(text))


def _get_adapter(config: "SynthesisConfig") -> LLMAdapter:
    """Return the appropriate adapter based on ``AMIS_SYNTHESIS_LIVE`` and ``LLM_PROVIDER``.

    Offline (``AMIS_SYNTHESIS_LIVE`` unset / falsy):
        Returns :class:`_FakeAdapter` regardless of ``LLM_PROVIDER``.

    Live (``AMIS_SYNTHESIS_LIVE=1``):
        ``LLM_PROVIDER=claude-cli`` (default) → :class:`ClaudeCliAdapter`
        ``LLM_PROVIDER=anthropic``            → :class:`AnthropicAdapter`
    """
    if not os.environ.get("AMIS_SYNTHESIS_LIVE"):
        return _FakeAdapter()

    provider = os.environ.get("LLM_PROVIDER", "claude-cli")

    if provider == "claude-cli":
        return ClaudeCliAdapter()

    if provider == "anthropic":
        api_key = os.environ.get(config.anthropic_api_key_env)
        if not api_key:
            raise RuntimeError(
                f"AMIS_SYNTHESIS_LIVE is set but {config.anthropic_api_key_env} "
                "is empty — cannot call the Anthropic API"
            )
        return AnthropicAdapter(api_key=api_key)

    raise ValueError(
        f"[ERROR] Unknown LLM_PROVIDER={provider!r}. Expected: claude-cli, anthropic"
    )
