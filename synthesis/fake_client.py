"""Deterministic offline Anthropic client (Epic #17 — Issue #39).

Mimics just enough of the ``anthropic`` SDK's ``messages.create`` surface
for :mod:`synthesis.weekly` to exercise the full synthesis pipeline
without a network call or an API key.

Why in-tree instead of monkeypatching the real SDK
---------------------------------------------------
The weekly runner picks between the live SDK and this fake based on the
``AMIS_SYNTHESIS_LIVE`` env var. Co-locating the fake next to the
production call site means:

* The fake's return shape is type-compatible with
  :class:`anthropic.types.Message` at the narrow surface
  :mod:`synthesis.weekly` consumes (``.content[0].text``) — so swapping
  ``AMIS_SYNTHESIS_LIVE=1`` on flips to the real SDK without any
  branching in callers beyond the client selection itself.
* The deterministic payload is the anchor for the golden snapshot test
  in ``tests/fixtures/synthesis/expected_retrospective.md``: re-running
  against ``AMIS_SYNTHESIS_LIVE`` unset must produce byte-identical
  output.

The Markdown returned below conforms to the epic template:
Velocity Trend / Unit Summary Table / Outlier Units / Abandoned Units
/ Dark Time / Clarifying Questions. Exactly two clarifying questions,
no Recommendations section (ADR: synthesis asks questions; the
experiment loop generates recommendations).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List


# ---------------------------------------------------------------------------
# SDK-shaped response objects
# ---------------------------------------------------------------------------


@dataclass
class FakeTextBlock:
    """Minimal stand-in for ``anthropic.types.TextBlock``."""

    text: str
    type: str = "text"


@dataclass
class FakeUsage:
    """Minimal stand-in for ``anthropic.types.Usage``.

    The live SDK returns rich token accounting here. Tests currently
    only need the fields to exist so call sites that log usage do not
    ``AttributeError``. All counters are zero in offline mode.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0


@dataclass
class FakeMessage:
    """Minimal stand-in for ``anthropic.types.Message``.

    The ``model``, ``role`` and ``stop_reason`` values mirror what the
    live SDK would return for a simple synthesis call, so offline output
    renders realistically in logs.
    """

    content: List[FakeTextBlock]
    model: str = "fake-claude"
    role: str = "assistant"
    stop_reason: str = "end_turn"
    usage: FakeUsage = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.usage is None:
            self.usage = FakeUsage()


# ---------------------------------------------------------------------------
# The canned retrospective Markdown
# ---------------------------------------------------------------------------
#
# This Markdown is the anchor for the golden snapshot
# (``tests/fixtures/synthesis/expected_retrospective.md``). Any change to
# this constant is a breaking change to that snapshot — update both in
# the same commit or the snapshot test will fail.
#
# Content invariants enforced by tests / the epic ADR:
#   * Headings present: Velocity Trend, Unit Summary Table,
#     Outlier Units, Abandoned Units, Dark Time, Clarifying Questions.
#   * Exactly two lines matching ``^\d+\.`` under Clarifying Questions
#     (the "≤2 total" constraint — two is the ceiling, not two per unit).
#   * No "Recommendations" heading anywhere.


_FAKE_RETROSPECTIVE_MD = """# Weekly Retrospective

## Velocity Trend

No historical baseline available yet; this is the first synthesised
week. Record this week's per-unit elapsed_days and total_reprompts as
the anchor for next week's delta.

## Unit Summary Table

| unit_id | root_node | elapsed_days | dark_time_pct | total_reprompts | review_cycles | status |
|---------|-----------|--------------|---------------|-----------------|---------------|--------|
| (populated from units table for the given week_start) | | | | | | |

## Outlier Units

Outlier units this week are those whose metric value exceeds
``median + 2sigma`` across the week's population. Empty list means no
unit breached the threshold — not that the pass did not run.

## Abandoned Units

Units with no graph_nodes event within the last 14 days and no open
issue/PR are flagged as abandoned. The abandonment flag is a hint for
triage, not a prescription.

## Dark Time

Dark time is the fraction of the unit's wall-clock span during which
no session was active. Single-session units report 0.0 by definition
(see ADR Decision 3).

## Clarifying Questions

1. Of the flagged outlier units, which one surprises you most, and what
   precondition do you think was missing when the work started?
2. For the abandoned units, is the plan still valid but deprioritised,
   or did the motivation evaporate once scoping revealed the true cost?
"""


# ---------------------------------------------------------------------------
# Public SDK-shaped facade
# ---------------------------------------------------------------------------


class FakeMessages:
    """Stand-in for ``anthropic.Anthropic().messages``.

    Only ``create`` is implemented — the synthesis runner never touches
    anything else. Unknown keyword arguments are accepted and ignored so
    future SDK additions (e.g. ``metadata``) do not break offline mode.
    """

    def create(
        self,
        *,
        model: str,
        max_tokens: int,
        system: Any,
        messages: list,
        **kwargs: Any,
    ) -> FakeMessage:
        # Signature is keyword-only to match the real SDK and to make
        # incompatible call sites fail loudly at call time rather than
        # silently ignoring a dropped argument.
        _ = (model, max_tokens, system, messages, kwargs)
        return FakeMessage(content=[FakeTextBlock(text=_FAKE_RETROSPECTIVE_MD)])


class FakeAnthropicClient:
    """Stand-in for ``anthropic.Anthropic``.

    Construct with no args. The ``messages`` attribute mirrors the real
    client's namespace so call sites do not need a type switch.
    """

    def __init__(self) -> None:
        self.messages = FakeMessages()


__all__ = [
    "FakeAnthropicClient",
    "FakeMessages",
    "FakeMessage",
    "FakeTextBlock",
    "FakeUsage",
]
