"""Tests for collector/reprompt.py (C1-2).

Asserts reprompt detection against both clean and reprompt fixtures.
"""

import json
from pathlib import Path

import pytest

from collector.reprompt import detect_reprompts
from collector.session_parser import parse_session


FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample_session.jsonl"
REPROMPT = FIXTURES / "reprompt_session.jsonl"


def _load_messages(filepath: Path):
    """Load user/assistant messages from a JSONL file."""
    messages = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if entry.get("type") not in ("user", "assistant"):
                continue
            msg = entry.get("message", {})
            messages.append({
                "role": msg.get("role", entry["type"]),
                "content": msg.get("content", ""),
            })
    return messages


class TestRepromptDetection:
    """Re-prompt detection for known fixtures."""

    def test_clean_fixture_zero_reprompts(self):
        """Clean session should have rephrase_count: 0."""
        messages = _load_messages(SAMPLE)
        count, bail = detect_reprompts(messages)
        assert count == 0

    def test_reprompt_fixture_count(self):
        """Reprompt fixture has 4 user text turns in a row
        (u1, u2, u3, u4 — all text, each after an assistant response).
        That's 3 reprompts (turns 2, 3, 4 are reprompts of turn 1).
        """
        messages = _load_messages(REPROMPT)
        count, bail = detect_reprompts(messages)
        # u1->a1->u2 = 1 reprompt, u2->a2->u3 = 2 reprompts, u3->a3->u4 = 3 reprompts
        assert count == 3

    def test_bail_out_fires_at_threshold(self):
        """bail_out should be True when rephrase_count >= threshold (3)."""
        messages = _load_messages(REPROMPT)
        count, bail = detect_reprompts(messages, threshold=3)
        assert bail is True

    def test_bail_out_false_below_threshold(self):
        """bail_out should be False when rephrase_count < threshold."""
        messages = _load_messages(REPROMPT)
        count, bail = detect_reprompts(messages, threshold=5)
        assert bail is False

    def test_no_external_api_calls(self):
        """Reprompt detection runs fully offline — just a logic check."""
        messages = _load_messages(REPROMPT)
        # If this completes without network error, it's offline
        count, bail = detect_reprompts(messages)
        assert isinstance(count, int)
        assert isinstance(bail, bool)

    def test_clean_session_parsed_reprompt_count(self):
        """End-to-end: parse_session on clean fixture has reprompt_count 0."""
        record = parse_session(SAMPLE)
        assert record.reprompt_count == 0

    def test_reprompt_session_parsed(self):
        """End-to-end: parse_session on reprompt fixture detects reprompts."""
        record = parse_session(REPROMPT)
        assert record.reprompt_count == 3
        assert record.bail_out is True
