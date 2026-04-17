"""Tests for collector/session_parser.py (C1-1).

Asserts exact field values against the sample_session.jsonl fixture.
"""

import json
from pathlib import Path

import pytest

from collector.session_parser import SessionRecord, parse_session, SessionParseError


FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample_session.jsonl"


class TestParseSession:
    """Parser returns correct values for all fields against the fixture."""

    def test_session_uuid(self):
        record = parse_session(SAMPLE)
        assert record.session_uuid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_turn_count(self):
        """Turns are user text messages (not tool results).

        Fixture has: u1 (text), u6 (text) = 2 human text turns.
        u2, u3, u4, u5 are tool results only.
        """
        record = parse_session(SAMPLE)
        assert record.turn_count == 2

    def test_tool_call_count(self):
        """Fixture has 4 tool_use blocks: Read, Edit, Bash, Bash."""
        record = parse_session(SAMPLE)
        assert record.tool_call_count == 4

    def test_tool_failure_count(self):
        """Fixture has 1 tool_result with is_error=true (u4)."""
        record = parse_session(SAMPLE)
        assert record.tool_failure_count == 1

    def test_session_duration(self):
        """First timestamp: 10:00:00.000, last: 10:00:35.000 = 35 seconds.

        Note: queue-operation timestamps are included too; the earliest
        is at 10:00:00.000Z.
        """
        record = parse_session(SAMPLE)
        assert record.session_duration_seconds == pytest.approx(35.0, abs=1.0)

    def test_working_directory(self):
        record = parse_session(SAMPLE)
        assert record.working_directory == "/workspaces/my-project"

    def test_git_branch(self):
        """git_branch is extracted from JSONL metadata (gitBranch field)."""
        record = parse_session(SAMPLE)
        assert record.git_branch == "fix/my-bug"

    def test_raw_content_json_no_thinking(self):
        """raw_content_json must not contain any thinking blocks."""
        record = parse_session(SAMPLE)
        raw = json.loads(record.raw_content_json)
        for turn in raw:
            content = turn.get("content", "")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        assert block.get("type") != "thinking", (
                            "raw_content_json should not contain thinking blocks"
                        )

    def test_raw_content_json_no_tool_use(self):
        """raw_content_json must not contain any tool_use blocks."""
        record = parse_session(SAMPLE)
        raw = json.loads(record.raw_content_json)
        for turn in raw:
            content = turn.get("content", "")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        assert block.get("type") != "tool_use", (
                            "raw_content_json should not contain tool_use blocks"
                        )

    def test_raw_content_json_no_tool_result(self):
        """raw_content_json must not contain any tool_result blocks."""
        record = parse_session(SAMPLE)
        raw = json.loads(record.raw_content_json)
        for turn in raw:
            content = turn.get("content", "")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        assert block.get("type") != "tool_result", (
                            "raw_content_json should not contain tool_result blocks"
                        )

    def test_raw_content_json_preserves_text(self):
        """User and assistant text turns should be preserved."""
        record = parse_session(SAMPLE)
        raw = json.loads(record.raw_content_json)

        # Should contain at least the user text turns and assistant text turns
        texts = []
        for turn in raw:
            content = turn.get("content", "")
            if isinstance(content, str):
                texts.append(content)
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        texts.append(block["text"])

        assert "Please fix the bug in main.py" in texts
        assert "Thanks, looks good!" in texts

    def test_input_tokens(self):
        """Sum of input_tokens across all assistant messages: 100+200+300+400+150=1150."""
        record = parse_session(SAMPLE)
        assert record.input_tokens == 1150

    def test_output_tokens(self):
        """Sum of output_tokens across all assistant messages: 50+80+20+100+30=280."""
        record = parse_session(SAMPLE)
        assert record.output_tokens == 280

    def test_cache_creation_tokens(self):
        """Only a2 has cache_creation_input_tokens=100."""
        record = parse_session(SAMPLE)
        assert record.cache_creation_tokens == 100

    def test_cache_read_tokens(self):
        """a3=100, a4=200, a5=300 → total 600."""
        record = parse_session(SAMPLE)
        assert record.cache_read_tokens == 600

    def test_fast_mode_turns(self):
        """a2 and a4 have speed='fast' → 2 fast turns."""
        record = parse_session(SAMPLE)
        assert record.fast_mode_turns == 2

    def test_tokens_zero_when_no_usage(self, tmp_path):
        """Sessions without usage fields default to zero tokens."""
        session_file = tmp_path / "no_usage.jsonl"
        lines = [
            json.dumps({"type": "user", "message": {"role": "user", "content": "hi"},
                        "uuid": "x1", "timestamp": "2026-01-01T00:00:00.000Z",
                        "sessionId": "no-usage-session"}),
            json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "hello"},
                        "uuid": "x2", "timestamp": "2026-01-01T00:00:01.000Z",
                        "sessionId": "no-usage-session"}),
        ]
        session_file.write_text("\n".join(lines) + "\n")
        record = parse_session(session_file)
        assert record.input_tokens == 0
        assert record.output_tokens == 0
        assert record.cache_creation_tokens == 0
        assert record.cache_read_tokens == 0
        assert record.fast_mode_turns == 0

    def test_record_is_dataclass(self):
        record = parse_session(SAMPLE)
        assert isinstance(record, SessionRecord)


class TestParserErrors:
    """Parser raises named exceptions on malformed input."""

    def test_missing_file(self, tmp_path):
        with pytest.raises(SessionParseError, match="not found"):
            parse_session(tmp_path / "nonexistent.jsonl")

    def test_empty_file(self, tmp_path):
        empty = tmp_path / "empty.jsonl"
        empty.write_text("")
        with pytest.raises(SessionParseError, match="sessionId"):
            parse_session(empty)

    def test_malformed_json(self, tmp_path):
        bad = tmp_path / "bad.jsonl"
        bad.write_text('{"type": "user"}\n{not json\n')
        with pytest.raises(SessionParseError, match="Malformed JSON"):
            parse_session(bad)

    def test_truncated_file(self, tmp_path):
        truncated = tmp_path / "truncated.jsonl"
        truncated.write_text('{"type": "ai-title", "sessionId": "abc"')
        with pytest.raises(SessionParseError, match="Malformed JSON"):
            parse_session(truncated)


class TestGitBranchFallback:
    """git_branch is None when working directory is not a git repo."""

    def test_non_git_dir(self, tmp_path):
        """Create a minimal session with cwd pointing to a non-git dir."""
        session_file = tmp_path / "session.jsonl"
        lines = [
            json.dumps({
                "type": "user",
                "message": {"role": "user", "content": "hello"},
                "uuid": "x1",
                "timestamp": "2026-01-01T00:00:00.000Z",
                "cwd": str(tmp_path),
                "sessionId": "test-no-git",
            }),
            json.dumps({
                "type": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}]},
                "uuid": "x2",
                "timestamp": "2026-01-01T00:00:01.000Z",
                "sessionId": "test-no-git",
            }),
        ]
        session_file.write_text("\n".join(lines) + "\n")
        record = parse_session(session_file)
        assert record.git_branch is None


class TestSessionTimestamps:
    """Epic #17 Sub-Issue 2 (#35): session_started_at and session_ended_at."""

    def test_extracts_first_and_last_timestamp(self):
        record = parse_session(SAMPLE)
        # First timestamp in the fixture is 2026-01-15T10:00:00.000Z on a
        # queue-operation entry; last is 2026-01-15T10:00:35.000Z.
        assert record.session_started_at is not None
        assert record.session_started_at.startswith("2026-01-15T10:00:00")
        assert record.session_ended_at is not None
        assert record.session_ended_at.startswith("2026-01-15T10:00:35")

    def test_no_timestamps_returns_none(self, tmp_path):
        """A session file with zero parseable timestamps yields None/None."""
        session_file = tmp_path / "no-ts.jsonl"
        session_file.write_text(
            json.dumps({
                "type": "user",
                "message": {"role": "user", "content": "hello"},
                "sessionId": "ts-none",
            }) + "\n"
        )
        record = parse_session(session_file)
        assert record.session_started_at is None
        assert record.session_ended_at is None
