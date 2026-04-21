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


class TestSkillInvocationDetection:
    """Issue #86: <command-name>/xxx</command-name> tags in user-turn content
    become skill_invocations rows. Detection runs before _strip_content_blocks
    so the tags remain visible even though tool_use / tool_result blocks are
    stripped from raw_content_json."""

    def _write_session(self, path: Path, entries: list) -> None:
        path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")

    def test_plain_string_content_tag_detected(self, tmp_path):
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "user",
                "message": {"role": "user", "content": "<command-name>/refine-issue</command-name> do thing"},
                "uuid": "u1",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "sessionId": "skill-s1",
            },
        ])
        record = parse_session(session_file)
        assert len(record.skill_invocations) == 1
        inv = record.skill_invocations[0]
        assert inv["skill_name"] == "refine-issue"
        assert inv["invoked_at"] == "2026-04-20T10:00:00.000Z"
        assert inv["invocation_index"] == 0

    def test_structured_content_tag_detected(self, tmp_path):
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "context here"},
                        {"type": "text", "text": "<command-name>/resolve-issue</command-name> 42"},
                    ],
                },
                "uuid": "u1",
                "timestamp": "2026-04-20T11:00:00.000Z",
                "sessionId": "skill-s2",
            },
        ])
        record = parse_session(session_file)
        assert [i["skill_name"] for i in record.skill_invocations] == ["resolve-issue"]

    def test_assistant_turn_ignored(self, tmp_path):
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "I used <command-name>/review-fix</command-name>"}]},
                "uuid": "a1",
                "timestamp": "2026-04-20T12:00:00.000Z",
                "sessionId": "skill-s3",
            },
            # Must have a user turn for sessionId extraction + no SessionParseError
            {
                "type": "user",
                "message": {"role": "user", "content": "plain"},
                "uuid": "u1",
                "timestamp": "2026-04-20T12:00:05.000Z",
                "sessionId": "skill-s3",
            },
        ])
        record = parse_session(session_file)
        assert record.skill_invocations == []

    def test_multiple_tags_monotonic_index(self, tmp_path):
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "user",
                "message": {"role": "user", "content": "<command-name>/refine-issue</command-name>"},
                "uuid": "u1",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "sessionId": "skill-s4",
            },
            {
                "type": "user",
                "message": {"role": "user", "content": "<command-name>/resolve-issue</command-name>"},
                "uuid": "u2",
                "timestamp": "2026-04-20T11:00:00.000Z",
                "sessionId": "skill-s4",
            },
            {
                "type": "user",
                "message": {"role": "user", "content": "<command-name>/review-fix</command-name>"},
                "uuid": "u3",
                "timestamp": "2026-04-20T12:00:00.000Z",
                "sessionId": "skill-s4",
            },
        ])
        record = parse_session(session_file)
        names = [i["skill_name"] for i in record.skill_invocations]
        indices = [i["invocation_index"] for i in record.skill_invocations]
        assert names == ["refine-issue", "resolve-issue", "review-fix"]
        assert indices == [0, 1, 2]

    def test_target_resolved_from_gh_event(self, tmp_path):
        """When /refine-issue is followed by a resolved gh issue comment event,
        target_repo and target_ref are populated."""
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "user",
                "message": {"role": "user", "content": "<command-name>/refine-issue</command-name> 86"},
                "uuid": "u1",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "sessionId": "skill-s5",
            },
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "t1", "name": "Bash",
                         "input": {"command": "gh issue comment 86 --repo org/repo --body x"}},
                    ],
                },
                "uuid": "a1",
                "timestamp": "2026-04-20T10:00:05.000Z",
                "sessionId": "skill-s5",
            },
        ])
        record = parse_session(session_file)
        assert len(record.skill_invocations) == 1
        inv = record.skill_invocations[0]
        assert inv["target_repo"] == "org/repo"
        assert inv["target_ref"] == "86"

    def test_skill_tag_survives_content_stripping(self, tmp_path):
        """The skill tag must be detected even though raw_content_json may
        strip tool_use blocks. The tag itself lives in user text, so it's
        preserved — this test asserts that detection runs on the pre-strip
        content and that raw_content_json still contains the text turn."""
        session_file = tmp_path / "s.jsonl"
        self._write_session(session_file, [
            {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "tool_result", "tool_use_id": "t0", "content": "stripped"},
                        {"type": "text", "text": "<command-name>/refine-issue</command-name>"},
                    ],
                },
                "uuid": "u1",
                "timestamp": "2026-04-20T10:00:00.000Z",
                "sessionId": "skill-s6",
            },
        ])
        record = parse_session(session_file)
        assert len(record.skill_invocations) == 1
        assert record.skill_invocations[0]["skill_name"] == "refine-issue"
