"""Tests for synthesis.coverage (Issue #70).

One test class per acceptance scenario in the refined spec. Fixtures mirror
the style of ``tests/test_backfill_session_timestamps.py``: small JSONL
builders + bare-row inserts.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from am_i_shipping.db import init_sessions_db
from synthesis.coverage import (
    _classify_fill,
    _extract_jsonl_refs,
    _jsonl_has_text_turns,
    backfill_full,
    backfill_partial,
    collect_coverage,
    run_coverage,
)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _write_jsonl(
    path: Path,
    session_uuid: str,
    entries: list[dict],
) -> None:
    """Write an arbitrary list of JSONL entries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for entry in entries:
        entry = dict(entry)  # shallow copy so tests can reuse dicts
        entry.setdefault("sessionId", session_uuid)
        lines.append(json.dumps(entry))
    path.write_text("\n".join(lines) + "\n")


def _text_turn(
    role: str = "user",
    text: str = "hello",
    timestamp: str = "2026-04-15T10:00:00Z",
) -> dict:
    return {
        "type": role,
        "timestamp": timestamp,
        "message": {"role": role, "content": [{"type": "text", "text": text}]},
    }


def _tool_use_turn(
    command: str,
    timestamp: str = "2026-04-15T10:05:00Z",
) -> dict:
    return {
        "type": "assistant",
        "timestamp": timestamp,
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "tool_xyz",
                    "name": "Bash",
                    "input": {"command": command},
                }
            ],
        },
    }


def _insert_session(
    db: Path,
    session_uuid: str,
    raw_content_json,
    session_started_at: str | None = None,
) -> None:
    """Insert a session row with a specified raw_content_json value."""
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            """
            INSERT INTO sessions (
                session_uuid, turn_count, tool_call_count, tool_failure_count,
                reprompt_count, bail_out, session_duration_seconds,
                working_directory, git_branch, raw_content_json,
                input_tokens, output_tokens, cache_creation_tokens,
                cache_read_tokens, fast_mode_turns, session_started_at
            ) VALUES (?, 1, 0, 0, 0, 0, 0.0, NULL, NULL, ?, 0, 0, 0, 0, 0, ?)
            """,
            (session_uuid, raw_content_json, session_started_at),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def db_and_projects(tmp_path):
    db = tmp_path / "sessions.db"
    init_sessions_db(db)
    projects = tmp_path / "projects"
    projects.mkdir()
    return db, projects


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------


class TestClassifyFill:
    def test_null(self):
        assert _classify_fill(None) == "null"

    def test_empty_array(self):
        assert _classify_fill("[]") == "empty"
        assert _classify_fill("  []  ") == "empty"

    def test_empty_array_with_inner_whitespace(self):
        # A future serializer (e.g. indent=2) could produce these — they must
        # still classify as empty so the diagnostic isn't silently defeated.
        assert _classify_fill("[ ]") == "empty"
        assert _classify_fill("[\n]") == "empty"

    def test_nonempty(self):
        assert _classify_fill('[{"role":"user"}]') == "nonempty"

    def test_malformed_json_is_nonempty(self):
        # Misclassifying malformed JSON as empty would hide it from the
        # parser-bug diagnostic list.
        assert _classify_fill("not json") == "nonempty"


class TestJsonlHasTextTurns:
    def test_true_for_text(self, tmp_path):
        p = tmp_path / "a.jsonl"
        _write_jsonl(p, "uuid-a", [_text_turn(text="hi")])
        assert _jsonl_has_text_turns(p) is True

    def test_false_for_tool_only(self, tmp_path):
        p = tmp_path / "b.jsonl"
        _write_jsonl(p, "uuid-b", [_tool_use_turn("ls")])
        assert _jsonl_has_text_turns(p) is False


class TestExtractJsonlRefs:
    def test_hash_and_url(self, tmp_path):
        p = tmp_path / "c.jsonl"
        _write_jsonl(
            p, "uuid-c",
            [_text_turn(text="see #42 and https://github.com/a/b/pull/99")],
        )
        refs = _extract_jsonl_refs(p)
        keys = {r.bucket_key() for r in refs}
        assert "#42" in keys
        assert "a/b#99" in keys

    def test_gh_cli_inside_tool_use(self, tmp_path):
        p = tmp_path / "d.jsonl"
        _write_jsonl(
            p, "uuid-d",
            [_tool_use_turn("gh issue view 70 --repo owner/repo")],
        )
        refs = _extract_jsonl_refs(p)
        keys = {r.bucket_key() for r in refs}
        assert "owner/repo#70" in keys


# ---------------------------------------------------------------------------
# Scenario tests
# ---------------------------------------------------------------------------


class TestDefaultReport:
    """Scenario: `am-synthesize coverage` default report mode."""

    def test_counts_and_buckets(self, db_and_projects):
        db, projects = db_and_projects

        # Row 1: NULL. JSONL present.
        _insert_session(db, "uuid-null", None, "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj-A" / "null.jsonl", "uuid-null",
            [_text_turn(text="pre-migration text")],
        )
        # Row 2: "[]" with text turns in JSONL (parser-bug candidate).
        _insert_session(db, "uuid-empty-text", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj-A" / "empty-text.jsonl", "uuid-empty-text",
            [_text_turn(text="real text")],
        )
        # Row 3: "[]" with no text turns (degenerate).
        _insert_session(db, "uuid-empty-degen", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj-B" / "empty-degen.jsonl", "uuid-empty-degen",
            [_tool_use_turn("ls")],
        )
        # Row 4: non-empty.
        _insert_session(
            db, "uuid-good",
            '[{"role":"user","content":"hi"}]',
            "2026-04-15T10:00:00Z",
        )
        _write_jsonl(
            projects / "proj-B" / "good.jsonl", "uuid-good",
            [_text_turn(text="hi")],
        )
        # Row 5: JSONL on disk, no DB row (unprocessed).
        _write_jsonl(
            projects / "proj-C" / "unprocessed.jsonl", "uuid-unprocessed",
            [_text_turn(text="new")],
        )

        report = collect_coverage(db, projects, week_start="monday")

        assert report.total == 4
        assert report.null_count == 1
        assert report.empty_count == 2
        assert report.nonempty_count == 1
        # Four diagnostic lists — sizes.
        assert len(report.unprocessed_jsonls) == 1
        assert len(report.orphan_db_rows) == 0
        assert len(report.empty_but_jsonl_has_text) == 1
        assert len(report.empty_and_jsonl_truly_empty) == 1
        # Per-project bucketing exists.
        assert "proj-A" in report.per_project
        assert "proj-B" in report.per_project
        # No DB rows were changed — verify raw_content_json is untouched.
        conn = sqlite3.connect(str(db))
        try:
            rows = dict(conn.execute(
                "SELECT session_uuid, raw_content_json FROM sessions"
            ).fetchall())
        finally:
            conn.close()
        assert rows["uuid-null"] is None
        assert rows["uuid-empty-text"] == "[]"


class TestJsonOutput:
    """Scenario: `--json` emits a parseable JSON document."""

    def test_json_deterministic(self, db_and_projects):
        db, projects = db_and_projects
        _insert_session(db, "uuid-a", '[{"role":"user"}]', "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj-A" / "a.jsonl", "uuid-a",
            [_text_turn(text="hi")],
        )
        r1 = collect_coverage(db, projects, week_start="monday").to_json()
        r2 = collect_coverage(db, projects, week_start="monday").to_json()
        assert r1 == r2  # byte-identical under unchanged inputs
        parsed = json.loads(r1)
        assert parsed["total"] == 1
        assert parsed["nonempty_count"] == 1


class TestPartialBackfill:
    """Scenario: `--backfill` populates only NULL rows; idempotent."""

    def test_updates_only_null_rows(self, db_and_projects):
        db, projects = db_and_projects

        _insert_session(db, "uuid-null", None, None)
        _write_jsonl(
            projects / "proj" / "null.jsonl", "uuid-null",
            [_text_turn(text="recoverable")],
        )
        _insert_session(db, "uuid-empty", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "empty.jsonl", "uuid-empty",
            [_text_turn(text="also recoverable")],
        )
        _insert_session(
            db, "uuid-good",
            '[{"role":"user","content":"prior"}]',
            "2026-04-15T10:00:00Z",
        )
        _write_jsonl(
            projects / "proj" / "good.jsonl", "uuid-good",
            [_text_turn(text="new text")],
        )

        summary = backfill_partial(db, projects)
        assert summary.updated == 1
        assert summary.skipped_missing_jsonl == 0

        conn = sqlite3.connect(str(db))
        try:
            rows = dict(conn.execute(
                "SELECT session_uuid, raw_content_json FROM sessions"
            ).fetchall())
        finally:
            conn.close()
        assert rows["uuid-null"] is not None  # populated
        assert rows["uuid-null"] != "[]"
        assert rows["uuid-empty"] == "[]"  # NOT touched
        assert rows["uuid-good"] == '[{"role":"user","content":"prior"}]'  # NOT touched

        # Idempotent: re-run is a no-op.
        summary2 = backfill_partial(db, projects)
        assert summary2.updated == 0

    def test_skipped_missing_jsonl(self, db_and_projects):
        db, projects = db_and_projects
        _insert_session(db, "uuid-lost", None, None)
        # No JSONL written.
        summary = backfill_partial(db, projects)
        assert summary.updated == 0
        assert summary.skipped_missing_jsonl == 1


class TestFullBackfill:
    """Scenario: `--backfill --full` delete-rebuilds and preserves orphans."""

    def test_full_rebuild_updates_empty_row(self, db_and_projects):
        db, projects = db_and_projects

        # A row with "[]" whose JSONL has text turns — partial backfill will
        # skip this but --full must fix it.
        _insert_session(db, "uuid-broken", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "broken.jsonl", "uuid-broken",
            [_text_turn(text="was lost")],
        )
        # Orphan: DB row with no JSONL.
        _insert_session(db, "uuid-orphan", "[]", "2026-04-15T10:00:00Z")

        # Sanity: partial backfill leaves "[]" alone.
        partial = backfill_partial(db, projects)
        assert partial.updated == 0

        # --full must repair.
        full = backfill_full(db, projects)
        assert full.deleted_and_reingested == 1
        assert full.orphans_preserved == 1

        conn = sqlite3.connect(str(db))
        try:
            rows = dict(conn.execute(
                "SELECT session_uuid, raw_content_json FROM sessions"
            ).fetchall())
        finally:
            conn.close()
        assert rows["uuid-broken"] != "[]"
        assert rows["uuid-broken"] is not None
        # Orphan preserved byte-for-byte.
        assert "uuid-orphan" in rows
        assert rows["uuid-orphan"] == "[]"


class TestBackfillLogging:
    """F-3: exception swallowing must emit a logging.warning with uuid + path
    so the operator has a trail beyond the opaque summary.errored count.
    """

    def test_partial_backfill_logs_on_failure(self, db_and_projects, caplog):
        import logging as _logging
        db, projects = db_and_projects
        _insert_session(db, "uuid-bad", None, None)
        path = projects / "proj" / "bad.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"sessionId": "uuid-bad", "type": "summary"})
            + "\n{not json\n"
        )
        with caplog.at_level(_logging.WARNING, logger="synthesis.coverage"):
            summary = backfill_partial(db, projects)
        assert summary.errored == 1
        assert any("uuid-bad" in rec.message for rec in caplog.records)

    def test_full_backfill_logs_on_failure(self, db_and_projects, caplog):
        import logging as _logging
        db, projects = db_and_projects
        path = projects / "proj" / "bad.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"sessionId": "uuid-bad", "type": "summary"})
            + "\n{not json\n"
        )
        with caplog.at_level(_logging.WARNING, logger="synthesis.coverage"):
            summary = backfill_full(db, projects)
        assert summary.errored == 1
        assert any("uuid-bad" in rec.message for rec in caplog.records)


class TestFullBackfillParseFailurePreservesRow:
    """Regression (F-2): a parse_session failure mid-rebuild must not destroy
    the pre-existing DB row. Earlier implementation issued DELETE + commit
    before re-ingest, so a raised parse_session dropped the row permanently.
    """

    def test_parse_failure_preserves_existing_row(self, db_and_projects):
        db, projects = db_and_projects

        # Row with a valid pre-existing raw_content_json value — this is the
        # "last good copy" scenario the regression is about.
        _insert_session(
            db,
            "uuid-valuable",
            '[{"role":"user","content":"irreplaceable"}]',
            "2026-04-15T10:00:00Z",
        )
        # Write a JSONL that has the right sessionId (so it's picked up) but
        # malformed JSON on a later line so parse_session raises
        # SessionParseError.
        path = projects / "proj" / "malformed.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"sessionId": "uuid-valuable", "type": "summary"})
            + "\n{not valid json\n"
        )

        summary = backfill_full(db, projects)
        assert summary.errored == 1
        assert summary.deleted_and_reingested == 0

        # Critical invariant: the pre-existing row is still there, unchanged.
        conn = sqlite3.connect(str(db))
        try:
            rows = dict(conn.execute(
                "SELECT session_uuid, raw_content_json FROM sessions"
            ).fetchall())
        finally:
            conn.close()
        assert "uuid-valuable" in rows
        assert rows["uuid-valuable"] == '[{"role":"user","content":"irreplaceable"}]'


class TestTwoWayDiff:
    """Scenario: unprocessed JSONLs and orphan DB rows appear in distinct lists."""

    def test_diff_lists(self, db_and_projects):
        db, projects = db_and_projects
        # S: JSONL on disk, no DB row.
        _write_jsonl(
            projects / "proj" / "s.jsonl", "uuid-S",
            [_text_turn(text="unprocessed")],
        )
        # T: DB row with no JSONL.
        _insert_session(
            db, "uuid-T", '[{"role":"user","content":"x"}]',
            "2026-04-15T10:00:00Z",
        )

        report = collect_coverage(db, projects, week_start="monday")
        assert any(
            "s.jsonl" in path for path in report.unprocessed_jsonls
        )
        assert "uuid-T" in report.orphan_db_rows
        # Neither list contaminates the other.
        assert "uuid-T" not in report.unprocessed_jsonls
        assert not any(
            "uuid-S" in str(u) for u in report.orphan_db_rows
        )


class TestEmptyClassification:
    """Scenario: `"[]"` rows split by whether JSONL has text turns."""

    def test_parser_bug_vs_degenerate(self, db_and_projects):
        db, projects = db_and_projects

        _insert_session(db, "uuid-A", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "a.jsonl", "uuid-A",
            [_text_turn(text="has text")],
        )
        _insert_session(db, "uuid-B", "[]", "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "b.jsonl", "uuid-B",
            [_tool_use_turn("ls")],
        )

        report = collect_coverage(db, projects, week_start="monday")
        assert "uuid-A" in report.empty_but_jsonl_has_text
        assert "uuid-A" not in report.empty_and_jsonl_truly_empty
        assert "uuid-B" in report.empty_and_jsonl_truly_empty
        assert "uuid-B" not in report.empty_but_jsonl_has_text


class TestPerUnitBucketing:
    """Scenario: mechanical GH-ref parse buckets sessions into units."""

    def test_refs_from_tool_use(self, db_and_projects):
        db, projects = db_and_projects

        # Session X: reference only inside a tool_use Bash command.
        _insert_session(db, "uuid-X", '[{"role":"user"}]', "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "x.jsonl", "uuid-X",
            [_tool_use_turn("gh issue view 70 --repo owner/repo")],
        )
        # Session Y: two distinct references.
        _insert_session(db, "uuid-Y", '[{"role":"user"}]', "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "y.jsonl", "uuid-Y",
            [_text_turn(text="#42 and https://github.com/a/b/pull/99")],
        )
        # Session Z: no references at all.
        _insert_session(db, "uuid-Z", '[{"role":"user"}]', "2026-04-15T10:00:00Z")
        _write_jsonl(
            projects / "proj" / "z.jsonl", "uuid-Z",
            [_text_turn(text="just a plain message")],
        )

        report = collect_coverage(db, projects, week_start="monday")

        assert "owner/repo#70" in report.per_unit
        assert report.per_unit["owner/repo#70"]["total"] == 1
        assert "#42" in report.per_unit
        assert "a/b#99" in report.per_unit
        assert "unattributed" in report.per_unit
        assert report.per_unit["unattributed"]["total"] == 1
        # Sum of per-unit totals must be >= total (session Y double-counted).
        per_unit_sum = sum(v["total"] for v in report.per_unit.values())
        assert per_unit_sum >= report.total


class TestRunCoverageWrapper:
    """Exercises the `run_coverage` CLI glue."""

    def test_report_mode_exits_zero(self, db_and_projects, capsys):
        db, projects = db_and_projects
        rc = run_coverage(
            sessions_db=db,
            projects_path=projects,
            week_start="monday",
            data_dir=None,
            emit_json=False,
        )
        assert rc == 0
        captured = capsys.readouterr()
        assert "Coverage diagnostic" in captured.out

    def test_json_mode(self, db_and_projects, capsys):
        db, projects = db_and_projects
        _insert_session(db, "uuid-A", None, None)
        rc = run_coverage(
            sessions_db=db,
            projects_path=projects,
            week_start="monday",
            data_dir=None,
            emit_json=True,
        )
        assert rc == 0
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["total"] == 1
        assert parsed["null_count"] == 1

    def test_full_without_backfill_errors(self, db_and_projects, capsys):
        db, projects = db_and_projects
        rc = run_coverage(
            sessions_db=db,
            projects_path=projects,
            week_start="monday",
            data_dir=None,
            do_backfill=False,
            full_rebuild=True,
        )
        assert rc == 2
        captured = capsys.readouterr()
        # Operators hitting this case must see the flag name in the guidance,
        # not just an opaque exit code. Pin the text so a silent regression
        # (e.g. dropping "--full") breaks this test instead of breaking users.
        assert "--full requires --backfill" in captured.err
