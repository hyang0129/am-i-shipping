"""Coverage diagnostic for ``sessions.db`` × JSONL-on-disk (Issue #70).

This module is the engine behind the ``am-synthesize coverage`` CLI
subcommand. It is a **permanent pre-synthesis health check** that answers
one question: for the current state of ``sessions.db`` and the JSONL files
on disk, which sessions have a recoverable ``raw_content_json`` transcript
and which do not?

The diagnostic is decoupled from ``synthesis.unit_identifier``: unit
bucketing here is a **mechanical GH-reference parse** of the raw JSONL
(issue numbers, PR numbers, commit SHAs, URLs — including references that
appear inside ``tool_use`` / ``tool_result`` blocks that ``_strip_content_blocks``
removes from ``raw_content_json``). A session with multiple GH references is
counted in every bucket it hits; a session with zero references falls into
an ``unattributed`` bucket. Sessions are never silently excluded.

Two backfill modes repair the DB when the operator opts in:

``--backfill`` (partial, idempotent)
    Re-run ``parse_session`` + ``upsert_session`` only for rows with
    ``raw_content_json IS NULL`` whose JSONL is on disk. Rows with
    ``raw_content_json = "[]"`` or non-empty are NOT touched.

``--backfill --full`` (delete-rebuild)
    For every JSONL on disk, delete the corresponding DB row and re-ingest
    from scratch via ``parse_session`` + ``upsert_session``. Orphan DB
    rows (DB row, no JSONL) are surfaced in the summary and preserved.

``"[]"`` is never a normal state — it is either a parser bug (JSONL has
text turns but parse produced empty) or a degenerate case (JSONL truly
has no text turns). Both are surfaced as distinct lists; no auto-fix.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from collector.session_parser import (
    _discover_session_files,
    _strip_content_blocks,
    parse_session,
)
from collector.store import upsert_session


# ---------------------------------------------------------------------------
# Fill-state classification
# ---------------------------------------------------------------------------

_FILL_NULL = "null"
_FILL_EMPTY = "empty"  # literally "[]"
_FILL_NONEMPTY = "nonempty"


def _classify_fill(raw: Optional[str]) -> str:
    """Map ``raw_content_json`` column value to a fill-state label.

    ``parse_session`` currently serializes an empty transcript as exactly
    ``"[]"``; this function additionally accepts any serializer-variant that
    parses to the empty JSON array (e.g. ``"[ ]"``, ``"[\\n]"``) so a future
    switch to ``json.dumps(..., indent=2)`` or similar would not silently
    reclassify empty rows as non-empty and defeat the diagnostic.
    """
    if raw is None:
        return _FILL_NULL
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        # Malformed JSON counts as nonempty — we still have *something* the
        # operator needs to see, and misclassifying it as empty would hide it
        # from the parser-bug diagnostic list.
        return _FILL_NONEMPTY
    if parsed == []:
        return _FILL_EMPTY
    return _FILL_NONEMPTY


# ---------------------------------------------------------------------------
# GitHub-reference mechanical parser
# ---------------------------------------------------------------------------
#
# These patterns MUST NOT import from ``collector.session_parser`` or
# ``synthesis.graph_builder`` — this module is explicitly decoupled from the
# synthesis unit identifier (see issue #70 refined spec). Patterns are
# duplicated intentionally so future changes to the synthesis-pipeline parser
# do not silently reshape the coverage diagnostic's bucket keys.

# ``#123`` inside free text. Word-boundary guard keeps us out of SHAs and URLs.
_HASH_REF_RE = re.compile(r"(?:^|[^\w])#(\d+)\b")

# ``gh issue|pr (view|comment|create|edit) N [--repo OWNER/REPO]``
_GH_CLI_RE = re.compile(
    r"gh\s+(issue|pr)\s+(view|comment|create|edit|close|reopen)\s+"
    r"(?:(\d+)\s+)?(?:.*?--repo\s+([\w.\-]+/[\w.\-]+))?",
    re.DOTALL,
)

# Bare issue / PR URLs
_GH_URL_RE = re.compile(
    r"https?://github\.com/([\w.\-]+/[\w.\-]+)/(issues|pull)/(\d+)"
)

# Commit SHAs — 7 to 40 hex chars, as a standalone token. Word boundaries
# mean `abc123` inside `abc1234x` won't match. Accept surrounding spaces,
# punctuation, parentheses, and quotes.
_SHA_RE = re.compile(r"(?:^|[^0-9a-f])([0-9a-f]{7,40})(?=[^0-9a-f]|$)")


@dataclass(frozen=True)
class GhRef:
    """Single mechanical reference extracted from a JSONL.

    ``repo`` is ``""`` when the reference source didn't include one (e.g. a
    bare ``#42`` or a raw SHA). Bucketing uses the full tuple, so a bare
    ``#42`` in one session and ``owner/repo#42`` in another are DIFFERENT
    buckets — matching the refined-spec's intent to surface, not resolve.
    """

    repo: str
    kind: str  # "issue" | "pr" | "sha"
    ref: str

    def bucket_key(self) -> str:
        """Human-readable bucket label, e.g. ``owner/repo#42`` or ``sha:abc1234``."""
        if self.kind == "sha":
            return f"sha:{self.ref}"
        prefix = f"{self.repo}" if self.repo else ""
        return f"{prefix}#{self.ref}" if prefix else f"#{self.ref}"


def _scan_text_for_refs(text: str) -> set[GhRef]:
    """Extract every GH reference from a single text blob."""
    refs: set[GhRef] = set()

    for m in _GH_URL_RE.finditer(text):
        repo, kind_word, num = m.group(1), m.group(2), m.group(3)
        refs.add(GhRef(repo=repo, kind="pr" if kind_word == "pull" else "issue", ref=num))

    for m in _GH_CLI_RE.finditer(text):
        obj_kind = m.group(1)  # "issue" or "pr"
        num = m.group(3)
        repo = m.group(4) or ""
        if num:
            refs.add(GhRef(repo=repo, kind=obj_kind, ref=num))

    # ``#42`` — kind left as ``issue`` mechanically; we cannot disambiguate
    # issue vs PR from a bare hash. Bucket key is ``[repo]#42`` regardless.
    for m in _HASH_REF_RE.finditer(text):
        refs.add(GhRef(repo="", kind="issue", ref=m.group(1)))

    for m in _SHA_RE.finditer(text):
        refs.add(GhRef(repo="", kind="sha", ref=m.group(1)))

    return refs


def _extract_jsonl_refs(jsonl_path: Path) -> set[GhRef]:
    """Scan every line of a JSONL and return the union of GH references.

    Unlike ``raw_content_json``, this scan **includes** ``tool_use`` and
    ``tool_result`` blocks — which is the whole point: a session that only
    references issue #70 inside a ``gh issue view 70`` Bash call must still
    be bucketed into that unit.
    """
    refs: set[GhRef] = set()
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # Cheapest path: stringify the whole entry and regex over it.
                # Avoids a bespoke walk over nested content blocks; patterns
                # are anchored enough that JSON delimiters don't false-match.
                refs.update(_scan_text_for_refs(json.dumps(entry, ensure_ascii=False)))
    except OSError:
        return set()
    return refs


def _jsonl_has_text_turns(jsonl_path: Path) -> bool:
    """True iff ``_strip_content_blocks`` would produce at least one non-empty turn.

    Mirrors ``session_parser.parse_session``'s logic: iterate user/assistant
    entries, strip ``thinking``/``tool_use``/``tool_result`` blocks, keep the
    turn iff what remains is a non-empty string or a non-empty list.
    """
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") not in ("user", "assistant"):
                    continue
                msg = entry.get("message", {})
                content = msg.get("content", "")
                stripped = _strip_content_blocks(content)
                if stripped is not None and stripped != [] and stripped != "":
                    return True
    except OSError:
        return False
    return False


# ---------------------------------------------------------------------------
# JSONL discovery + session-UUID indexing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JsonlRecord:
    """One JSONL file and the metadata needed to bucket it."""

    path: Path
    session_uuid: Optional[str]
    project: str  # parent directory name under projects_path
    first_ts: Optional[datetime]


def _discover_jsonls(projects_path: Path) -> List[Path]:
    """Delegate to the canonical discovery helper in ``collector.session_parser``.

    Kept as a thin wrapper so callers within this module don't need to know the
    origin — and so that the subagent-skip rule has a single source of truth.
    """
    return _discover_session_files(projects_path)


def _read_jsonl_header(jsonl: Path, projects_path: Path) -> JsonlRecord:
    """Extract (session_uuid, project, first_ts) in one pass."""
    uuid: Optional[str] = None
    project = ""
    first_ts: Optional[datetime] = None

    # Project = JSONL's parent directory name relative to projects_path.
    # When the JSONL sits deeper, take the first directory under projects_path.
    try:
        rel = jsonl.relative_to(projects_path)
        project = rel.parts[0] if len(rel.parts) > 1 else ""
    except ValueError:
        project = jsonl.parent.name

    try:
        with open(jsonl, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if uuid is None and "sessionId" in entry:
                    uuid = entry["sessionId"]
                if first_ts is None:
                    ts_str = entry.get("timestamp")
                    if ts_str:
                        try:
                            first_ts = datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            )
                        except ValueError:
                            pass
                if uuid is not None and first_ts is not None:
                    break
    except OSError:
        pass

    return JsonlRecord(path=jsonl, session_uuid=uuid, project=project, first_ts=first_ts)


# ---------------------------------------------------------------------------
# Week bucketing
# ---------------------------------------------------------------------------


def _week_start_of(dt: datetime, week_start: str) -> date:
    """Return the date of the week-start boundary containing ``dt``.

    ``week_start`` ∈ {``"monday"``, ``"sunday"``}. Matches the convention used
    by the rest of the synthesis pipeline (``config.synthesis.week_start``).
    """
    d = dt.date()
    if week_start == "sunday":
        # Python's weekday(): Mon=0..Sun=6. For Sunday-start, offset = (d.weekday() + 1) % 7.
        offset = (d.weekday() + 1) % 7
    else:
        # Monday-start (default).
        offset = d.weekday()
    return d - timedelta(days=offset)


# ---------------------------------------------------------------------------
# Coverage report data model
# ---------------------------------------------------------------------------


@dataclass
class CoverageReport:
    total: int = 0
    null_count: int = 0
    empty_count: int = 0
    nonempty_count: int = 0

    per_week: Dict[str, Dict[str, int]] = field(default_factory=dict)
    per_project: Dict[str, Dict[str, int]] = field(default_factory=dict)
    per_unit: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # Diagnostic lists — session_uuids and/or JSONL paths.
    unprocessed_jsonls: List[str] = field(default_factory=list)
    orphan_db_rows: List[str] = field(default_factory=list)
    empty_but_jsonl_has_text: List[str] = field(default_factory=list)
    empty_and_jsonl_truly_empty: List[str] = field(default_factory=list)

    def to_json(self) -> str:
        """Deterministic JSON serialization (keys sorted, lists pre-sorted)."""
        d = asdict(self)
        # Sort diagnostic lists for determinism. Bucket dicts rely on key-sort
        # via ``sort_keys=True`` below.
        for key in (
            "unprocessed_jsonls",
            "orphan_db_rows",
            "empty_but_jsonl_has_text",
            "empty_and_jsonl_truly_empty",
        ):
            d[key] = sorted(d[key])
        return json.dumps(d, sort_keys=True, indent=2, ensure_ascii=False)


def _bump(bucket: Dict[str, Dict[str, int]], key: str, fill: str) -> None:
    row = bucket.setdefault(
        key,
        {"total": 0, "null": 0, "empty": 0, "nonempty": 0},
    )
    row["total"] += 1
    row[fill] += 1


# ---------------------------------------------------------------------------
# Main entry point — collect_coverage
# ---------------------------------------------------------------------------


def collect_coverage(
    sessions_db: Path,
    projects_path: Path,
    *,
    week_start: str = "monday",
) -> CoverageReport:
    """Read ``sessions.db`` + JSONL files on disk; return a CoverageReport.

    Pure function — no writes. All DB writes are confined to
    :func:`backfill_partial` / :func:`backfill_full`.
    """
    report = CoverageReport()

    # ---- Scan JSONLs once ----
    jsonls = _discover_jsonls(projects_path)
    jsonl_by_uuid: Dict[str, JsonlRecord] = {}
    for jsonl in jsonls:
        rec = _read_jsonl_header(jsonl, projects_path)
        if rec.session_uuid:
            # First writer wins — matches ``_build_session_index``.
            jsonl_by_uuid.setdefault(rec.session_uuid, rec)

    # ---- Read sessions.db ----
    db_rows: List[Tuple[str, Optional[str], Optional[str]]] = []
    if sessions_db.exists():
        conn = sqlite3.connect(str(sessions_db))
        try:
            cursor = conn.execute(
                "SELECT session_uuid, raw_content_json, session_started_at "
                "FROM sessions"
            )
            db_rows = list(cursor.fetchall())
        except sqlite3.OperationalError:
            # Table missing — treat as empty DB.
            db_rows = []
        finally:
            conn.close()

    db_uuids: set[str] = set()

    # ---- Per-row bucketing ----
    # Cache extracted refs and text-turn flags per JSONL so we pay each JSONL
    # cost at most once.
    refs_cache: Dict[Path, set[GhRef]] = {}
    text_cache: Dict[Path, bool] = {}

    def _get_refs(p: Path) -> set[GhRef]:
        r = refs_cache.get(p)
        if r is None:
            r = _extract_jsonl_refs(p)
            refs_cache[p] = r
        return r

    def _get_has_text(p: Path) -> bool:
        v = text_cache.get(p)
        if v is None:
            v = _jsonl_has_text_turns(p)
            text_cache[p] = v
        return v

    for session_uuid, raw_content, session_started_at in db_rows:
        db_uuids.add(session_uuid)
        fill = _classify_fill(raw_content)
        report.total += 1
        if fill == _FILL_NULL:
            report.null_count += 1
        elif fill == _FILL_EMPTY:
            report.empty_count += 1
        else:
            report.nonempty_count += 1

        jsonl_rec = jsonl_by_uuid.get(session_uuid)

        # ---- Week bucket ----
        first_ts: Optional[datetime] = None
        if jsonl_rec is not None:
            first_ts = jsonl_rec.first_ts
        if first_ts is None and session_started_at:
            try:
                first_ts = datetime.fromisoformat(
                    session_started_at.replace("Z", "+00:00")
                )
            except ValueError:
                first_ts = None
        week_key = (
            _week_start_of(first_ts, week_start).isoformat()
            if first_ts is not None
            else "unknown"
        )
        _bump(report.per_week, week_key, fill)

        # ---- Project bucket ----
        project = jsonl_rec.project if jsonl_rec is not None else "unknown"
        if not project:
            project = "unknown"
        _bump(report.per_project, project, fill)

        # ---- Unit bucket (mechanical GH-ref parse from JSONL) ----
        if jsonl_rec is not None:
            refs = _get_refs(jsonl_rec.path)
        else:
            refs = set()
        if not refs:
            _bump(report.per_unit, "unattributed", fill)
        else:
            for ref in refs:
                _bump(report.per_unit, ref.bucket_key(), fill)

        # ---- "[]" classification ----
        # Only classify when the JSONL is on disk — a missing JSONL is
        # already surfaced via ``orphan_db_rows`` and calling such a row
        # "truly empty" would be an unsupported claim (we can't read the
        # source to verify).
        if fill == _FILL_EMPTY and jsonl_rec is not None:
            if _get_has_text(jsonl_rec.path):
                report.empty_but_jsonl_has_text.append(session_uuid)
            else:
                report.empty_and_jsonl_truly_empty.append(session_uuid)

    # ---- Two-way diff: unprocessed JSONLs + orphan DB rows ----
    for uuid_on_disk, rec in jsonl_by_uuid.items():
        if uuid_on_disk not in db_uuids:
            report.unprocessed_jsonls.append(str(rec.path))

    for uuid_in_db in db_uuids:
        if uuid_in_db not in jsonl_by_uuid:
            report.orphan_db_rows.append(uuid_in_db)

    return report


# ---------------------------------------------------------------------------
# Text formatter
# ---------------------------------------------------------------------------


def format_text(report: CoverageReport) -> str:
    """Human-readable report for stdout."""
    lines: List[str] = []
    lines.append("Coverage diagnostic — sessions.db × JSONL on disk")
    lines.append("=" * 52)
    lines.append(
        f"Total:     {report.total}   "
        f"NULL: {report.null_count}   "
        f"'[]': {report.empty_count}   "
        f"non-empty: {report.nonempty_count}"
    )
    lines.append("")

    def _section(title: str, bucket: Dict[str, Dict[str, int]]) -> None:
        lines.append(title)
        lines.append("-" * len(title))
        if not bucket:
            lines.append("  (no data)")
            lines.append("")
            return
        for key in sorted(bucket.keys()):
            row = bucket[key]
            lines.append(
                f"  {key}: total={row['total']} "
                f"null={row['null']} empty={row['empty']} "
                f"nonempty={row['nonempty']}"
            )
        lines.append("")

    _section("Per week", report.per_week)
    _section("Per project", report.per_project)
    _section("Per unit (mechanical GH-ref parse from JSONL)", report.per_unit)

    lines.append("Diagnostic lists")
    lines.append("-" * 16)
    lines.append(f"  unprocessed JSONL (JSONL present, no DB row):      {len(report.unprocessed_jsonls)}")
    lines.append(f"  orphan DB rows   (DB row, JSONL missing):           {len(report.orphan_db_rows)}")
    lines.append(f"  '[]' but JSONL has text turns (parser-bug suspect): {len(report.empty_but_jsonl_has_text)}")
    lines.append(f"  '[]' and JSONL truly empty (degenerate):            {len(report.empty_and_jsonl_truly_empty)}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Backfill — partial and full
# ---------------------------------------------------------------------------


@dataclass
class BackfillSummary:
    updated: int = 0
    skipped_already_populated: int = 0
    skipped_missing_jsonl: int = 0
    errored: int = 0
    # --full only
    deleted_and_reingested: int = 0
    orphans_preserved: int = 0


def _build_uuid_index(projects_path: Path) -> Dict[str, Path]:
    """Scan JSONLs and return ``{session_uuid: path}``."""
    index: Dict[str, Path] = {}
    for jsonl in _discover_jsonls(projects_path):
        try:
            with open(jsonl, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    sid = entry.get("sessionId")
                    if sid:
                        index.setdefault(sid, jsonl)
                        break
        except OSError:
            continue
    return index


def backfill_partial(
    sessions_db: Path,
    projects_path: Path,
    *,
    data_dir: Optional[Path] = None,
) -> BackfillSummary:
    """Populate ``raw_content_json`` only for rows that are currently NULL.

    Idempotent: rows with any non-NULL ``raw_content_json`` (including ``"[]"``)
    are NOT touched. Re-running against the same state produces zero updates.
    """
    summary = BackfillSummary()
    if not sessions_db.exists():
        return summary

    index = _build_uuid_index(projects_path)

    conn = sqlite3.connect(str(sessions_db))
    try:
        cursor = conn.execute(
            "SELECT session_uuid FROM sessions WHERE raw_content_json IS NULL"
        )
        targets = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

    for session_uuid in targets:
        jsonl = index.get(session_uuid)
        if jsonl is None:
            summary.skipped_missing_jsonl += 1
            continue
        try:
            record = parse_session(jsonl)
            upsert_session(
                record,
                db_path=sessions_db,
                data_dir=data_dir,
                skip_init=True,
                skip_health=True,
            )
            summary.updated += 1
        except Exception:  # noqa: BLE001 — surface count, keep the loop going
            summary.errored += 1

    return summary


def backfill_full(
    sessions_db: Path,
    projects_path: Path,
    *,
    data_dir: Optional[Path] = None,
) -> BackfillSummary:
    """Re-ingest every JSONL on disk, overwriting every column of the matching
    DB row.

    Orphan rows (DB row present, JSONL absent) are LEFT IN PLACE and surfaced
    in ``summary.orphans_preserved`` — never silently dropped. Requires an
    explicit ``--full`` flag at the CLI layer; this function assumes the
    caller has already asserted that.

    Implementation note: we rely on ``upsert_session``'s
    ``INSERT ... ON CONFLICT(session_uuid) DO UPDATE SET ...`` clause (see
    ``collector/store.py``) to overwrite every column. An earlier
    implementation issued a ``DELETE`` before the upsert, but that opened a
    destructive window: if ``parse_session`` raised mid-loop (malformed
    JSONL, truncated file) the row was already gone from the DB with no
    rollback path, even when the pre-rebuild DB value was the only surviving
    copy. The upsert-only path is atomic per-session and preserves the DB
    row on parse failure.
    """
    summary = BackfillSummary()
    if not sessions_db.exists():
        return summary

    index = _build_uuid_index(projects_path)

    # Determine orphans BEFORE the loop (so counting is independent of order).
    conn = sqlite3.connect(str(sessions_db))
    try:
        cursor = conn.execute("SELECT session_uuid FROM sessions")
        db_uuids = {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()

    summary.orphans_preserved = sum(1 for u in db_uuids if u not in index)

    for session_uuid, jsonl in index.items():
        try:
            record = parse_session(jsonl)
            upsert_session(
                record,
                db_path=sessions_db,
                data_dir=data_dir,
                skip_init=True,
                skip_health=True,
            )
            summary.deleted_and_reingested += 1
        except Exception:  # noqa: BLE001
            summary.errored += 1

    return summary


# ---------------------------------------------------------------------------
# CLI glue (called from synthesis.cli)
# ---------------------------------------------------------------------------


def run_coverage(
    *,
    sessions_db: Path,
    projects_path: Path,
    week_start: str,
    data_dir: Optional[Path],
    emit_json: bool = False,
    do_backfill: bool = False,
    full_rebuild: bool = False,
    stdout=None,
    stderr=None,
) -> int:
    """Dispatch the four coverage modes. Returns a shell-suitable exit code.

    Modes (mutually exclusive at the orchestration level, enforced by the CLI):

    * ``do_backfill=False, full_rebuild=False`` → report only.
    * ``do_backfill=True,  full_rebuild=False`` → partial backfill + summary.
    * ``do_backfill=True,  full_rebuild=True``  → delete-rebuild + summary.
    """
    # Resolve stdout/stderr at call time so pytest's capsys (which re-binds
    # sys.stdout per test) captures output correctly. Using defaults like
    # ``stdout=sys.stdout`` in the signature would bind to the interpreter-
    # startup stream and bypass the capture.
    if stdout is None:
        stdout = sys.stdout
    if stderr is None:
        stderr = sys.stderr
    if full_rebuild and not do_backfill:
        print("--full requires --backfill", file=stderr)
        return 2

    if do_backfill and full_rebuild:
        summary = backfill_full(sessions_db, projects_path, data_dir=data_dir)
        print(
            f"Backfill (--full) complete: "
            f"deleted+reingested={summary.deleted_and_reingested}, "
            f"orphans_preserved={summary.orphans_preserved}, "
            f"errored={summary.errored}",
            file=stderr,
        )
        return 0

    if do_backfill:
        summary = backfill_partial(sessions_db, projects_path, data_dir=data_dir)
        print(
            f"Backfill complete: "
            f"updated={summary.updated}, "
            f"skipped_missing_jsonl={summary.skipped_missing_jsonl}, "
            f"errored={summary.errored}",
            file=stderr,
        )
        return 0

    # Report-only mode.
    report = collect_coverage(
        sessions_db, projects_path, week_start=week_start
    )
    if emit_json:
        stdout.write(report.to_json() + "\n")
    else:
        stdout.write(format_text(report))
    return 0


def add_coverage_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Attach ``coverage`` subcommand to an existing ``subparsers`` action."""
    p = subparsers.add_parser(
        "coverage",
        help=(
            "Report raw_content_json coverage across sessions.db and the "
            "JSONL files on disk. Optional backfill modes."
        ),
    )
    p.add_argument(
        "--json", dest="emit_json", action="store_true",
        help="Emit machine-readable JSON to stdout instead of prose.",
    )
    p.add_argument(
        "--backfill", action="store_true",
        help=(
            "Re-run parse_session for rows with raw_content_json IS NULL "
            "whose JSONL is on disk. Idempotent."
        ),
    )
    p.add_argument(
        "--full", action="store_true",
        help=(
            "With --backfill: delete every session row whose JSONL is on "
            "disk and re-ingest from scratch. Orphan rows are preserved."
        ),
    )
    p.add_argument(
        "--config", default=None,
        help="Path to config.yaml (default: config.yaml in repo root).",
    )
