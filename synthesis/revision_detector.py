"""Epic #27 — X-3 (#74): mid-unit expectation revision detection.

Walks each unit's transcripts after the commitment point (from X-1's
``expectations`` row) and emits one row in ``expectation_revisions`` per
detected shift. Shifts are anchored to a specific structural trigger:

* ``reprompt``          — a user reprompt turn (counted by Phase 2 into
                          ``sessions.reprompt_count``). Revision detection
                          locates the actual turn indices rather than just
                          reading the aggregate count.
* ``scope_change_turn`` — a user text turn whose content matches a coarse
                          keyword pre-filter (``actually``, ``also``,
                          ``wait``, ``let's instead``, ...), then is
                          confirmed by the LLM classifier.
* ``session_break``     — a gap > 24 h between consecutive sessions in the
                          unit; anchored at the resumption turn.

Behavioral invariants (from the refined spec):

* **Zero rows is a valid result.** A unit with no structural triggers
  produces no rows — absence is information.
* **The committed expectation is immutable.** X-3 never mutates the
  ``expectations`` row; revisions are a parallel sibling history.
* **Low confidence is surfaced, not dropped.** Rows with
  ``confidence < 0.5`` are written as-is and marked in the retrospective.
* **Idempotent re-runs.** Upsert on ``(week_start, unit_id,
  revision_index)`` — re-running without ``--rebuild`` does not insert
  duplicates; existing ``detected_at`` is preserved on no-op re-runs.
* **Offline parity.** With ``AMIS_SYNTHESIS_LIVE`` unset, the fake
  adapter returns canned Markdown. The classifier coerces that into a
  low-confidence revision so the pipeline still exercises the row-write
  path end-to-end in tests.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.expectations import _extract_turns
from synthesis.llm_adapter import _get_adapter
from synthesis.weekly import _load_units


logger = logging.getLogger(__name__)


_MAX_OUTPUT_TOKENS = 512

# Session-break threshold. Hardcoded for v1 per the refined spec —
# parameterization is deferred until X-4 corrections indicate otherwise.
SESSION_BREAK_THRESHOLD_SECONDS = 24 * 60 * 60

REVISION_TRIGGER_ENUM: tuple[str, ...] = (
    "reprompt",
    "scope_change_turn",
    "session_break",
)

FACET_ENUM: tuple[str, ...] = ("scope", "effort", "outcome")

# Coarse keyword pre-filter for scope-change turns. Mirrors X-1's hybrid
# structural+LLM approach — cheap keyword filter first, LLM confirms and
# names the facet. Any text turn matching at least one of these cues is a
# scope-change candidate.
_SCOPE_CHANGE_CUES: tuple[str, ...] = (
    "actually",
    "also",
    "wait",
    "let's instead",
    "lets instead",
    "on second thought",
    "scratch that",
    "instead of",
    "forget that",
)


_REVISION_SYSTEM_PROMPT = """You are classifying a mid-unit expectation \
shift for a retrospective calibration system.

A "unit" is a software-development cycle. After the user accepts a plan \
(the commitment point), they may shift expectations mid-stream: reprompt \
with new requirements, resume after a break with a different attack, or \
insert a scope-change turn. You will be given the trigger type, the \
triggering turn text, and surrounding context. Classify:

1. facet: one of scope | effort | outcome. Which aspect of the original \
expectation shifted.
2. before_text: one sentence describing the pre-shift expectation.
3. after_text: one sentence describing the post-shift expectation.
4. confidence: self-reported certainty in [0.0, 1.0]. Low confidence is \
expected when the shift is ambiguous or the turn text is sparse — do \
NOT inflate confidence. Low-confidence rows are preserved downstream.

Return a single JSON object with exactly these keys:
  facet, before_text, after_text, confidence

Do NOT wrap in Markdown fences. Do NOT add extra keys."""


# ---------------------------------------------------------------------------
# Pure-function structural trigger detection
# ---------------------------------------------------------------------------


def _parse_iso_timestamp(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp string; return ``None`` on failure."""
    if not value:
        return None
    try:
        # Handle both ``Z`` suffix and offset-aware ISO strings.
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _is_reprompt_turn(turn: Dict[str, Any]) -> bool:
    """Heuristic: a user text turn after a prior assistant action.

    The structural walker uses positional context (prior assistant turn)
    rather than this predicate alone — see :func:`detect_structural_triggers`.
    """
    return (
        turn.get("role") == "user"
        and turn.get("kind") == "text"
        and bool((turn.get("text") or "").strip())
    )


def _matches_scope_change_cue(text: str) -> bool:
    """Return True if *text* contains a scope-change keyword cue.

    Case-insensitive substring match over the curated cue list. This is
    the pre-filter — the LLM confirms and names the facet downstream.
    """
    if not text:
        return False
    low = text.lower()
    return any(cue in low for cue in _SCOPE_CHANGE_CUES)


def detect_structural_triggers(
    turns: Sequence[Dict[str, Any]],
    *,
    commitment_turn_idx: Optional[int],
    reprompt_count: int,
    session_boundaries: Sequence[Tuple[int, datetime, datetime]] = (),
) -> List[Dict[str, Any]]:
    """Return structural trigger records for a unit's transcript.

    Pure function — no DB, no LLM, no I/O. Inputs:

    *turns* : flat turn list (from :func:`_extract_turns`).
    *commitment_turn_idx* : index of X-1's commitment point (walker only
    emits triggers at turns strictly AFTER this). ``None`` → walk from 0.
    *reprompt_count* : aggregate from ``sessions.reprompt_count``. Used
    to cap the number of reprompt triggers emitted.
    *session_boundaries* : optional list of
    ``(resumption_turn_idx, prev_session_end, this_session_start)``
    tuples, one per session break in the unit (only breaks >
    :data:`SESSION_BREAK_THRESHOLD_SECONDS`).

    Returns a list of dicts with keys:
      trigger, turn_idx, text, context

    ``context`` is a short window of surrounding text turns for the LLM.
    """
    if not turns:
        return []

    start = 0 if commitment_turn_idx is None else commitment_turn_idx + 1

    records: List[Dict[str, Any]] = []

    # Reprompt triggers — a reprompt is a user text turn that follows a
    # prior assistant action (text or tool_use) and is NOT the commitment
    # turn itself. We cap at ``reprompt_count`` so attribution stays
    # deterministic vs. Phase 2's aggregate.
    if reprompt_count > 0:
        emitted = 0
        saw_assistant = False
        for i in range(start, len(turns)):
            t = turns[i]
            if t.get("role") == "assistant":
                saw_assistant = True
                continue
            if emitted >= reprompt_count:
                break
            if saw_assistant and _is_reprompt_turn(t):
                records.append(
                    {
                        "trigger": "reprompt",
                        "turn_idx": i,
                        "text": t.get("text") or "",
                        "context": _collect_context(turns, i, window=2),
                    }
                )
                emitted += 1
                # Require a new assistant turn before the next reprompt.
                saw_assistant = False

    # Scope-change turn triggers — user text turns matching the keyword
    # pre-filter. These overlap with reprompts by design; the LLM can
    # reassign facet and deduplication happens downstream via
    # (revision_turn, trigger) uniqueness within the emitted records.
    seen_turns = {r["turn_idx"] for r in records}
    for i in range(start, len(turns)):
        t = turns[i]
        if t.get("role") != "user" or t.get("kind") != "text":
            continue
        text = t.get("text") or ""
        if not _matches_scope_change_cue(text):
            continue
        if i in seen_turns:
            continue
        records.append(
            {
                "trigger": "scope_change_turn",
                "turn_idx": i,
                "text": text,
                "context": _collect_context(turns, i, window=2),
            }
        )
        seen_turns.add(i)

    # Session-break triggers — anchored at each resumption turn.
    for resume_idx, prev_end, this_start in session_boundaries:
        if resume_idx < start:
            continue
        if not prev_end or not this_start:
            continue
        gap = (this_start - prev_end).total_seconds()
        if gap <= SESSION_BREAK_THRESHOLD_SECONDS:
            continue
        if 0 <= resume_idx < len(turns):
            text = turns[resume_idx].get("text") or ""
        else:
            text = ""
        records.append(
            {
                "trigger": "session_break",
                "turn_idx": resume_idx,
                "text": text,
                "context": _collect_context(turns, resume_idx, window=2),
            }
        )

    # Order by turn index so revision_index is deterministic.
    records.sort(key=lambda r: (r["turn_idx"], r["trigger"]))
    return records


def _collect_context(
    turns: Sequence[Dict[str, Any]], anchor_idx: int, window: int = 2
) -> str:
    """Return a short text block of ±``window`` text turns around *anchor_idx*."""
    if anchor_idx < 0 or anchor_idx >= len(turns):
        return ""
    text_idxs = [
        i
        for i, t in enumerate(turns)
        if t.get("kind") == "text" and (t.get("text") or "").strip()
    ]
    if anchor_idx not in text_idxs:
        # Anchor is not itself a text turn; surface nearest text turns.
        # Find position of first text index >= anchor_idx, fall back to last.
        pos = 0
        for p, idx in enumerate(text_idxs):
            if idx >= anchor_idx:
                pos = p
                break
        else:
            pos = max(0, len(text_idxs) - 1)
    else:
        pos = text_idxs.index(anchor_idx)
    lo = max(0, pos - window)
    hi = min(len(text_idxs), pos + window + 1)
    lines: List[str] = []
    for p in range(lo, hi):
        idx = text_idxs[p]
        marker = " [ANCHOR]" if idx == anchor_idx else ""
        text = turns[idx].get("text") or ""
        lines.append(f"turn {idx}{marker}: {text}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM parsing
# ---------------------------------------------------------------------------


def _parse_llm_response(response_text: str) -> Optional[Dict[str, Any]]:
    if not response_text:
        return None
    m = re.search(r"\{.*\}", response_text, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def _coerce_facet(value: Any) -> str:
    if isinstance(value, str) and value in FACET_ENUM:
        return value
    return "scope"


def _coerce_confidence(value: Any) -> float:
    try:
        f = float(value)
    except (ValueError, TypeError):
        return 0.3
    if f < 0.0:
        return 0.0
    if f > 1.0:
        return 1.0
    return f


def classify_revision(
    trigger_record: Dict[str, Any],
    *,
    adapter,
    model: str,
    expectation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Call the LLM classifier for one trigger record and return a revision dict.

    Returns a dict with keys ``facet``, ``before_text``, ``after_text``,
    ``confidence`` — every key is non-NULL (the offline / declined path
    falls back to low-confidence defaults).
    """
    user_parts: List[str] = []
    user_parts.append(f"## Trigger: {trigger_record['trigger']}")
    user_parts.append(f"## Turn index: {trigger_record['turn_idx']}")
    if expectation:
        user_parts.append("")
        user_parts.append("### Original expectation (from X-1)")
        user_parts.append(
            f"- expected_scope: {expectation.get('expected_scope') or '(unknown)'}"
        )
        user_parts.append(
            f"- expected_effort: {expectation.get('expected_effort') or '(unknown)'}"
        )
        user_parts.append(
            f"- expected_outcome: {expectation.get('expected_outcome') or '(unknown)'}"
        )
    user_parts.append("")
    user_parts.append("### Anchor + context")
    user_parts.append(trigger_record.get("context") or "")
    user_text = "\n".join(user_parts)

    try:
        result = adapter.call(
            _REVISION_SYSTEM_PROMPT, user_text, model, _MAX_OUTPUT_TOKENS
        )
        parsed = _parse_llm_response(result.text)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "revision_detector LLM call failed for turn=%s: %s — "
            "using low-confidence fallback",
            trigger_record.get("turn_idx"),
            exc,
        )
        parsed = None

    if not parsed:
        # Offline fake-adapter path returns canned Markdown, not this
        # JSON. Fall back to a low-confidence record derived from the
        # trigger text so the pipeline still produces a non-empty row.
        anchor_text = (trigger_record.get("text") or "").strip() or "(no text)"
        return {
            "facet": "scope",
            "before_text": "(pre-revision expectation not parsed)",
            "after_text": anchor_text[:200],
            "confidence": 0.3,
        }

    return {
        "facet": _coerce_facet(parsed.get("facet")),
        "before_text": (
            parsed.get("before_text") if isinstance(parsed.get("before_text"), str)
            else ""
        ),
        "after_text": (
            parsed.get("after_text") if isinstance(parsed.get("after_text"), str)
            else ""
        ),
        "confidence": _coerce_confidence(parsed.get("confidence")),
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _load_expectations(
    exp_conn: sqlite3.Connection, week_start: str
) -> List[Dict[str, Any]]:
    rows = exp_conn.execute(
        "SELECT week_start, unit_id, commitment_point, expected_scope, "
        "       expected_effort, expected_outcome, skip_reason "
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
            "skip_reason": r[6],
        }
        for r in rows
    ]


def _load_unit_sessions(
    gh_conn: sqlite3.Connection,
    sessions_conn: sqlite3.Connection,
    week_start: str,
    unit_id: str,
) -> List[Dict[str, Any]]:
    """Return one dict per session in the unit, ordered by start time.

    Each dict: ``session_uuid``, ``raw_content_json``, ``reprompt_count``,
    ``session_started_at``, ``session_ended_at`` (as datetime or None).
    """
    # Resolve the unit's session UUIDs via the shared helper, which handles
    # both graph-edge linkage (PR/session-rooted units) and the
    # session_issue_attribution fallback for issue-rooted units.
    from synthesis.weekly import _resolve_unit_sessions

    row = gh_conn.execute(
        "SELECT root_node_id, root_node_type FROM units WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchone()
    root_node_id = row[0] if row else ""
    root_node_type = row[1] if row else ""
    session_uuids = _resolve_unit_sessions(
        gh_conn, week_start, root_node_id or "", root_node_type or ""
    )
    if not session_uuids:
        return []

    placeholders = ",".join("?" * len(session_uuids))
    rows = sessions_conn.execute(
        f"SELECT session_uuid, raw_content_json, reprompt_count, "
        f"       session_started_at, session_ended_at "
        f"FROM sessions WHERE session_uuid IN ({placeholders})",
        session_uuids,
    ).fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append(
            {
                "session_uuid": r[0],
                "raw_content_json": r[1] or "",
                "reprompt_count": r[2] or 0,
                "session_started_at": _parse_iso_timestamp(r[3]),
                "session_ended_at": _parse_iso_timestamp(r[4]),
            }
        )
    # Order by start timestamp — None sorts last so unknown-order sessions
    # still produce deterministic output.
    items.sort(
        key=lambda s: (
            s["session_started_at"] or datetime.max.replace(tzinfo=timezone.utc),
            s["session_uuid"],
        )
    )
    return items


def _assemble_unit_turns(
    sessions: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int, List[Tuple[int, datetime, datetime]]]:
    """Concatenate turns from every session; emit session-boundary records.

    Returns ``(all_turns, total_reprompt_count, boundaries)``. Each
    boundary is ``(first_turn_idx_of_this_session, prev_session_end,
    this_session_start)`` for the second + later sessions.
    """
    all_turns: List[Dict[str, Any]] = []
    boundaries: List[Tuple[int, datetime, datetime]] = []
    total_reprompts = 0

    prev_session_end: Optional[datetime] = None
    for s in sessions:
        turns = _extract_turns(s["raw_content_json"])
        if not turns:
            prev_session_end = s.get("session_ended_at") or prev_session_end
            total_reprompts += s.get("reprompt_count") or 0
            continue
        start_idx = len(all_turns)
        all_turns.extend(turns)
        total_reprompts += s.get("reprompt_count") or 0
        if prev_session_end and s.get("session_started_at"):
            boundaries.append(
                (start_idx, prev_session_end, s["session_started_at"])
            )
        prev_session_end = s.get("session_ended_at") or prev_session_end
    return all_turns, total_reprompts, boundaries


def _parse_commitment_turn_idx(commitment_point: Optional[str]) -> Optional[int]:
    """Extract a numeric turn index from X-1's free-text commitment_point.

    Matches the pattern ``turn <N>`` (case-insensitive) to avoid picking up
    stray digit sequences (e.g. version numbers like "v2").  Returns ``None``
    when no such pattern is found.
    """
    if not commitment_point:
        return None
    m = re.search(r"turn\s+(\d+)", commitment_point, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _existing_revision_keys(
    exp_conn: sqlite3.Connection, week_start: str, unit_id: str
) -> set[int]:
    rows = exp_conn.execute(
        "SELECT revision_index FROM expectation_revisions "
        "WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchall()
    return {r[0] for r in rows}


def _upsert_revision(
    exp_conn: sqlite3.Connection,
    *,
    week_start: str,
    unit_id: str,
    revision_index: int,
    revision_turn: Optional[int],
    revision_trigger: str,
    facet: str,
    before_text: Optional[str],
    after_text: Optional[str],
    confidence: float,
    preserve_detected_at: bool,
) -> None:
    """Insert or update a revision row.

    When *preserve_detected_at* is True AND a row already exists at this
    key, the existing ``detected_at`` is kept (idempotent re-run). When
    False, a fresh ``datetime('now')`` is written.
    """
    if preserve_detected_at:
        existing = exp_conn.execute(
            "SELECT detected_at FROM expectation_revisions "
            "WHERE week_start = ? AND unit_id = ? AND revision_index = ?",
            (week_start, unit_id, revision_index),
        ).fetchone()
        if existing:
            # Row is already present — do not touch it. Idempotent no-op.
            return
    exp_conn.execute(
        "INSERT OR REPLACE INTO expectation_revisions "
        "(week_start, unit_id, revision_index, revision_turn, "
        " revision_trigger, facet, before_text, after_text, "
        " confidence, detected_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (
            week_start,
            unit_id,
            revision_index,
            revision_turn,
            revision_trigger,
            facet,
            before_text,
            after_text,
            confidence,
        ),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    week_start: str,
    *,
    github_db: str,
    sessions_db: str,
    expectations_db: str,
    config: Optional[SynthesisConfig] = None,
    rebuild: bool = False,
    repo: Optional[str] = None,
    unit_ids: Optional[List[str]] = None,
) -> int:
    """Detect expectation revisions for every unit in *week_start*.

    Parameters
    ----------
    week_start:
        ``YYYY-MM-DD`` anchor.
    github_db, sessions_db, expectations_db:
        Paths to the collector / expectation DBs. Only ``expectations_db``
        is written to; the others are read-only.
    config:
        Optional :class:`SynthesisConfig`. Used to select the LLM model
        and adapter. When ``None``, the fake adapter is still used
        (matching gap_analysis's offline behavior).
    rebuild:
        When True, existing revision rows for the week are deleted before
        the pass. Otherwise the upsert preserves existing rows
        (idempotent default — per AS-7).

    Returns
    -------
    Number of revision rows written for the week. ``0`` is a valid no-op
    (no expectations, or no units have structural triggers).
    """
    exp_conn = sqlite3.connect(str(expectations_db))
    gh_conn = sqlite3.connect(str(github_db))
    sessions_conn = None
    try:
        # Pre-flight: ensure the table exists.
        try:
            exp_conn.execute(
                "SELECT 1 FROM expectation_revisions LIMIT 1"
            ).fetchall()
        except sqlite3.OperationalError:
            logger.warning(
                "expectation_revisions table missing — caller did not run "
                "init_expectations_db; skipping revision pass for week=%s",
                week_start,
            )
            return 0

        _same = sessions_db == github_db
        try:
            if not _same:
                _same = os.path.samefile(github_db, sessions_db)
        except OSError:
            pass
        sessions_conn = gh_conn if _same else sqlite3.connect(str(sessions_db))

        expectations = _load_expectations(exp_conn, week_start)

        # Issue #88: restrict to the targeted repo's unit set. Same
        # strategy as gap_analysis.run — compute the unit set once via
        # weekly's filter helper and drop expectation rows outside it.
        if repo:
            targeted_units = {
                u["unit_id"] for u in _load_units(gh_conn, week_start, repo=repo)
            }
            expectations = [
                e for e in expectations if e["unit_id"] in targeted_units
            ]

        if unit_ids:
            uid_set = set(unit_ids)
            expectations = [e for e in expectations if e["unit_id"] in uid_set]

        if not expectations:
            repo_suffix = f" repo={repo}" if repo else ""
            logger.info(
                "revision_detector: no expectations for week=%s%s; no-op",
                week_start, repo_suffix,
            )
            return 0

        if rebuild:
            exp_conn.execute(
                "DELETE FROM expectation_revisions WHERE week_start = ?",
                (week_start,),
            )
            exp_conn.commit()

        # Resolve the LLM adapter once. ``_get_adapter`` accepts None for
        # offline mode via the AMIS_SYNTHESIS_LIVE env check, but its
        # signature requires a SynthesisConfig — synthesize one if the
        # caller did not provide it.
        if config is None:
            config = SynthesisConfig()
        try:
            adapter = _get_adapter(config)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "revision_detector: adapter init failed (%s); skipping "
                "revision pass for week=%s", exc, week_start,
            )
            return 0

        total_written = 0
        for exp in expectations:
            unit_id = exp["unit_id"]

            # Skip units with a skip_reason — there is no committed
            # expectation to revise against.
            if exp.get("skip_reason"):
                continue

            sessions = _load_unit_sessions(
                gh_conn, sessions_conn, week_start, unit_id
            )
            if not sessions:
                continue

            all_turns, total_reprompts, boundaries = _assemble_unit_turns(
                sessions
            )
            if not all_turns:
                continue

            commitment_idx = _parse_commitment_turn_idx(
                exp.get("commitment_point")
            )

            triggers = detect_structural_triggers(
                all_turns,
                commitment_turn_idx=commitment_idx,
                reprompt_count=total_reprompts,
                session_boundaries=boundaries,
            )
            if not triggers:
                continue

            existing_keys = (
                set() if rebuild else _existing_revision_keys(
                    exp_conn, week_start, unit_id
                )
            )

            for idx, trigger_record in enumerate(triggers):
                # Idempotency: when not rebuilding and the row at this
                # index already exists, _upsert_revision is a no-op and
                # preserves the original detected_at.
                preserve = (not rebuild) and (idx in existing_keys)

                classification = classify_revision(
                    trigger_record,
                    adapter=adapter,
                    model=config.model,
                    expectation=exp,
                )

                _upsert_revision(
                    exp_conn,
                    week_start=week_start,
                    unit_id=unit_id,
                    revision_index=idx,
                    revision_turn=trigger_record["turn_idx"],
                    revision_trigger=trigger_record["trigger"],
                    facet=classification["facet"],
                    before_text=classification["before_text"],
                    after_text=classification["after_text"],
                    confidence=classification["confidence"],
                    preserve_detected_at=preserve,
                )
                if not preserve:
                    total_written += 1

        exp_conn.commit()
        logger.info(
            "revision_detector complete: week=%s revisions_written=%d",
            week_start, total_written,
        )
        return total_written
    finally:
        if sessions_conn is not None and sessions_conn is not gh_conn:
            sessions_conn.close()
        exp_conn.close()
        gh_conn.close()


def load_revision_rows(
    expectations_db: str,
    week_start: str,
    *,
    repo: Optional[str] = None,
    github_db: Optional[str] = None,
    unit_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return revision rows for *week_start*, ordered by unit + revision_index.

    When *repo* is set (issue #88), rows are additionally filtered to
    the targeted repo's unit set via :func:`synthesis.weekly._load_units`.
    The caller must also pass *github_db* so the unit set can be
    resolved — if *repo* is set without *github_db* this function raises
    ``ValueError`` rather than silently returning cross-repo rows (F-2
    cycle-1 fix).
    """
    if repo and not github_db:
        raise ValueError(
            "load_revision_rows: repo filter requires github_db to resolve "
            "the targeted unit set (expectations.db does not carry the "
            "repo column)"
        )
    conn = sqlite3.connect(str(expectations_db))
    try:
        rows = conn.execute(
            "SELECT unit_id, revision_index, revision_turn, revision_trigger, "
            "       facet, before_text, after_text, confidence "
            "FROM expectation_revisions WHERE week_start = ? "
            "ORDER BY unit_id, revision_index",
            (week_start,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    results = [
        {
            "unit_id": r[0],
            "revision_index": r[1],
            "revision_turn": r[2],
            "revision_trigger": r[3],
            "facet": r[4],
            "before_text": r[5],
            "after_text": r[6],
            "confidence": r[7],
        }
        for r in rows
    ]

    if repo and github_db:
        gh_conn = sqlite3.connect(str(github_db))
        try:
            targeted = {
                u["unit_id"]
                for u in _load_units(gh_conn, week_start, repo=repo)
            }
        finally:
            gh_conn.close()
        results = [r for r in results if r["unit_id"] in targeted]

    if unit_ids is not None:
        uid_set = set(unit_ids)
        results = [r for r in results if r["unit_id"] in uid_set]

    return results


__all__ = [
    "REVISION_TRIGGER_ENUM",
    "FACET_ENUM",
    "SESSION_BREAK_THRESHOLD_SECONDS",
    "detect_structural_triggers",
    "classify_revision",
    "run",
    "load_revision_rows",
]
