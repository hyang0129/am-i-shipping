"""Epic #27 — X-4 (#75): interactive agentic feedback loop.

Drives a multi-turn LLM conversation that walks the user through the
major/critical gap rows produced by X-2 (``expectation_gaps``) and
persists their corrections into ``expectation_corrections``. Also owns
the auto-confirm sweep: any gap row older than 14 days without a user
correction gets a ``corrected_by='auto_confirm'`` row written on every
``am-synthesize --week`` invocation (passive assent).

Design priors inherited from the epic intent:

* **Ship skeleton over plan to perfection.** The v1 agentic loop is
  deliberately minimal — one facet per LLM turn, bounded by a hard
  6-turn cap per gap. Sophistication lives in X-5's consumption of the
  corrections, not in the UI polish here.
* **Corrections are durable in ``expectations.db``.** The shipped
  retrospective ``.md`` is NEVER rewritten (Epic #17 idempotency).
  Future retrospectives read corrected values from the DB.
* **Original value preserved.** ``expectation_gaps`` rows are not
  mutated — ``original_value`` is a snapshot captured at correction
  time. X-5 needs the before/after delta intact.
* **Re-entrant.** A partial correction session can be resumed:
  already-corrected ``(week_start, unit_id, facet)`` triples are
  skipped.

The module is intentionally I/O-parameterised (``input_fn`` /
``output_fn``) so tests drive the loop without a real TTY, and the LLM
adapter is the same ``_get_adapter`` abstraction the rest of synthesis
uses. In offline mode (``AMIS_SYNTHESIS_OFFLINE=1``) the fake client
returns canned text that does not parse as a correction turn; the
module treats unparseable agent output as "confirm, no change" so the
offline smoke-test path still writes rows.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.llm_adapter import _get_adapter


logger = logging.getLogger(__name__)


# The full facet list a correction session walks for a single gap row.
# Order is intentional — the "what did we think would happen" facets
# come first (commitment_point, scope, effort, outcome), then the
# "what went wrong" facets (severity, failure_precondition).
FACETS: Tuple[str, ...] = (
    "commitment_point",
    "scope",
    "effort",
    "outcome",
    "severity",
    "failure_precondition",
)

# Hard cap per gap per X4-OQ-1 default. Unbounded turns risk cost
# overruns; the cap is applied AFTER the initial presentation turn.
MAX_TURNS_PER_GAP = 6

# Auto-confirm horizon — matches the X-2 expectation_gaps sweep window.
AUTO_CONFIRM_DAYS = 14

_MAX_OUTPUT_TOKENS = 512


_CORRECTION_SYSTEM_PROMPT = """You are the correction agent for a \
retrospective calibration system. The user is reviewing a gap row that \
describes an expectation-vs-actual deviation for one software unit. Your \
job for this turn is to propose the single most useful clarifying \
question for the named facet, OR — if the user has already given a clear \
answer — to emit a structured correction.

You will be given:
- The gap row's original expectation + actual outcome for context.
- The facet under review (one of: commitment_point, scope, effort, \
outcome, severity, failure_precondition).
- The user's most recent reply (may be empty on the first turn).

Return a single JSON object with exactly these keys:
  action: "ask" | "confirm" | "correct"
  question: string (non-empty when action='ask', otherwise "")
  corrected_value: string (only meaningful when action='correct')
  correction_note: string (short rationale; empty string when unused)

Rules:
- "ask" — you need more information; question is one short sentence.
- "confirm" — the user has indicated the original value is correct.
- "correct" — the user has supplied a new value; copy it into \
corrected_value.
- Do NOT wrap the JSON in Markdown fences. Do NOT add extra keys."""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_agent_turn(text: str) -> Optional[Dict[str, Any]]:
    """Parse the LLM's JSON response for a single correction turn.

    Returns ``None`` on any parse failure. The caller treats ``None``
    as "confirm no change" so the offline path still writes rows.
    """
    if not text:
        return None
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    action = obj.get("action")
    if action not in {"ask", "confirm", "correct"}:
        return None
    return obj


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _load_major_critical_gaps(
    exp_conn: sqlite3.Connection,
    week_start: str,
    unit_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return major/critical gap rows for the week (optionally one unit)."""
    sql = (
        "SELECT week_start, unit_id, commitment_point, scope_gap, "
        "       effort_gap, outcome_gap, severity, direction, "
        "       failure_precondition "
        "FROM expectation_gaps "
        "WHERE week_start = ? AND severity IN ('major', 'critical') "
    )
    params: List[Any] = [week_start]
    if unit_id is not None:
        sql += "AND unit_id = ? "
        params.append(unit_id)
    sql += "ORDER BY unit_id"
    rows = exp_conn.execute(sql, params).fetchall()
    return [
        {
            "week_start": r[0],
            "unit_id": r[1],
            "commitment_point": r[2],
            "scope_gap": r[3],
            "effort_gap": r[4],
            "outcome_gap": r[5],
            "severity": r[6],
            "direction": r[7],
            "failure_precondition": r[8],
        }
        for r in rows
    ]


def _load_expectation(
    exp_conn: sqlite3.Connection, week_start: str, unit_id: str
) -> Optional[Dict[str, Any]]:
    row = exp_conn.execute(
        "SELECT commitment_point, expected_scope, expected_effort, "
        "       expected_outcome, confidence, skip_reason "
        "FROM expectations WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchone()
    if row is None:
        return None
    return {
        "commitment_point": row[0],
        "expected_scope": row[1],
        "expected_effort": row[2],
        "expected_outcome": row[3],
        "confidence": row[4],
        "skip_reason": row[5],
    }


def _existing_correction_facets(
    exp_conn: sqlite3.Connection, week_start: str, unit_id: str
) -> set[str]:
    rows = exp_conn.execute(
        "SELECT facet FROM expectation_corrections "
        "WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchall()
    return {r[0] for r in rows}


def _insert_correction(
    exp_conn: sqlite3.Connection,
    *,
    week_start: str,
    unit_id: str,
    facet: str,
    original_value: Optional[str],
    corrected_value: Optional[str],
    correction_note: Optional[str],
    corrected_by: str,
) -> None:
    """Insert a correction row. No-op on PK conflict (re-entrant safe)."""
    exp_conn.execute(
        "INSERT OR IGNORE INTO expectation_corrections "
        "(week_start, unit_id, facet, original_value, corrected_value, "
        " correction_note, corrected_by, corrected_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (
            week_start,
            unit_id,
            facet,
            original_value,
            corrected_value,
            correction_note,
            corrected_by,
        ),
    )


def _mark_gap_auto_confirmed(
    exp_conn: sqlite3.Connection,
    week_start: str,
    unit_id: str,
    value: int,
) -> None:
    exp_conn.execute(
        "UPDATE expectation_gaps SET auto_confirmed = ? "
        "WHERE week_start = ? AND unit_id = ?",
        (value, week_start, unit_id),
    )


def _original_value_for_facet(
    gap: Dict[str, Any], expectation: Optional[Dict[str, Any]], facet: str
) -> Optional[str]:
    """Snapshot the current value for *facet* from the gap + expectation."""
    if facet == "commitment_point":
        return gap.get("commitment_point")
    if facet == "severity":
        return gap.get("severity")
    if facet == "failure_precondition":
        return gap.get("failure_precondition")
    if expectation is None:
        # Fall back to the gap-row's textual description of the deviation.
        fallback_map = {
            "scope": "scope_gap",
            "effort": "effort_gap",
            "outcome": "outcome_gap",
        }
        return gap.get(fallback_map.get(facet, ""))
    if facet == "scope":
        return expectation.get("expected_scope")
    if facet == "effort":
        return expectation.get("expected_effort")
    if facet == "outcome":
        return expectation.get("expected_outcome")
    return None


# ---------------------------------------------------------------------------
# Auto-confirm sweep (AS-6)
# ---------------------------------------------------------------------------


def auto_confirm_sweep(
    expectations_db: str,
    *,
    days: int = AUTO_CONFIRM_DAYS,
    now: Optional[datetime] = None,
) -> int:
    """Write ``corrected_by='auto_confirm'`` rows for stale gaps.

    For every gap row older than *days* that has no entry in
    ``expectation_corrections``, insert one correction row per facet
    (snapshotting the current value into ``original_value`` and
    ``corrected_value``) and flip ``expectation_gaps.auto_confirmed``
    to 1. Returns the number of correction rows written.

    Idempotent: already-corrected facets are skipped via the PK
    ``INSERT OR IGNORE``.

    Parameters
    ----------
    expectations_db:
        Path to ``expectations.db``.
    days:
        Age threshold in days. Default 14 (X-4 spec).
    now:
        Clock override for tests. Default ``datetime.now(UTC)``.
    """
    clock = now or datetime.now(timezone.utc)
    cutoff = (clock - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(str(expectations_db))
    try:
        try:
            stale = conn.execute(
                "SELECT week_start, unit_id, commitment_point, scope_gap, "
                "       effort_gap, outcome_gap, severity, direction, "
                "       failure_precondition "
                "FROM expectation_gaps "
                "WHERE computed_at IS NOT NULL AND computed_at < ?",
                (cutoff,),
            ).fetchall()
        except sqlite3.OperationalError:
            # expectation_gaps table missing — nothing to sweep.
            return 0

        if not stale:
            return 0

        written = 0
        for r in stale:
            gap = {
                "week_start": r[0],
                "unit_id": r[1],
                "commitment_point": r[2],
                "scope_gap": r[3],
                "effort_gap": r[4],
                "outcome_gap": r[5],
                "severity": r[6],
                "direction": r[7],
                "failure_precondition": r[8],
            }
            existing = _existing_correction_facets(
                conn, gap["week_start"], gap["unit_id"]
            )
            expectation = _load_expectation(
                conn, gap["week_start"], gap["unit_id"]
            )
            for facet in FACETS:
                if facet in existing:
                    continue
                original = _original_value_for_facet(gap, expectation, facet)
                _insert_correction(
                    conn,
                    week_start=gap["week_start"],
                    unit_id=gap["unit_id"],
                    facet=facet,
                    original_value=original,
                    corrected_value=original,
                    correction_note="auto-confirmed after 14 days without user correction",
                    corrected_by="auto_confirm",
                )
                written += 1
            # Flip the gap row's auto_confirmed flag. The gap row's own
            # values are NOT mutated — only the flag.
            _mark_gap_auto_confirmed(
                conn, gap["week_start"], gap["unit_id"], 1
            )
        conn.commit()
        logger.info(
            "auto_confirm_sweep: wrote %d correction rows for %d stale gaps",
            written, len(stale),
        )
        return written
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Interactive agentic loop (AS-3, AS-4, AS-5, AS-8, AS-9)
# ---------------------------------------------------------------------------


def _build_gap_context(
    gap: Dict[str, Any], expectation: Optional[Dict[str, Any]]
) -> str:
    parts: List[str] = []
    parts.append(f"Unit: {gap['unit_id']}")
    parts.append(f"Week: {gap['week_start']}")
    parts.append(f"Severity: {gap['severity']}")
    parts.append(f"Direction: {gap['direction']}")
    parts.append(f"Failure precondition: {gap.get('failure_precondition')}")
    parts.append("")
    if expectation is not None:
        parts.append("Expectations at commitment point:")
        parts.append(f"  commitment_point: {expectation.get('commitment_point')}")
        parts.append(f"  expected_scope: {expectation.get('expected_scope')}")
        parts.append(f"  expected_effort: {expectation.get('expected_effort')}")
        parts.append(f"  expected_outcome: {expectation.get('expected_outcome')}")
    parts.append("")
    parts.append("Actual deviation (from X-2):")
    parts.append(f"  scope_gap: {gap.get('scope_gap')}")
    parts.append(f"  effort_gap: {gap.get('effort_gap')}")
    parts.append(f"  outcome_gap: {gap.get('outcome_gap')}")
    return "\n".join(parts)


def _run_facet_turn(
    adapter: Any,
    model: str,
    gap_context: str,
    facet: str,
    original_value: Optional[str],
    user_reply: str,
) -> Dict[str, Any]:
    """Ask the LLM to produce one structured correction turn.

    Returns the parsed JSON (action/question/corrected_value/note) or a
    default "confirm no change" dict when parsing fails. The "confirm on
    parse failure" default is deliberate — in offline mode the fake
    adapter returns non-JSON text and we still want the loop to make
    progress.
    """
    user_text = (
        f"{gap_context}\n\n"
        f"Facet under review: {facet}\n"
        f"Current value: {original_value!r}\n"
        f"User reply (may be empty): {user_reply!r}\n"
    )
    try:
        result = adapter.call(
            _CORRECTION_SYSTEM_PROMPT, user_text, model, _MAX_OUTPUT_TOKENS
        )
    except Exception as exc:  # noqa: BLE001 — don't crash the whole loop
        logger.warning(
            "correction: adapter.call failed (%s); defaulting to confirm",
            exc,
        )
        return {
            "action": "confirm",
            "question": "",
            "corrected_value": original_value,
            "correction_note": "",
        }
    parsed = _parse_agent_turn(result.text)
    if parsed is None:
        return {
            "action": "confirm",
            "question": "",
            "corrected_value": original_value,
            "correction_note": "",
        }
    return parsed


def _process_facet(
    adapter: Any,
    model: str,
    gap_context: str,
    facet: str,
    original_value: Optional[str],
    input_fn: Callable[[str], str],
    output_fn: Callable[[str], None],
) -> Tuple[Optional[str], str]:
    """Run the per-facet mini-loop. Returns ``(corrected_value, note)``.

    The loop is bounded by :data:`MAX_TURNS_PER_GAP` per facet. On the
    first turn ``user_reply`` is empty; subsequent turns feed the user's
    reply from ``input_fn`` back into the LLM.
    """
    user_reply = ""
    for turn in range(MAX_TURNS_PER_GAP):
        decision = _run_facet_turn(
            adapter, model, gap_context, facet, original_value, user_reply
        )
        action = decision.get("action")
        if action == "confirm":
            return original_value, decision.get("correction_note", "") or ""
        if action == "correct":
            cv = decision.get("corrected_value")
            return (
                cv if isinstance(cv, str) else original_value,
                decision.get("correction_note", "") or "",
            )
        # action == "ask"
        question = decision.get("question") or (
            f"Is the current value for {facet} ({original_value!r}) correct?"
        )
        output_fn(f"[{facet}] {question}")
        try:
            user_reply = input_fn(f"{facet} > ")
        except EOFError:
            # Non-interactive input exhausted — confirm no change.
            return original_value, ""
        if not user_reply.strip():
            # Empty reply → confirm.
            return original_value, ""
    # Hit the turn cap without resolution — fall back to confirm.
    logger.warning(
        "correction: facet=%s hit max-turn cap (%d); confirming no change",
        facet, MAX_TURNS_PER_GAP,
    )
    return original_value, ""


def run_correction_session(
    week_start: str,
    *,
    expectations_db: str,
    config: SynthesisConfig,
    unit_id: Optional[str] = None,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> int:
    """Walk the week's major/critical gaps and persist user corrections.

    Returns the number of correction rows written in this session.
    Rows for already-corrected facets are skipped — the session is
    re-entrant (AS-8).

    When *unit_id* is provided the loop scopes to that unit only.
    Otherwise every major/critical gap for *week_start* is iterated.
    """
    conn = sqlite3.connect(str(expectations_db))
    try:
        try:
            gaps = _load_major_critical_gaps(conn, week_start, unit_id)
        except sqlite3.OperationalError as exc:
            logger.warning(
                "run_correction_session: expectation_gaps missing (%s); "
                "run 'am-init-db' first.",
                exc,
            )
            return 0

        if not gaps:
            output_fn(
                f"No major/critical gaps for week={week_start}"
                + (f", unit={unit_id}" if unit_id else "")
            )
            return 0

        adapter = _get_adapter(config)
        model = config.model
        total_written = 0

        for gap in gaps:
            existing = _existing_correction_facets(
                conn, gap["week_start"], gap["unit_id"]
            )
            if len(existing) >= len(FACETS):
                # Fully corrected already — skip (AS-8 re-entrancy).
                continue

            expectation = _load_expectation(
                conn, gap["week_start"], gap["unit_id"]
            )
            gap_context = _build_gap_context(gap, expectation)
            output_fn("")
            output_fn(f"=== Gap: unit={gap['unit_id']} severity={gap['severity']} ===")
            output_fn(gap_context)

            gap_wrote_any = False
            for facet in FACETS:
                if facet in existing:
                    continue
                original = _original_value_for_facet(gap, expectation, facet)
                corrected_value, note = _process_facet(
                    adapter,
                    model,
                    gap_context,
                    facet,
                    original,
                    input_fn,
                    output_fn,
                )
                _insert_correction(
                    conn,
                    week_start=gap["week_start"],
                    unit_id=gap["unit_id"],
                    facet=facet,
                    original_value=original,
                    corrected_value=corrected_value,
                    correction_note=note,
                    corrected_by="user",
                )
                total_written += 1
                gap_wrote_any = True

            if gap_wrote_any:
                # AS-4 — flip auto_confirmed to reflect the user's
                # resolution. 1 when every facet was confirmed (no value
                # change), 0 when any facet was corrected.
                any_corrected = bool(
                    conn.execute(
                        "SELECT 1 FROM expectation_corrections "
                        "WHERE week_start = ? AND unit_id = ? "
                        "  AND corrected_by = 'user' "
                        "  AND original_value IS NOT corrected_value "
                        "LIMIT 1",
                        (gap["week_start"], gap["unit_id"]),
                    ).fetchone()
                )
                _mark_gap_auto_confirmed(
                    conn,
                    gap["week_start"],
                    gap["unit_id"],
                    0 if any_corrected else 1,
                )
            conn.commit()
        return total_written
    finally:
        conn.close()


__all__ = [
    "AUTO_CONFIRM_DAYS",
    "FACETS",
    "MAX_TURNS_PER_GAP",
    "auto_confirm_sweep",
    "run_correction_session",
]
