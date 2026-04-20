"""Epic #27 — X-2 (#73): per-unit expectation vs. actual gap analysis.

Joins ``expectations`` rows (from X-1, stored in ``expectations.db``) with
actual-outcome metrics (from ``units`` and ``unit_summaries`` in
``github.db``), computes a per-facet gap (scope / effort / outcome),
assigns a coarse severity + direction, and attributes any non-``none``
severity to a specific idealized-workflow precondition.

Output lands in the ``expectation_gaps`` table (schema owned by
:mod:`am_i_shipping.db`). One row per ``(week_start, unit_id)`` that has
an expectation row. Column names (``commitment_point``, ``scope_gap``,
``effort_gap``, ``outcome_gap``) are IRREVERSIBLE per the epic ADR.

Behavioral invariants (from the refined spec):

* **One gap row per expectation row.** Every unit with an expectations
  row produces exactly one gap row. A unit without an expectations row
  produces no gap row.
* **Constrained ``failure_precondition`` enum.** Drawn from
  ``idealized-workflow.md``. NULL only when ``severity='none'``.
* **Idempotent re-runs.** On each invocation, existing gap rows for the
  target week are deleted and rewritten. The retrospective ``.md``
  refuse-to-overwrite guard (Epic #17 Decision 2) is unaffected — all
  persistent state lives in ``expectations.db``, never in the ``.md``.
* **Auto-confirm sweep.** Gap rows with ``computed_at`` older than 14
  days and ``auto_confirmed=0`` are flipped to 1 on every run. X-4 will
  read / override this column later.
* **Offline parity.** When ``AMIS_SYNTHESIS_LIVE`` is unset the
  :class:`FakeAnthropicClient` path returns a deterministic canned
  JSON; severity computation is a pure function and does not require an
  API call.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from am_i_shipping.config_loader import SynthesisConfig
from synthesis.llm_adapter import _get_adapter


logger = logging.getLogger(__name__)


_MAX_OUTPUT_TOKENS = 512


# The constrained enum for ``failure_precondition``. Any value returned by
# the classifier that is not in this set is coerced to ``phase_0_setup`` —
# the setup phase is the "unknown root cause" bucket because it upstream of
# every design-phase step.
FAILURE_PRECONDITION_ENUM: tuple[str, ...] = (
    "phase_0_setup",
    "step_1_intent",
    "step_2_motivation",
    "step_3_motivation_confirmed",
    "step_4_plan",
    "step_5_plan_confirmed",
)

SEVERITY_ENUM: tuple[str, ...] = ("none", "minor", "major", "critical")
DIRECTION_ENUM: tuple[str, ...] = ("under", "over", "match", "ambiguous")


_GAP_SYSTEM_PROMPT = """You are attributing a software-development unit's \
expectation / actual gap to a specific step of the idealized workflow for \
a retrospective calibration system.

The idealized workflow steps (closed set):
- phase_0_setup: CLAUDE.md, hooks, venv, tooling — the standing preconditions.
- step_1_intent: user stated the intent.
- step_2_motivation: Claude disambiguated the motivation.
- step_3_motivation_confirmed: the motivation was locked in.
- step_4_plan: Claude proposed a bounded plan.
- step_5_plan_confirmed: the plan was accepted.

You will be given the unit's expectations (scope / effort / outcome at the \
commitment point), the actual outcome metrics (elapsed_days, \
total_reprompts, review_cycles, status), and the unit summary text. \
Decide:

1. severity: one of none | minor | major | critical.
   Rubric (apply within the LLM, not externally):
   * none     — actual matched expected across all three facets.
   * minor    — one facet deviated but the unit completed as expected.
   * major    — two+ facets deviated OR one facet deviated significantly \
(>2x expected effort, scope doubled, outcome partially missed).
   * critical — outcome missed entirely OR unit abandoned.
2. direction: one of under | over | match | ambiguous.
3. failure_precondition: which idealized-workflow step was the root cause.
   Must be one of phase_0_setup, step_1_intent, step_2_motivation, \
step_3_motivation_confirmed, step_4_plan, step_5_plan_confirmed. \
Set to null when severity is 'none'.
4. scope_gap, effort_gap, outcome_gap: one-sentence descriptions of the \
deviation for each facet. Use empty string (\"\") when there is no gap.

Return a single JSON object with exactly these keys:
  severity, direction, failure_precondition,
  scope_gap, effort_gap, outcome_gap

Do NOT wrap in Markdown fences. Do NOT add extra keys."""


# ---------------------------------------------------------------------------
# Pure-function severity / direction heuristic
# ---------------------------------------------------------------------------


def compute_severity_direction(
    *,
    status: Optional[str],
    total_reprompts: Optional[int],
    review_cycles: Optional[int],
    elapsed_days: Optional[float],
    expected_effort: Optional[str],
    expected_outcome: Optional[str],
    skip_reason: Optional[str],
) -> Tuple[str, str]:
    """Return ``(severity, direction)`` from actual-outcome metrics.

    Pure function, no I/O. Used as a pre-LLM baseline and as the sole
    severity source in offline mode (the fake client does not return
    rubric-driven JSON).

    Heuristic:

    * ``skip_reason`` non-empty → severity ``none``, direction
      ``ambiguous`` (no expectation to compare against).
    * ``status`` in {abandoned, open, stale} → severity ``critical``,
      direction ``under`` (the unit did not reach its expected
      outcome).
    * ``total_reprompts >= 10`` or ``review_cycles >= 5`` → severity
      ``major``, direction ``over`` (the user had to intervene far
      more than expected).
    * ``total_reprompts >= 4`` or ``review_cycles >= 2`` → severity
      ``minor``, direction ``over``.
    * Otherwise → severity ``none``, direction ``match``.

    The thresholds are intentionally coarse — the epic's "ship skeleton
    over plan to perfection" decision prior. Tune from X-4 corrections.
    """
    if skip_reason:
        return "none", "ambiguous"

    status_norm = (status or "").strip().lower()
    if status_norm in {"abandoned", "open", "stale", "stalled"}:
        return "critical", "under"

    reprompts = total_reprompts if isinstance(total_reprompts, (int, float)) else 0
    reviews = review_cycles if isinstance(review_cycles, (int, float)) else 0

    if reprompts >= 10 or reviews >= 5:
        return "major", "over"
    if reprompts >= 4 or reviews >= 2:
        return "minor", "over"
    return "none", "match"


# ---------------------------------------------------------------------------
# LLM plumbing
# ---------------------------------------------------------------------------


def _parse_llm_response(response_text: str) -> Optional[Dict[str, Any]]:
    """Parse the classifier's JSON response. Returns ``None`` on failure."""
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


def _coerce_failure_precondition(value: Any, severity: str) -> Optional[str]:
    """Coerce the LLM's ``failure_precondition`` into the allowed enum.

    Returns ``None`` when ``severity='none'`` (per the spec). Otherwise
    maps out-of-enum values to ``phase_0_setup`` — the catch-all
    "something upstream failed" bucket.
    """
    if severity == "none":
        return None
    if isinstance(value, str) and value in FAILURE_PRECONDITION_ENUM:
        return value
    # Unknown / missing → fall back to setup phase (root of all downstream
    # design-phase failures per idealized-workflow.md).
    return "phase_0_setup"


def _coerce_enum(value: Any, allowed: tuple[str, ...], fallback: str) -> str:
    if isinstance(value, str) and value in allowed:
        return value
    return fallback


# ---------------------------------------------------------------------------
# Input assembly
# ---------------------------------------------------------------------------


def _build_unit_input(
    expectation: Dict[str, Any],
    unit: Dict[str, Any],
    summary_text: Optional[str],
) -> str:
    """Assemble the LLM input for one unit.

    Combines the expectation facets with actual-outcome metrics and the
    summary text. Pure function — tests can feed hand-built dicts.
    """
    parts: List[str] = []
    parts.append(f"## Unit: {expectation['unit_id']}")
    parts.append(f"## Week: {expectation['week_start']}")
    parts.append("")
    parts.append("### Expectations (from X-1 at commitment point)")
    parts.append(
        f"- commitment_point: {expectation.get('commitment_point') or '(unknown)'}"
    )
    parts.append(
        f"- expected_scope: {expectation.get('expected_scope') or '(unknown)'}"
    )
    parts.append(
        f"- expected_effort: {expectation.get('expected_effort') or '(unknown)'}"
    )
    parts.append(
        f"- expected_outcome: {expectation.get('expected_outcome') or '(unknown)'}"
    )
    parts.append("")
    parts.append("### Actual outcome (from units table)")
    parts.append(f"- status: {unit.get('status')}")
    parts.append(f"- elapsed_days: {unit.get('elapsed_days')}")
    parts.append(f"- total_reprompts: {unit.get('total_reprompts')}")
    parts.append(f"- review_cycles: {unit.get('review_cycles')}")
    parts.append(f"- dark_time_pct: {unit.get('dark_time_pct')}")
    parts.append(f"- outlier_flags: {unit.get('outlier_flags')}")
    parts.append(f"- abandonment_flag: {unit.get('abandonment_flag')}")
    parts.append("")
    parts.append("### Unit summary (narrative)")
    parts.append(summary_text or "(no summary available)")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _load_expectations(
    exp_conn: sqlite3.Connection, week_start: str
) -> List[Dict[str, Any]]:
    rows = exp_conn.execute(
        "SELECT week_start, unit_id, commitment_point, expected_scope, "
        "       expected_effort, expected_outcome, confidence, skip_reason "
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
            "confidence": r[6],
            "skip_reason": r[7],
        }
        for r in rows
    ]


def _load_units_by_id(
    gh_conn: sqlite3.Connection, week_start: str
) -> Dict[str, Dict[str, Any]]:
    rows = gh_conn.execute(
        "SELECT unit_id, root_node_type, root_node_id, elapsed_days, "
        "       dark_time_pct, total_reprompts, review_cycles, status, "
        "       outlier_flags, abandonment_flag "
        "FROM units WHERE week_start = ?",
        (week_start,),
    ).fetchall()
    return {
        r[0]: {
            "unit_id": r[0],
            "root_node_type": r[1],
            "root_node_id": r[2],
            "elapsed_days": r[3],
            "dark_time_pct": r[4],
            "total_reprompts": r[5],
            "review_cycles": r[6],
            "status": r[7],
            "outlier_flags": r[8],
            "abandonment_flag": r[9],
        }
        for r in rows
    }


def _load_unit_summaries(
    gh_conn: sqlite3.Connection, week_start: str
) -> Dict[str, str]:
    try:
        rows = gh_conn.execute(
            "SELECT unit_id, summary_text FROM unit_summaries "
            "WHERE week_start = ?",
            (week_start,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {r[0]: r[1] for r in rows}


def _replace_gap_rows(
    exp_conn: sqlite3.Connection, week_start: str
) -> None:
    """DELETE existing gap rows for the week (idempotent re-run)."""
    exp_conn.execute(
        "DELETE FROM expectation_gaps WHERE week_start = ?", (week_start,)
    )
    exp_conn.commit()


def _insert_gap_row(
    exp_conn: sqlite3.Connection,
    *,
    week_start: str,
    unit_id: str,
    commitment_point: Optional[str],
    scope_gap: Optional[str],
    effort_gap: Optional[str],
    outcome_gap: Optional[str],
    severity: str,
    direction: str,
    failure_precondition: Optional[str],
) -> None:
    exp_conn.execute(
        "INSERT OR REPLACE INTO expectation_gaps "
        "(week_start, unit_id, commitment_point, scope_gap, effort_gap, "
        " outcome_gap, severity, direction, failure_precondition, "
        " computed_at, auto_confirmed) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 0)",
        (
            week_start,
            unit_id,
            commitment_point,
            scope_gap,
            effort_gap,
            outcome_gap,
            severity,
            direction,
            failure_precondition,
        ),
    )


def _auto_confirm_sweep(
    exp_conn: sqlite3.Connection, *, days: int = 14
) -> int:
    """Flip ``auto_confirmed=1`` on gap rows older than *days* days.

    Rows younger than *days* are unchanged. Returns the number of rows
    updated. Safe to call when the table is empty — returns 0.
    """
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=days)
    ).strftime("%Y-%m-%d %H:%M:%S")
    cur = exp_conn.execute(
        "UPDATE expectation_gaps SET auto_confirmed = 1 "
        "WHERE auto_confirmed = 0 AND computed_at IS NOT NULL "
        "  AND computed_at < ?",
        (cutoff,),
    )
    exp_conn.commit()
    return cur.rowcount or 0


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    week_start: str,
    *,
    github_db: str,
    expectations_db: str,
    config: Optional[SynthesisConfig] = None,
) -> int:
    """Compute gap rows for every unit with an expectations row in *week_start*.

    Parameters
    ----------
    week_start:
        ``YYYY-MM-DD`` anchor.
    github_db:
        Path to ``github.db`` (read-only for this module). Must contain
        ``units`` and, ideally, ``unit_summaries`` rows.
    expectations_db:
        Path to ``expectations.db``. Must already be initialized (the
        caller is expected to have run ``init_expectations_db``). Written
        to — both ``expectation_gaps`` and the auto-confirm sweep land
        here.
    config:
        Optional :class:`SynthesisConfig`. Required for the LLM call in
        live mode. When ``None`` (or in offline / fake-adapter mode) the
        pure-function severity heuristic is used without an LLM call.

    Returns
    -------
    Number of gap rows written for the week. ``0`` is a valid no-op
    (either no expectations rows exist for the week, or
    ``expectations.db`` has no ``expectation_gaps`` content post-sweep).
    """
    exp_conn = sqlite3.connect(str(expectations_db))
    gh_conn = sqlite3.connect(str(github_db))
    # Pre-flight: make sure the auto-confirm sweep does not fail with
    # ``no such table`` on a freshly-created DB whose X-2 slice has not
    # been applied. Caller SHOULD have run init already — this is belt +
    # braces.
    try:
        exp_conn.execute("SELECT 1 FROM expectation_gaps LIMIT 1").fetchall()
    except sqlite3.OperationalError:
        logger.warning(
            "expectation_gaps table missing — caller did not run "
            "init_expectations_db; skipping gap pass for week=%s",
            week_start,
        )
        exp_conn.close()
        gh_conn.close()
        return 0

    try:
        expectations = _load_expectations(exp_conn, week_start)
        if not expectations:
            logger.info(
                "No expectations rows for week=%s; gap pass is a no-op",
                week_start,
            )
            # Still run the auto-confirm sweep — old rows may still exist.
            _auto_confirm_sweep(exp_conn)
            return 0

        units_by_id = _load_units_by_id(gh_conn, week_start)
        summaries = _load_unit_summaries(gh_conn, week_start)

        _replace_gap_rows(exp_conn, week_start)

        # Select adapter lazily: offline mode uses the heuristic only and
        # never calls the LLM. Live mode calls the LLM for the facet
        # descriptions + failure_precondition attribution.
        adapter = None
        if config is not None:
            try:
                adapter = _get_adapter(config)
            except Exception as exc:  # noqa: BLE001 — don't break pipeline
                logger.warning(
                    "gap_analysis: adapter init failed (%s); falling back "
                    "to heuristic-only severity", exc,
                )
                adapter = None

        written = 0
        for exp in expectations:
            unit_id = exp["unit_id"]
            unit = units_by_id.get(unit_id, {})
            summary = summaries.get(unit_id)

            base_severity, base_direction = compute_severity_direction(
                status=unit.get("status"),
                total_reprompts=unit.get("total_reprompts"),
                review_cycles=unit.get("review_cycles"),
                elapsed_days=unit.get("elapsed_days"),
                expected_effort=exp.get("expected_effort"),
                expected_outcome=exp.get("expected_outcome"),
                skip_reason=exp.get("skip_reason"),
            )

            severity = base_severity
            direction = base_direction
            scope_gap_text: Optional[str] = None
            effort_gap_text: Optional[str] = None
            outcome_gap_text: Optional[str] = None
            failure_precondition: Optional[str] = None

            if adapter is not None:
                user_text = _build_unit_input(exp, unit, summary)
                try:
                    result = adapter.call(
                        _GAP_SYSTEM_PROMPT,
                        user_text,
                        config.model if config else "claude-sonnet-4-5",
                        _MAX_OUTPUT_TOKENS,
                    )
                    parsed = _parse_llm_response(result.text)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "gap_analysis LLM call failed for unit=%s: %s — "
                        "falling back to heuristic",
                        unit_id, exc,
                    )
                    parsed = None

                if parsed:
                    severity = _coerce_enum(
                        parsed.get("severity"), SEVERITY_ENUM, base_severity
                    )
                    direction = _coerce_enum(
                        parsed.get("direction"), DIRECTION_ENUM, base_direction
                    )
                    failure_precondition = _coerce_failure_precondition(
                        parsed.get("failure_precondition"), severity
                    )
                    scope_gap_text = (
                        parsed.get("scope_gap")
                        if isinstance(parsed.get("scope_gap"), str)
                        else None
                    )
                    effort_gap_text = (
                        parsed.get("effort_gap")
                        if isinstance(parsed.get("effort_gap"), str)
                        else None
                    )
                    outcome_gap_text = (
                        parsed.get("outcome_gap")
                        if isinstance(parsed.get("outcome_gap"), str)
                        else None
                    )

            # Offline / no-adapter / LLM-declined path: derive
            # failure_precondition from heuristic.
            if failure_precondition is None and severity != "none":
                failure_precondition = _heuristic_failure_precondition(
                    severity, direction, unit
                )

            _insert_gap_row(
                exp_conn,
                week_start=week_start,
                unit_id=unit_id,
                commitment_point=exp.get("commitment_point"),
                scope_gap=scope_gap_text,
                effort_gap=effort_gap_text,
                outcome_gap=outcome_gap_text,
                severity=severity,
                direction=direction,
                failure_precondition=failure_precondition,
            )
            written += 1

        exp_conn.commit()

        # Auto-confirm sweep — on every run, regardless of whether new
        # rows were written.
        swept = _auto_confirm_sweep(exp_conn)

        logger.info(
            "gap_analysis complete: week=%s gaps_written=%d auto_confirmed=%d",
            week_start, written, swept,
        )
        return written
    finally:
        exp_conn.close()
        gh_conn.close()


def _heuristic_failure_precondition(
    severity: str, direction: str, unit: Dict[str, Any]
) -> str:
    """Map a heuristic-only gap to a workflow step.

    Used when no LLM is available (offline mode) or the LLM declined.
    The mapping is intentionally coarse:

    * abandoned / stalled units → ``step_3_motivation_confirmed`` (the
      motivation evaporated after scoping revealed the true cost).
    * ``over`` direction with high reprompts → ``step_4_plan`` (the
      plan did not anticipate the required work).
    * ``over`` with moderate reprompts → ``step_5_plan_confirmed`` (the
      user accepted a plan they could not fully evaluate).
    * everything else → ``phase_0_setup`` (the catch-all "something
      upstream was missing" bucket).
    """
    status = (unit.get("status") or "").strip().lower()
    if status in {"abandoned", "stale", "stalled", "open"}:
        return "step_3_motivation_confirmed"
    reprompts = unit.get("total_reprompts") or 0
    if direction == "over" and reprompts >= 10:
        return "step_4_plan"
    if direction == "over":
        return "step_5_plan_confirmed"
    return "phase_0_setup"


def load_gap_rows(
    expectations_db: str,
    week_start: str,
    *,
    min_severity: Optional[tuple[str, ...]] = None,
) -> List[Dict[str, Any]]:
    """Return gap rows for *week_start*, optionally filtered by severity.

    *min_severity* is a tuple of severities to include (e.g.
    ``("major", "critical")`` for retrospective rendering). ``None``
    returns every row.
    """
    conn = sqlite3.connect(str(expectations_db))
    try:
        if min_severity is None:
            rows = conn.execute(
                "SELECT unit_id, commitment_point, scope_gap, effort_gap, "
                "       outcome_gap, severity, direction, "
                "       failure_precondition "
                "FROM expectation_gaps WHERE week_start = ? "
                "ORDER BY unit_id",
                (week_start,),
            ).fetchall()
        else:
            placeholders = ",".join("?" * len(min_severity))
            rows = conn.execute(
                f"SELECT unit_id, commitment_point, scope_gap, effort_gap, "
                f"       outcome_gap, severity, direction, "
                f"       failure_precondition "
                f"FROM expectation_gaps "
                f"WHERE week_start = ? AND severity IN ({placeholders}) "
                f"ORDER BY unit_id",
                [week_start, *min_severity],
            ).fetchall()
        return [
            {
                "unit_id": r[0],
                "commitment_point": r[1],
                "scope_gap": r[2],
                "effort_gap": r[3],
                "outcome_gap": r[4],
                "severity": r[5],
                "direction": r[6],
                "failure_precondition": r[7],
            }
            for r in rows
        ]
    finally:
        conn.close()


__all__ = [
    "FAILURE_PRECONDITION_ENUM",
    "SEVERITY_ENUM",
    "DIRECTION_ENUM",
    "compute_severity_direction",
    "load_gap_rows",
    "run",
]
