"""Epic #27 — X-5 (#76): calibration learning.

Once ≥20 user corrections have accumulated in ``expectation_corrections``,
this module performs two behaviours:

1. **Few-shot injection** (see :func:`build_few_shot_block`) — exposes a
   helper that assembles a Markdown block of the most recent user
   corrections. :mod:`synthesis.expectations` prepends that block to the
   classifier's user message so future extractions benefit from the
   accumulated signal.
2. **Calibration trends pass** (see :func:`run`) — joins
   ``expectation_corrections`` with ``github.db::issues.type_label`` to
   produce per-work-type averages of the user-supplied facet deltas.
   Results UPSERT into ``expectation_calibration_trends``. Below the
   threshold, the pass is a strict no-op.

Behavioural invariants (from the refined spec):

* **Gated activation.** Fewer than 20 ``corrected_by='user'`` rows means
  ``build_few_shot_block`` returns ``""`` and :func:`run` writes nothing
  and returns an empty dict. The pipeline behaves exactly as it did in
  X-1..X-4 v1.
* **User-only filter.** Auto-confirmed rows are excluded from the
  few-shot corpus and from the trend computation. This is the guard
  against the feared failure mode (calibrating on noise).
* **Deterministic selection.** Few-shot rows are sorted by
  ``corrected_at DESC, unit_id ASC`` so repeated runs pick the same
  five examples.
* **Idempotent writes.** Trend rows are upserted by
  ``(work_type, week_start)``; the retrospective ``.md`` is never
  rewritten.

Architecture notes
------------------
* Cross-database reads use ``ATTACH DATABASE`` — the intent document
  flagged this as implementer-owned. The calibration module owns the
  pattern here. ``type_label`` fallback is the literal ``"unknown"``.
* The public API is two functions: :func:`build_few_shot_block` (read-
  only helper consumed by :mod:`synthesis.expectations`) and :func:`run`
  (pipeline entry point invoked by :mod:`synthesis.weekly`).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


# X5-OQ-3 resolved: module constant (not a config value). Tweak in code
# if the first calibration pass produces noisy output.
CALIBRATION_MIN_CORRECTIONS = 20

# X5-OQ-1 resolved: five examples is 25% of the 20-row floor — large
# enough to signal, small enough to avoid bloating every per-unit call.
FEW_SHOT_SAMPLE_SIZE = 5

# Marker string the few-shot block uses. Exposed so tests can assert on
# it without hard-coding the exact Markdown heading.
FEW_SHOT_BLOCK_MARKER = "## Prior Corrections"

# The three calibration facets we report trends on in the retrospective.
# ``commitment_point`` is intentionally omitted from trend averaging —
# the value is a turn reference string, not a scalar we can average.
# We still include ``commitment_point`` rows in the few-shot corpus for
# calibration of the structural-detection heuristic.
TREND_FACETS = ("scope", "effort", "outcome")


# ---------------------------------------------------------------------------
# Correction corpus helpers
# ---------------------------------------------------------------------------


def _count_user_corrections(exp_conn: sqlite3.Connection) -> int:
    """Return the number of ``corrected_by='user'`` rows in the DB."""
    try:
        row = exp_conn.execute(
            "SELECT COUNT(*) FROM expectation_corrections "
            "WHERE corrected_by = 'user'"
        ).fetchone()
    except sqlite3.OperationalError:
        # Table missing (expectations.db not initialised yet). Treat as
        # "below threshold".
        return 0
    return int(row[0]) if row else 0


def _load_recent_user_corrections(
    exp_conn: sqlite3.Connection, limit: int = FEW_SHOT_SAMPLE_SIZE
) -> List[Dict[str, Any]]:
    """Return the *limit* most recent user corrections, newest first.

    Stable tie-break by ``unit_id ASC`` so the selection is deterministic
    when multiple rows share a ``corrected_at`` value (AS-5).
    """
    try:
        rows = exp_conn.execute(
            "SELECT week_start, unit_id, facet, original_value, "
            "       corrected_value, correction_note, corrected_at "
            "FROM expectation_corrections "
            "WHERE corrected_by = 'user' "
            "ORDER BY corrected_at DESC, unit_id ASC "
            "LIMIT ?",
            (limit,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    keys = (
        "week_start",
        "unit_id",
        "facet",
        "original_value",
        "corrected_value",
        "correction_note",
        "corrected_at",
    )
    return [dict(zip(keys, r)) for r in rows]


# ---------------------------------------------------------------------------
# Few-shot block assembly (consumed by synthesis.expectations)
# ---------------------------------------------------------------------------


def build_few_shot_block(expectations_db: str) -> str:
    """Return a Markdown few-shot block, or ``""`` below the threshold.

    Below ``CALIBRATION_MIN_CORRECTIONS`` user corrections this is a
    strict no-op (AS-1). At or above the threshold the block contains
    the ``FEW_SHOT_SAMPLE_SIZE`` most recent user corrections (AS-2,
    AS-5, AS-6).

    Auto-confirmed rows are excluded — they snapshot the original value
    unchanged and calibrating on them is the exact feared failure mode.
    Exclusion is logged at INFO the first time the block is assembled
    for a given run.
    """
    conn = sqlite3.connect(str(expectations_db))
    try:
        user_count = _count_user_corrections(conn)
        if user_count < CALIBRATION_MIN_CORRECTIONS:
            return ""
        examples = _load_recent_user_corrections(conn)
        if not examples:
            return ""

        # AS-6 diagnostic — count auto-confirm rows so the operator can
        # see they were excluded from the corpus.
        try:
            auto_row = conn.execute(
                "SELECT COUNT(*) FROM expectation_corrections "
                "WHERE corrected_by = 'auto_confirm'"
            ).fetchone()
            auto_count = int(auto_row[0]) if auto_row else 0
        except sqlite3.OperationalError:
            auto_count = 0
    finally:
        conn.close()

    logger.info(
        "Few-shot calibration active: %d user corrections found, using "
        "%d most recent (auto_confirm rows excluded: %d)",
        user_count,
        len(examples),
        auto_count,
    )

    lines: List[str] = [FEW_SHOT_BLOCK_MARKER, ""]
    lines.append(
        "The following are recent user corrections to prior expectation "
        "extractions. Use them as calibration examples for the facets "
        "the user tends to correct — match the granularity and phrasing "
        "of the corrected_value column when extracting new expectations."
    )
    lines.append("")
    for ex in examples:
        lines.append(f"- unit {ex['unit_id']} ({ex['week_start']}):")
        lines.append(f"    - facet: {ex['facet']}")
        orig = ex.get("original_value")
        corr = ex.get("corrected_value")
        lines.append(
            f"    - original: {orig if orig is not None else '(none)'}"
        )
        lines.append(
            f"    - corrected: {corr if corr is not None else '(none)'}"
        )
        note = ex.get("correction_note")
        if note:
            lines.append(f"    - note: {note}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Trend computation
# ---------------------------------------------------------------------------


def _joined_user_corrections(
    exp_conn: sqlite3.Connection, github_db: str
) -> List[Dict[str, Any]]:
    """Return user-correction rows joined with ``issues.type_label``.

    Uses ``ATTACH DATABASE`` to reach into ``github.db`` for the
    ``type_label``. ``unit_id`` encodes the root node; for issue-rooted
    units the format is ``issue:<repo>#<number>``. For units rooted at
    other node types (PR, session) the join falls through and the row
    is grouped under ``"unknown"`` (fallback).
    """
    # Attach github.db under a fixed alias. We detach in a finally to
    # leave the connection clean for further queries.
    exp_conn.execute("ATTACH DATABASE ? AS gh", (str(github_db),))
    try:
        # Join ``units`` on the expectations side's (week_start, unit_id)
        # to pick up the ``root_node_id``, then parse the repo/number out
        # of it and join ``gh.issues`` on that. Doing the parsing in SQL
        # is fiddly — easier to do the join in Python after a single
        # bulk fetch.
        rows = exp_conn.execute(
            "SELECT ec.week_start, ec.unit_id, ec.facet, "
            "       ec.original_value, ec.corrected_value, "
            "       ec.corrected_at, u.root_node_type, u.root_node_id "
            "FROM expectation_corrections ec "
            "LEFT JOIN gh.units u "
            "  ON u.week_start = ec.week_start "
            " AND u.unit_id = ec.unit_id "
            "WHERE ec.corrected_by = 'user'"
        ).fetchall()
        # Pre-fetch issues for a cheap in-memory lookup. A full week's
        # corrections hit at most a few tens of distinct issues.
        issue_rows = exp_conn.execute(
            "SELECT repo, issue_number, type_label FROM gh.issues"
        ).fetchall()
    finally:
        try:
            exp_conn.execute("DETACH DATABASE gh")
        except sqlite3.OperationalError:
            pass

    issue_type: Dict[tuple[str, int], Optional[str]] = {
        (r[0], int(r[1])): r[2] for r in issue_rows
    }

    out: List[Dict[str, Any]] = []
    for row in rows:
        (
            week_start,
            unit_id,
            facet,
            original_value,
            corrected_value,
            corrected_at,
            root_node_type,
            root_node_id,
        ) = row
        work_type: Optional[str] = None
        if root_node_type == "issue" and isinstance(root_node_id, str):
            # root_node_id format: "issue:<repo>#<number>"
            try:
                without_prefix = root_node_id[len("issue:"):]
                repo_part, issue_part = without_prefix.rsplit("#", 1)
                issue_number = int(issue_part)
                work_type = issue_type.get((repo_part, issue_number))
            except (ValueError, IndexError):
                work_type = None
        out.append(
            {
                "week_start": week_start,
                "unit_id": unit_id,
                "facet": facet,
                "original_value": original_value,
                "corrected_value": corrected_value,
                "corrected_at": corrected_at,
                "work_type": work_type or "unknown",
            }
        )
    return out


def _delta_signal(original: Optional[str], corrected: Optional[str]) -> Optional[float]:
    """Return a scalar signal for the user's correction on this facet.

    The stored values are free-text (scope/effort/outcome are strings in
    ``expectations``), so we cannot compute a numeric delta. We report
    a *change indicator*: ``+1.0`` when the user changed the value and
    ``0.0`` when they confirmed without change. The sign is by
    convention positive — direction would require parsing the text,
    which is out of scope for v1.

    Returns ``None`` when both sides are NULL — no signal at all.
    """
    if original is None and corrected is None:
        return None
    if (original or "").strip() == (corrected or "").strip():
        return 0.0
    return 1.0


def _compute_group_deltas(
    rows: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Aggregate joined rows into ``{work_type: {avg_*_delta, sample_count}}``.

    ``avg_*_delta`` is the mean of the per-row change indicator for that
    facet across all corrections in the work-type group. ``sample_count``
    is the total number of user-correction rows contributing to the
    group (across all facets).
    """
    grouped: Dict[str, Dict[str, List[float]]] = {}
    sample_counts: Dict[str, int] = {}
    for row in rows:
        wt = row["work_type"]
        grouped.setdefault(wt, {f: [] for f in TREND_FACETS})
        sample_counts[wt] = sample_counts.get(wt, 0) + 1
        facet = row["facet"]
        if facet not in TREND_FACETS:
            continue
        signal = _delta_signal(row["original_value"], row["corrected_value"])
        if signal is None:
            continue
        grouped[wt][facet].append(signal)

    out: Dict[str, Dict[str, Any]] = {}
    for wt, facet_map in grouped.items():
        entry: Dict[str, Any] = {"sample_count": sample_counts.get(wt, 0)}
        for facet in TREND_FACETS:
            values = facet_map[facet]
            entry[f"avg_{facet}_delta"] = (
                sum(values) / len(values) if values else None
            )
        out[wt] = entry
    return out


def _upsert_trends(
    exp_conn: sqlite3.Connection,
    week_start: str,
    trends: Dict[str, Dict[str, Any]],
    *,
    computed_at: Optional[str] = None,
) -> None:
    """UPSERT per-work-type trend rows for *week_start*."""
    ts = computed_at or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for work_type, entry in trends.items():
        exp_conn.execute(
            "INSERT INTO expectation_calibration_trends "
            "(work_type, week_start, avg_scope_delta, avg_effort_delta, "
            " avg_outcome_delta, sample_count, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(work_type, week_start) DO UPDATE SET "
            "  avg_scope_delta = excluded.avg_scope_delta, "
            "  avg_effort_delta = excluded.avg_effort_delta, "
            "  avg_outcome_delta = excluded.avg_outcome_delta, "
            "  sample_count = excluded.sample_count, "
            "  computed_at = excluded.computed_at",
            (
                work_type,
                week_start,
                entry.get("avg_scope_delta"),
                entry.get("avg_effort_delta"),
                entry.get("avg_outcome_delta"),
                entry.get("sample_count", 0),
                ts,
            ),
        )
    exp_conn.commit()


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run(
    week_start: str,
    *,
    github_db: str,
    expectations_db: str,
) -> Dict[str, Dict[str, Any]]:
    """Run the calibration trends pass for *week_start*.

    Returns the per-work-type trend dict. Below the correction threshold
    this is a strict no-op: returns ``{}`` without writing to
    ``expectation_calibration_trends`` (AS-1 / below-threshold scenario).
    At or above the threshold the trend rows are UPSERTed and the same
    dict is returned for downstream Markdown rendering.
    """
    conn = sqlite3.connect(str(expectations_db))
    try:
        user_count = _count_user_corrections(conn)
        if user_count < CALIBRATION_MIN_CORRECTIONS:
            logger.info(
                "Calibration pass skipped for week=%s: %d user corrections "
                "< threshold=%d",
                week_start,
                user_count,
                CALIBRATION_MIN_CORRECTIONS,
            )
            return {}
        joined = _joined_user_corrections(conn, github_db)
        trends = _compute_group_deltas(joined)
        if not trends:
            logger.info(
                "Calibration pass for week=%s produced no trend groups "
                "(no user corrections joined to a work type).",
                week_start,
            )
            return {}
        _upsert_trends(conn, week_start, trends)
        logger.info(
            "Calibration pass wrote %d trend rows for week=%s "
            "(user_corrections=%d)",
            len(trends),
            week_start,
            user_count,
        )
        return trends
    finally:
        conn.close()


def load_trends(
    expectations_db: str, week_start: str
) -> Dict[str, Dict[str, Any]]:
    """Load previously computed trend rows for *week_start*.

    Returns ``{work_type: {avg_scope_delta, avg_effort_delta,
    avg_outcome_delta, sample_count}}``. Empty dict when nothing has
    been written for the week.
    """
    conn = sqlite3.connect(str(expectations_db))
    try:
        try:
            rows = conn.execute(
                "SELECT work_type, avg_scope_delta, avg_effort_delta, "
                "       avg_outcome_delta, sample_count "
                "FROM expectation_calibration_trends "
                "WHERE week_start = ? "
                "ORDER BY sample_count DESC, work_type ASC",
                (week_start,),
            ).fetchall()
        except sqlite3.OperationalError:
            return {}
    finally:
        conn.close()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        out[r[0]] = {
            "avg_scope_delta": r[1],
            "avg_effort_delta": r[2],
            "avg_outcome_delta": r[3],
            "sample_count": r[4],
        }
    return out


def render_calibration_block(
    trends: Dict[str, Dict[str, Any]], *, top_n: int = 3
) -> List[str]:
    """Render the ``## Calibration Trends`` Markdown block.

    Returns a list of lines; empty list when ``trends`` is empty (caller
    omits the section entirely — AS-1 / below-threshold scenario).
    """
    if not trends:
        return []
    # Rank by the most pronounced correction rate across scope/effort/
    # outcome — higher average means the user changed the value more
    # often in that work-type.
    def _pronounced(entry: Dict[str, Any]) -> float:
        vals = [
            entry.get(f"avg_{f}_delta")
            for f in TREND_FACETS
        ]
        vals_f = [v for v in vals if isinstance(v, (int, float))]
        return max(vals_f) if vals_f else 0.0

    ranked = sorted(
        trends.items(),
        key=lambda kv: (_pronounced(kv[1]), kv[1].get("sample_count", 0)),
        reverse=True,
    )[: max(1, top_n)]

    lines: List[str] = ["", "## Calibration Trends (from X-5 calibration)", ""]
    for work_type, entry in ranked:
        sample = entry.get("sample_count", 0)
        parts: List[str] = []
        for facet in TREND_FACETS:
            v = entry.get(f"avg_{facet}_delta")
            if isinstance(v, (int, float)):
                parts.append(f"{facet}_correction_rate={v:.2f}")
        detail = ", ".join(parts) if parts else "no facet signal"
        lines.append(
            f"- {work_type}: {detail} (sample={sample})"
        )
    lines.append("")
    return lines


__all__ = [
    "CALIBRATION_MIN_CORRECTIONS",
    "FEW_SHOT_SAMPLE_SIZE",
    "FEW_SHOT_BLOCK_MARKER",
    "TREND_FACETS",
    "build_few_shot_block",
    "run",
    "load_trends",
    "render_calibration_block",
]
