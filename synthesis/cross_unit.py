"""S-1 cross-unit outlier + abandonment flags (Epic #17 — Issue #38).

Runs AFTER :mod:`synthesis.unit_identifier` has populated the ``units``
table for a given ``week_start``. Produces two per-unit signals:

* ``outlier_flags``   — JSON-encoded list of metric names (one or more
  of ``elapsed_days``, ``dark_time_pct``, ``total_reprompts``,
  ``review_cycles``) on which the unit's value is strictly greater than
  ``median + outlier_sigma * stdev`` across *all* units for the same
  ``week_start``. Empty list ``"[]"`` means the unit was evaluated and
  cleared — distinct from NULL, which means the cross-unit pass has not
  yet run.
* ``abandonment_flag`` — ``1`` when the unit has no ``graph_nodes``
  event with a timestamp within the last ``abandonment_days`` days
  (relative to *now*), ``0`` when it has recent activity. Units with no
  parseable timestamp on any node count as abandoned — the absence of a
  dated signal is itself a signal.

Storage
-------
Both fields live as columns on the existing ``units`` row rather than a
sibling ``unit_flags`` table: every read site that already pulls
``units.*`` gets the flags for free, and there is no second table to
keep in sync with the append-only unit identity. The columns are added
by the idempotent ``_GITHUB_MIGRATIONS`` list in :mod:`am_i_shipping.db`.

Unlike :func:`synthesis.unit_identifier.identify_units`, which is
append-only (``INSERT OR IGNORE`` keyed on ``(week_start, unit_id)``),
this pass **overwrites** ``outlier_flags`` and ``abandonment_flag`` on
every call so cross-unit statistics always reflect the latest population
for ``week_start``. The unit identity row is still never rewritten —
only the two flag columns are updated.

Statistics
----------
Population standard deviation (not sample) is used so a week with a
single unit produces ``stdev == 0`` and — because the cutoff is strictly
``>`` — that lone unit is never flagged as an outlier against itself.
A metric column that contains only ``NULL`` values is skipped entirely;
otherwise ``NULL`` values are dropped from the population before the
median/stdev are computed.
"""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

from synthesis import metrics
from synthesis.unit_identifier import parse_repo_number


# Metrics that participate in outlier detection. Order is stable so the
# JSON-encoded ``outlier_flags`` column sorts deterministically and is
# readable by humans paging through the DB.
_OUTLIER_METRICS: tuple[str, ...] = (
    "elapsed_days",
    "dark_time_pct",
    "total_reprompts",
    "review_cycles",
)


def _median_and_stdev(values: list[float]) -> tuple[float, float]:
    """Return ``(median, population_stdev)`` for the given values.

    ``statistics.pstdev`` handles the ``len == 1`` case by returning
    ``0`` — we rely on that so a solo unit never flags itself.
    """
    med = statistics.median(values)
    # ``pstdev`` raises StatisticsError on empty; caller guards against
    # that with the ``if not values`` check below.
    sdev = statistics.pstdev(values)
    return med, sdev


def _latest_node_ts(
    root_id: Optional[str],
    nodes: dict[str, Optional[str]],
    adj: dict[str, set[str]],
) -> Optional[datetime]:
    """Return the most-recent node timestamp reachable from *root_id*.

    Walks every node reachable from the unit's ``root_node_id`` via
    ``graph_edges`` within the week (BFS over the adjacency map), parses
    each reachable node's timestamp via :func:`metrics.parse_ts`, and
    returns the latest naive-UTC ``datetime`` found.

    Returns ``None`` when ``root_id`` is falsy, is not present in
    *nodes* (orphaned unit), or no reachable node has a parseable
    timestamp. Callers treat ``None`` as "abandoned".

    Pure function — no DB access. The caller hoists the
    ``graph_nodes`` / ``graph_edges`` reads out of the per-unit loop in
    :func:`compute_flags` and passes *nodes* + *adj* in so one scan
    serves every unit for the week (F-1-1). ``nodes`` maps
    ``node_id -> ts`` where *ts* is the **first non-NULL value** from a
    fixed priority chain: for ``issue`` nodes,
    ``COALESCE(closed_at, updated_at, created_at)``; for ``pr`` nodes,
    ``COALESCE(merged_at, updated_at, created_at)``; falling back to
    ``graph_nodes.created_at`` when no source row is found. This is a
    priority-ordered COALESCE — not a MAX — so ``closed_at``/``merged_at``
    takes precedence over a later ``updated_at`` (e.g. a comment added
    after close). ``adj`` is an undirected adjacency
    ``node_id -> set[node_id]``.
    """
    if not root_id or root_id not in nodes:
        return None

    # BFS over the component starting from the unit's root node.
    seen = {root_id}
    stack = [root_id]
    while stack:
        cur = stack.pop()
        for nxt in adj.get(cur, ()):
            if nxt not in seen and nxt in nodes:
                seen.add(nxt)
                stack.append(nxt)

    # Parse timestamps and track the latest.
    latest: Optional[datetime] = None
    for nid in seen:
        ts = metrics.parse_ts(nodes.get(nid))
        if ts is None:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest


def compute_flags(
    github_db_path: Union[str, Path],
    week_start: str,
    outlier_sigma: float = 2.0,
    abandonment_days: int = 14,
    *,
    now: Optional[datetime] = None,
) -> int:
    """Populate ``outlier_flags`` + ``abandonment_flag`` for *week_start*.

    Returns the number of ``units`` rows updated. Safe to re-run:
    every row for the week is UPDATEd each call, overwriting any prior
    pass for the same ``week_start``.

    The return value counts rows whose UPDATE's WHERE clause matched —
    i.e. every ``units`` row for ``week_start`` — and does **not**
    filter to "rows whose column values actually changed". Re-running
    with identical inputs therefore returns the same count as the first
    run. This mirrors SQLite's ``Cursor.rowcount`` semantics on UPDATE.

    Parameters
    ----------
    github_db_path:
        Path to ``github.db`` (the DB that holds ``units`` and the
        ``graph_nodes`` / ``graph_edges`` this function reads to
        evaluate abandonment).
    week_start:
        ``YYYY-MM-DD`` anchor. Only ``units`` rows for this week are
        considered — the median/stdev are computed over this week's
        population, not the lifetime population.
    outlier_sigma:
        A unit's metric value is flagged as an outlier when strictly
        greater than ``median + outlier_sigma * stdev``. Defaults to
        the epic ADR value of ``2.0``. Matches
        :class:`am_i_shipping.config_loader.SynthesisConfig.outlier_sigma`.
    abandonment_days:
        Units with no activity within the last ``abandonment_days`` days
        (relative to *now*) are flagged. For issue and PR nodes, activity
        is ``COALESCE(closed_at/merged_at, updated_at, created_at)`` from
        the source table; other node types fall back to
        ``graph_nodes.created_at``. Epic ADR default is ``14``.
    now:
        Injection point for tests. Defaults to
        ``datetime.now(timezone.utc)``. Aware datetimes are normalised
        to naive UTC before comparison with parsed node timestamps.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    # Normalise to naive UTC so the timedelta comparison matches the
    # naive datetimes produced by :func:`metrics.parse_ts`.
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc).replace(tzinfo=None)

    cutoff = now - timedelta(days=abandonment_days)

    conn = sqlite3.connect(str(github_db_path))
    try:
        # Pull the per-unit metrics AND the root_node_id in one scan.
        # Historically ``root_node_id`` was re-queried per unit inside
        # ``_latest_node_ts``; hoisting it into this single SELECT removes
        # an N+1 round trip against the ``units`` table.
        rows = conn.execute(
            "SELECT unit_id, elapsed_days, dark_time_pct, "
            "       total_reprompts, review_cycles, root_node_id "
            "FROM units WHERE week_start = ?",
            (week_start,),
        ).fetchall()

        if not rows:
            # No units to evaluate — nothing to do, not an error.
            return 0

        # --- shared graph_nodes / graph_edges scan (F-1-1) -----------
        # These queries return the same data for every unit in the
        # week, so we execute them once and pass the materialised dicts
        # into ``_latest_node_ts`` below. Previously the per-unit loop
        # re-queried them, producing O(N) redundant DB round trips.
        raw_nodes: list[tuple[str, str, Optional[str], Optional[str]]] = conn.execute(
            "SELECT node_id, node_type, node_ref, created_at FROM graph_nodes "
            "WHERE week_start = ?",
            (week_start,),
        ).fetchall()

        # Collect issue and PR node_refs for the batch lookups below.
        # node_ref format: "{repo}#{number}" (e.g. "owner/repo#42")
        issue_refs: set[str] = set()
        pr_refs: set[str] = set()
        for _nid, ntype, nref, _cat in raw_nodes:
            if nref and ntype == "issue":
                issue_refs.add(nref)
            elif nref and ntype == "pr":
                pr_refs.add(nref)

        # For each issue node_ref, fetch COALESCE(closed_at, updated_at, created_at).
        # node_ref = "{repo}#{number}", split on "#" with rpartition.
        issue_activity: dict[str, str] = {}
        for ref in issue_refs:
            parsed = parse_repo_number(ref)
            if parsed is None:
                continue
            repo, num = parsed
            row = conn.execute(
                "SELECT COALESCE(closed_at, updated_at, created_at) "
                "FROM issues WHERE repo = ? AND issue_number = ?",
                (repo, num),
            ).fetchone()
            if row and row[0] is not None:
                issue_activity[ref] = row[0]

        pr_activity: dict[str, str] = {}
        for ref in pr_refs:
            parsed = parse_repo_number(ref)
            if parsed is None:
                continue
            repo, num = parsed
            row = conn.execute(
                "SELECT COALESCE(merged_at, updated_at, created_at) "
                "FROM pull_requests WHERE repo = ? AND pr_number = ?",
                (repo, num),
            ).fetchone()
            if row and row[0] is not None:
                pr_activity[ref] = row[0]

        # Build the activity-aware nodes map.
        nodes: dict[str, Optional[str]] = {}
        for nid, ntype, nref, created_at in raw_nodes:
            if ntype == "issue" and nref and nref in issue_activity:
                nodes[nid] = issue_activity[nref]
            elif ntype == "pr" and nref and nref in pr_activity:
                nodes[nid] = pr_activity[nref]
            else:
                nodes[nid] = created_at

        # Epic #93 / Slice 2: walk only ownership ('own') edges. The
        # adjacency is symmetric (we add both directions below) for the
        # latest-activity propagation, but the *set* of edges considered is
        # restricted to ownership so cross-references can't pull stale
        # timestamps in across unrelated units.
        adj: dict[str, set[str]] = {}
        for src, dst in conn.execute(
            "SELECT src_node_id, dst_node_id FROM graph_edges "
            "WHERE week_start = ? AND traversal = 'own'",
            (week_start,),
        ).fetchall():
            adj.setdefault(src, set()).add(dst)
            adj.setdefault(dst, set()).add(src)

        # --- per-metric thresholds -----------------------------------
        # Build one list per metric, dropping NULLs. Thresholds are
        # computed only for metrics that have any non-null value for
        # this week; metrics that are entirely NULL are skipped so a
        # stubbed-out metric can't drag every unit into the flagged
        # column.
        per_metric_values: dict[str, list[float]] = {
            name: [] for name in _OUTLIER_METRICS
        }
        # row index 0 is unit_id; metrics start at index 1.
        for row in rows:
            for i, name in enumerate(_OUTLIER_METRICS, start=1):
                v = row[i]
                if v is None:
                    continue
                # ``float()`` is safe for both INTEGER and REAL SQLite
                # storage classes. NaN is dropped because any
                # comparison against NaN is False anyway.
                fv = float(v)
                if math.isnan(fv):
                    continue
                per_metric_values[name].append(fv)

        thresholds: dict[str, float] = {}
        for name, vals in per_metric_values.items():
            if not vals:
                continue
            median, sdev = _median_and_stdev(vals)
            thresholds[name] = median + outlier_sigma * sdev

        # --- per-unit evaluation -------------------------------------
        updated = 0
        for row in rows:
            unit_id = row[0]
            root_node_id = row[5]

            # Outlier flags: list of metric names that breach the cutoff.
            flagged: list[str] = []
            for i, name in enumerate(_OUTLIER_METRICS, start=1):
                if name not in thresholds:
                    continue
                v = row[i]
                if v is None:
                    continue
                fv = float(v)
                if math.isnan(fv):
                    continue
                if fv > thresholds[name]:
                    flagged.append(name)
            # Sort so ordering is deterministic regardless of the
            # ``_OUTLIER_METRICS`` tuple order in future refactors.
            flagged.sort()
            outlier_flags_json = json.dumps(flagged)

            # Issue #98: ``abandonment_flag`` is retired as the source of
            # truth for abandoned units.  ``status == "abandoned"`` from
            # ``_summarise_unit`` is now the canonical signal.  The column
            # is kept for backward compatibility but is always written as 0
            # so downstream consumers fall through to ``status``.
            abandonment_flag = 0

            cur = conn.execute(
                "UPDATE units "
                "SET outlier_flags = ?, abandonment_flag = ? "
                "WHERE week_start = ? AND unit_id = ?",
                (
                    outlier_flags_json,
                    abandonment_flag,
                    week_start,
                    unit_id,
                ),
            )
            if cur.rowcount:
                updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()


__all__ = ["compute_flags"]
