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
from typing import Iterable, Optional, Union

from synthesis import metrics


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
    github_conn: sqlite3.Connection,
    week_start: str,
    unit_id: str,
) -> Optional[datetime]:
    """Return the latest ``graph_nodes.created_at`` for *unit_id*.

    The mapping from unit to nodes is reconstructed via the usual
    identifier path: the component is the connected set of nodes
    referenced by ``graph_edges`` for the week, plus any singletons that
    hash into the same ``unit_id``. Reproducing union-find here would
    duplicate logic from :mod:`synthesis.unit_identifier`; instead we
    pull every node that appears in *any* edge whose other endpoint
    resolves into this unit, starting from the unit's ``root_node_id``
    and expanding. For fixtures (and real data) where singletons map
    1:1 to units, the root_node_id alone is sufficient.

    The query is simpler than a full component walk because the only
    decision the abandonment flag cares about is "does ANY node of this
    unit have a recent timestamp?" — a single row with a recent
    ``created_at`` short-circuits the answer.
    """
    # Pull every node for the week and every edge for the week; reuse
    # the small-scale graph in memory. Units rarely number in the
    # thousands per week, so the scan cost is negligible.
    nodes = {
        nid: created_at
        for nid, created_at in github_conn.execute(
            "SELECT node_id, created_at FROM graph_nodes "
            "WHERE week_start = ?",
            (week_start,),
        ).fetchall()
    }
    if not nodes:
        return None

    edges = github_conn.execute(
        "SELECT src_node_id, dst_node_id FROM graph_edges "
        "WHERE week_start = ?",
        (week_start,),
    ).fetchall()

    # Build an adjacency map then BFS from the unit's root node. We
    # look up the unit's root via the ``units`` row because
    # ``unit_id`` is a hash of the sorted node list — not directly
    # invertible.
    root_row = github_conn.execute(
        "SELECT root_node_id FROM units "
        "WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchone()
    if not root_row or not root_row[0]:
        return None
    root_id = root_row[0]
    if root_id not in nodes:
        return None

    adj: dict[str, set[str]] = {}
    for src, dst in edges:
        adj.setdefault(src, set()).add(dst)
        adj.setdefault(dst, set()).add(src)

    # BFS over the component.
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
        Units with no ``graph_nodes.created_at`` within the last
        ``abandonment_days`` days (relative to *now*) are flagged. Epic
        ADR default is ``14``.
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
        rows = conn.execute(
            "SELECT unit_id, elapsed_days, dark_time_pct, "
            "       total_reprompts, review_cycles "
            "FROM units WHERE week_start = ?",
            (week_start,),
        ).fetchall()

        if not rows:
            # No units to evaluate — nothing to do, not an error.
            return 0

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

            # Abandonment flag: 1 if no node event within cutoff.
            latest = _latest_node_ts(conn, week_start, unit_id)
            if latest is None or latest < cutoff:
                abandonment_flag = 1
            else:
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
