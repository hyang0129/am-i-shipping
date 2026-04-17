"""G-2 unit identifier (Epic #17 — Issue #37).

Walks ``graph_edges`` for a given ``week_start``, finds connected
components via a hand-rolled union-find (no networkx — same minimalism
convention as ``graph_builder``), materialises one row per component in
the append-only ``units`` table, and fills in the per-unit metrics
defined in ``synthesis.metrics``.

Key invariants
--------------
* **Deterministic unit IDs.** ``unit_id = sha256("|".join(sorted(node_ids)))[:16]``.
  Re-running over the same graph produces identical IDs.
* **Append-only.** Writes use ``INSERT OR IGNORE`` keyed on
  ``(week_start, unit_id)``. Re-running for the same ``week_start`` is a
  no-op — historical rows are preserved even if the underlying graph
  rotated.
* **Singleton friendly.** Nodes with no incident edges produce
  single-node units. Each still gets a deterministic ID, metrics, and
  status derived from its lone node.
* **Root pick is stable.** Priority ``issue > pr > commit > session``.
  Ties broken by sorting ``node_id`` — never depends on insertion order.

Status derivation
-----------------
A unit's ``status`` is a rough per-unit summary derived entirely from
the fixture/live data we already have:

* ``"open"``      — any issue/PR in the unit is currently open.
* ``"closed"``    — every issue/PR is closed (for PRs: ``merged_at`` set
  OR state == closed; no explicit ``state`` column — closed-unmerged PRs
  fall out of here because ``merged_at`` is NULL and the issue/PR date
  test fires the abandonment rule).
* ``"abandoned"`` — last activity older than 14 days AND no open items.
  ``cross_unit.py`` (Sub-Issue 6) refines this.

These values are coarse and deliberately conservative. Downstream
consumers treat them as hints; the true source of truth remains the
per-event data in ``issues``/``pull_requests``/``sessions``.
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

from synthesis import metrics


# Priority order for picking a unit's "root" node. Lower index = higher
# priority. ``session`` is last on purpose: sessions are the Claude-side
# artefact and tell us nothing about the WHY of the work, which is
# what a root label is supposed to summarise.
_ROOT_PRIORITY = {"issue": 0, "pr": 1, "commit": 2, "session": 3}

# Days of inactivity past which we mark a unit ``abandoned`` in the
# absence of any open item. Epic ADR default; cross_unit.py may refine.
_ABANDONMENT_DAYS_DEFAULT = 14


# ---------------------------------------------------------------------------
# Union-find
# ---------------------------------------------------------------------------


class _UnionFind:
    """Minimal union-find. ``parent[x] == x`` is the root.

    Path compression + union-by-size; both standard. No rank tracking —
    size is enough for the handful of nodes we ever see in one week.
    """

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._size: dict[str, int] = {}

    def add(self, x: str) -> None:
        if x not in self._parent:
            self._parent[x] = x
            self._size[x] = 1

    def find(self, x: str) -> str:
        # Iterative path compression to keep the stack bounded for
        # pathological chains, not that we expect any.
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Compress.
        cur = x
        while self._parent[cur] != root:
            self._parent[cur], cur = root, self._parent[cur]
        return root

    def union(self, a: str, b: str) -> None:
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Attach the smaller tree under the larger.
        if self._size[ra] < self._size[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        self._size[ra] += self._size[rb]

    def components(self) -> list[list[str]]:
        """Return components as a list of sorted node-id lists.

        The outer list is sorted by each component's smallest node_id so
        iteration order is deterministic.
        """
        groups: dict[str, list[str]] = {}
        for node in self._parent:
            groups.setdefault(self.find(node), []).append(node)
        return sorted(
            (sorted(group) for group in groups.values()),
            key=lambda g: g[0],
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unit_id_from_nodes(node_ids: list[str]) -> str:
    """Deterministic 16-char unit ID from the component's node IDs.

    ``sha256("|".join(sorted(node_ids)))[:16]``. The pipe separator
    prevents collisions between ``"abc"+"def"`` and ``"ab"+"cdef"`` if a
    future node naming scheme ever allowed concatenation ambiguity.
    """
    key = "|".join(sorted(node_ids))
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def _pick_root(
    nodes_in_unit: list[tuple[str, str, Optional[str]]],
) -> tuple[str, str]:
    """Return ``(root_node_type, root_node_id)`` for this unit.

    *nodes_in_unit* is a list of ``(node_id, node_type, node_ref)``
    tuples. We pick by priority first, then break ties by sorting
    ``node_id`` so the pick is independent of row insertion order.
    """
    best = None
    for node_id, node_type, _ref in nodes_in_unit:
        prio = _ROOT_PRIORITY.get(node_type or "", 99)
        key = (prio, node_id)
        if best is None or key < best[0]:
            best = (key, node_type or "", node_id)
    assert best is not None  # guaranteed by caller: components never empty
    return best[1], best[2]


def parse_repo_number(node_ref: Optional[str]) -> Optional[tuple[str, int]]:
    """Return ``(repo, number)`` for ``"owner/repo#N"`` refs, else None.

    Public helper — ``synthesis.unit_timeline`` imports this to avoid a
    duplicated copy.
    """
    if not node_ref or "#" not in node_ref:
        return None
    repo, _, num = node_ref.rpartition("#")
    try:
        return repo, int(num)
    except ValueError:
        return None


# Private alias kept so intra-module call sites below do not churn.
_parse_repo_number = parse_repo_number


# ---------------------------------------------------------------------------
# Status + metric aggregation for one unit
# ---------------------------------------------------------------------------


def _summarise_unit(
    unit_nodes: list[tuple[str, str, Optional[str]]],
    github_conn: sqlite3.Connection,
    sessions_conn: sqlite3.Connection,
    abandonment_days: int,
    now: datetime,
) -> dict:
    """Collect metric + status inputs for one unit into one dict.

    Returning a dict rather than a 7-tuple keeps the caller's row
    assembly readable (``row[5]`` vs ``row["dark_time_pct"]``).
    """
    # --- gather per-type membership -----------------------------------
    session_uuids: list[str] = []
    issue_refs: list[tuple[str, int]] = []  # (repo, issue_number)
    pr_refs: list[tuple[str, int]] = []     # (repo, pr_number)

    for _nid, ntype, nref in unit_nodes:
        if ntype == "session" and nref:
            session_uuids.append(nref)
        elif ntype == "issue":
            parsed = _parse_repo_number(nref)
            if parsed:
                issue_refs.append(parsed)
        elif ntype == "pr":
            parsed = _parse_repo_number(nref)
            if parsed:
                pr_refs.append(parsed)

    # --- pull source rows --------------------------------------------
    session_rows: list[tuple[Optional[str], Optional[str], int]] = []
    # (session_started_at, session_ended_at, reprompt_count)
    if session_uuids:
        placeholders = ",".join("?" * len(session_uuids))
        cur = sessions_conn.execute(
            f"SELECT session_started_at, session_ended_at, reprompt_count "
            f"FROM sessions WHERE session_uuid IN ({placeholders})",
            session_uuids,
        )
        session_rows = list(cur.fetchall())

    issue_rows: list[tuple[str, Optional[str], Optional[str], Optional[str]]] = []
    # (state, created_at, closed_at, updated_at)
    for repo, num in issue_refs:
        cur = github_conn.execute(
            "SELECT state, created_at, closed_at, updated_at "
            "FROM issues WHERE repo = ? AND issue_number = ?",
            (repo, num),
        )
        row = cur.fetchone()
        if row is not None:
            issue_rows.append(row)

    pr_rows: list[tuple[Optional[str], Optional[str], Optional[str]]] = []
    # (created_at, merged_at, updated_at)
    for repo, num in pr_refs:
        cur = github_conn.execute(
            "SELECT created_at, merged_at, updated_at "
            "FROM pull_requests WHERE repo = ? AND pr_number = ?",
            (repo, num),
        )
        row = cur.fetchone()
        if row is not None:
            pr_rows.append(row)

    # --- metrics ------------------------------------------------------
    all_timestamps: list[Optional[str]] = []
    for started, ended, _rc in session_rows:
        all_timestamps.append(started)
        all_timestamps.append(ended)
    for state, created, closed, updated in issue_rows:
        all_timestamps.extend((created, closed, updated))
    for created, merged, updated in pr_rows:
        all_timestamps.extend((created, merged, updated))

    elapsed = metrics.elapsed_days(all_timestamps)
    dark = metrics.dark_time_pct(
        [(s, e) for s, e, _ in session_rows]
    )
    reprompts = metrics.total_reprompts(session_uuids, sessions_conn)
    cycles = metrics.review_cycles(pr_refs, github_conn)

    # --- status -------------------------------------------------------
    has_open = False
    for state, *_ in issue_rows:
        if (state or "").lower() == "open":
            has_open = True
            break
    if not has_open:
        # A PR without merged_at that is referenced here is either
        # closed-unmerged or still open. We can't tell from the
        # pull_requests table (no ``state`` column in the schema), so we
        # treat missing ``merged_at`` as "open-ish" only when updated_at
        # is recent; otherwise it's stale/abandoned.
        for created, merged, updated in pr_rows:
            if merged:
                continue
            latest = metrics.parse_ts(updated) or metrics.parse_ts(created)
            if latest is None:
                continue
            if now - latest <= timedelta(days=abandonment_days):
                has_open = True
                break

    status = "closed"
    if has_open:
        status = "open"
    else:
        # Abandonment check: last activity older than N days AND unit
        # has any non-session content (singleton sessions don't qualify
        # as "abandoned" — they just completed).
        last_activity: Optional[datetime] = None
        for ts in all_timestamps:
            parsed = metrics.parse_ts(ts)
            if parsed is None:
                continue
            if last_activity is None or parsed > last_activity:
                last_activity = parsed
        has_tracked_work = bool(issue_rows or pr_rows)
        if (
            has_tracked_work
            and last_activity is not None
            and now - last_activity > timedelta(days=abandonment_days)
        ):
            status = "abandoned"

    return {
        "elapsed_days": elapsed,
        "dark_time_pct": dark,
        "total_reprompts": reprompts,
        "review_cycles": cycles,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def identify_units(
    github_db_path: Union[str, Path],
    sessions_db_path: Union[str, Path],
    week_start: str,
    *,
    abandonment_days: int = _ABANDONMENT_DAYS_DEFAULT,
    now: Optional[datetime] = None,
) -> int:
    """Populate ``units`` for *week_start*. Returns rows inserted.

    *now* is an injection point for tests that need to pin the
    abandonment cutoff. Defaults to ``datetime.utcnow()``.

    Parameters
    ----------
    github_db_path, sessions_db_path:
        Paths to the collector DBs. May be the same file (the fixture
        packs both schemas into one SQLite).
    week_start:
        YYYY-MM-DD anchor. Only ``graph_nodes``/``graph_edges`` rows
        with a matching ``week_start`` are considered.

    Side effects
    ------------
    Writes to ``units`` via ``INSERT OR IGNORE``. Safe to re-run:
    existing rows are preserved unchanged (append-only).
    """
    if now is None:
        # ``datetime.utcnow()`` is deprecated on 3.12+. Build a naive-UTC
        # datetime explicitly so the rest of this module's naive-vs-naive
        # arithmetic keeps working.
        now = datetime.now(timezone.utc).replace(tzinfo=None)

    gh_path = Path(github_db_path)
    sess_path = Path(sessions_db_path)
    gh = sqlite3.connect(str(gh_path))
    sess = gh if sess_path == gh_path else sqlite3.connect(str(sess_path))

    try:
        # --- read graph_nodes + graph_edges for this week -------------
        node_rows = gh.execute(
            "SELECT node_id, node_type, node_ref FROM graph_nodes "
            "WHERE week_start = ? ORDER BY node_id",
            (week_start,),
        ).fetchall()
        edge_rows = gh.execute(
            "SELECT src_node_id, dst_node_id FROM graph_edges "
            "WHERE week_start = ? ORDER BY src_node_id, dst_node_id",
            (week_start,),
        ).fetchall()

        if not node_rows:
            return 0

        # --- build components -----------------------------------------
        uf = _UnionFind()
        for node_id, _type, _ref in node_rows:
            uf.add(node_id)
        node_set = {nid for nid, *_ in node_rows}
        for src, dst in edge_rows:
            # Guard against edges that reference nodes outside this
            # week's partition. The graph builder shouldn't produce
            # these, but tolerating them keeps the identifier robust to
            # hand-edited debugging DBs.
            if src in node_set and dst in node_set:
                uf.union(src, dst)

        node_info = {nid: (nt, nr) for nid, nt, nr in node_rows}
        components = uf.components()

        # --- write one row per component -----------------------------
        inserted = 0
        for comp in components:
            unit_id = _unit_id_from_nodes(comp)
            nodes_in_unit = [(nid, *node_info[nid]) for nid in comp]
            root_type, root_id = _pick_root(nodes_in_unit)
            summary = _summarise_unit(
                nodes_in_unit,
                github_conn=gh,
                sessions_conn=sess,
                abandonment_days=abandonment_days,
                now=now,
            )
            cur = gh.execute(
                "INSERT OR IGNORE INTO units "
                "(week_start, unit_id, root_node_type, root_node_id, "
                " elapsed_days, dark_time_pct, total_reprompts, "
                " review_cycles, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    week_start,
                    unit_id,
                    root_type,
                    root_id,
                    summary["elapsed_days"],
                    summary["dark_time_pct"],
                    summary["total_reprompts"],
                    summary["review_cycles"],
                    summary["status"],
                ),
            )
            if cur.rowcount:
                inserted += 1
        gh.commit()
        return inserted
    finally:
        if sess is not gh:
            sess.close()
        gh.close()
