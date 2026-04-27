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
* **Issue/PR anchor required.** Components that contain neither an
  ``issue`` nor a ``pr`` node are dropped as noise and do not produce
  ``units`` rows. Session-only components (e.g. sessions with no linked
  issue or PR) are silently skipped.
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
    week_start: Optional[str] = None,
) -> dict:
    """Collect metric + status inputs for one unit into one dict.

    Returning a dict rather than a 7-tuple keeps the caller's row
    assembly readable (``row[5]`` vs ``row["dark_time_pct"]``).

    When the unit's anchor is an issue, session contributions to
    ``elapsed_days``, ``dark_time_pct``, ``total_reprompts``, and
    ``review_cycles`` are scaled by the session's ``fraction`` from
    ``session_issue_attribution`` (AS-5).  Sessions that have no
    attribution row (e.g. PR-rooted units) use fraction=1.0.
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
    # Also keep a per-uuid reprompt map for correct fractional scaling.
    session_reprompt_map: dict[str, int] = {}  # uuid -> reprompt_count
    if session_uuids:
        placeholders = ",".join("?" * len(session_uuids))
        cur = sessions_conn.execute(
            f"SELECT session_uuid, session_started_at, session_ended_at, reprompt_count "
            f"FROM sessions WHERE session_uuid IN ({placeholders})",
            session_uuids,
        )
        for uuid, started, ended, rc in cur.fetchall():
            session_rows.append((started, ended, rc))
            session_reprompt_map[uuid] = int(rc or 0)

    issue_rows: list[tuple[str, Optional[str], Optional[str], Optional[str], Optional[str]]] = []
    # (state, created_at, closed_at, updated_at, state_reason)
    for repo, num in issue_refs:
        cur = github_conn.execute(
            "SELECT state, created_at, closed_at, updated_at, state_reason "
            "FROM issues WHERE repo = ? AND issue_number = ?",
            (repo, num),
        )
        row = cur.fetchone()
        if row is not None:
            # Gracefully handle DBs without the state_reason column yet
            # (e.g. pre-migration fixtures). Pad with None if needed.
            if len(row) < 5:
                issue_rows.append((*row, None))  # type: ignore[arg-type]
            else:
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

    # --- fractional attribution lookup (AS-5) -------------------------
    # Build a per-session fraction map keyed by session_uuid.
    # When the unit is issue-rooted, look up session_issue_attribution for
    # the unit's anchor issue(s).  For non-issue-rooted units (PR-only),
    # default to 1.0 so behaviour is unchanged.
    session_fractions: dict[str, float] = {uuid: 1.0 for uuid in session_uuids}
    if issue_refs and session_uuids and week_start:
        # Use the first (highest-priority) issue ref as the anchor.
        anchor_repo, anchor_issue = issue_refs[0]
        try:
            placeholders = ",".join("?" * len(session_uuids))
            attr_rows = github_conn.execute(
                f"SELECT session_uuid, fraction "
                f"FROM session_issue_attribution "
                f"WHERE week_start = ? AND repo = ? AND issue_number = ? "
                f"  AND session_uuid IN ({placeholders})",
                [week_start, anchor_repo, anchor_issue] + list(session_uuids),
            ).fetchall()
            for uuid, frac in attr_rows:
                session_fractions[uuid] = frac
        except sqlite3.OperationalError:
            # Table not yet migrated — fall back to fraction=1.0.
            pass

    # --- metrics ------------------------------------------------------
    all_timestamps: list[Optional[str]] = []
    for started, ended, _rc in session_rows:
        all_timestamps.append(started)
        all_timestamps.append(ended)
    for state, created, closed, updated, _state_reason in issue_rows:
        all_timestamps.extend((created, closed, updated))
    for created, merged, updated in pr_rows:
        all_timestamps.extend((created, merged, updated))

    elapsed = metrics.elapsed_days(all_timestamps)
    dark = metrics.dark_time_pct(
        [(s, e) for s, e, _ in session_rows]
    )
    cycles = metrics.review_cycles(pr_refs, github_conn)

    # Apply fractional scaling (AS-5): scale session-derived metrics correctly
    # when sessions have different fractions.
    #
    # total_reprompts: weighted sum — each session's reprompt_count scaled by
    # its own fraction then summed.  This is exact regardless of whether
    # fractions differ across sessions.
    #
    # elapsed_days / dark_time_pct: computed from the union of all sessions'
    # timestamps, so per-session decomposition is lossy.  We apply avg_fraction
    # as an approximation; a code comment documents this.  When all fractions
    # agree the result is exact; in mixed-fraction cases it is a reasonable
    # approximation (proportional to the average attribution share).
    #
    # review_cycles: PR-level metric, not per-session — scale by avg_fraction
    # as an approximation (same reasoning as elapsed_days).
    if session_uuids:
        # Per-session weighted reprompts (exact).
        reprompts = round(
            sum(
                session_reprompt_map.get(u, 0) * session_fractions.get(u, 1.0)
                for u in session_uuids
            )
        )

        avg_fraction = (
            sum(session_fractions.get(u, 1.0) for u in session_uuids)
            / len(session_uuids)
        )
        # elapsed_days and dark_time_pct are union-of-sessions aggregates;
        # scaling by avg_fraction is an approximation (exact when all
        # fractions are equal, proportional otherwise).
        if avg_fraction < 1.0:
            if elapsed is not None:
                elapsed = elapsed * avg_fraction
            if dark is not None:
                dark = dark * avg_fraction
            if cycles is not None:
                cycles = round(cycles * avg_fraction)
    else:
        reprompts = 0

    # --- status -------------------------------------------------------
    # 6-status taxonomy (issue #98):
    #   open           — any anchor issue is still open
    #   shipped        — all closed, COMPLETED (or legacy empty) + merged linked PR
    #   completed-no-pr — all closed, COMPLETED + no merged linked PR
    #   not-planned    — all closed, NOT_PLANNED (regardless of PR linkage)
    #   closed-unknown — all closed, empty/NULL state_reason + no merged linked PR
    #   abandoned      — any open (or recent-PR) unit with no activity > N days
    #
    # ``open`` check: any anchor issue still open, OR any unmerged PR with
    # recent activity (same heuristic as before — no ``state`` col on PRs).
    has_open = False
    for state, *_ in issue_rows:
        if (state or "").lower() == "open":
            has_open = True
            break
    if not has_open and not issue_rows:
        # PR-only unit (no anchor issues): treat an unmerged PR with recent
        # activity as "open-ish" since there is no issue to tell us the true
        # state.  When there ARE closed anchor issues, the issue state wins —
        # we do not let an unmerged component PR override a closed issue.
        for created, merged, updated in pr_rows:
            if merged:
                continue
            latest = metrics.parse_ts(updated) or metrics.parse_ts(created)
            if latest is None:
                continue
            if now - latest <= timedelta(days=abandonment_days):
                has_open = True
                break

    if has_open:
        # Abandonment check applies to open units too: if the unit is
        # open but has had no activity for > N days, mark abandoned.
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
        else:
            status = "open"
    else:
        # All issues closed. Determine the most specific closed status.
        # For each closed issue check the state_reason and whether a
        # merged linked PR exists via the pr_closes_issue graph edges.
        #
        # Helper: does a merged linked PR exist for any anchor issue in
        # the unit?  We look up ``issue_has_pr`` edges in graph_edges
        # (joining through pull_requests to check merged_at).  The
        # graph_edges query is only run when we have issue_refs AND a
        # week_start, because graph edges are week-partitioned.
        #
        # Epic #93 / Slice 2: edge direction inverted from the legacy
        # ``pr_closes_issue`` (PR→issue) to ``issue_has_pr`` (issue→PR).
        # This is a LOGIC INVERSION: PR is now the dst, issue is now the
        # src. The traversal='own' filter is explicit per the BFS
        # invariant in Section 8 of the epic intent doc.
        def _has_merged_linked_pr() -> bool:
            if not issue_refs or not week_start:
                # Fall back: check if any PR in the component itself is merged.
                return any(merged for (_c, merged, _u) in pr_rows if merged)
            # Build the set of issue node IDs for the unit's anchor issues.
            issue_node_ids = {
                f"issue:{repo}#{num}" for repo, num in issue_refs
            }
            # Find PR node IDs that have an issue_has_pr edge originating
            # from one of our anchor issue nodes.
            placeholders = ",".join("?" * len(issue_node_ids))
            try:
                linked_pr_node_ids = {
                    row[0]
                    for row in github_conn.execute(
                        f"SELECT dst_node_id FROM graph_edges "
                        f"WHERE week_start = ? AND traversal = 'own' "
                        f"AND edge_type = 'issue_has_pr' "
                        f"AND src_node_id IN ({placeholders})",
                        [week_start, *issue_node_ids],
                    ).fetchall()
                }
            except sqlite3.OperationalError:
                # graph_edges table absent in test fixtures — fall back.
                return any(merged for (_c, merged, _u) in pr_rows if merged)

            if not linked_pr_node_ids:
                # No issue_has_pr edges — check component PRs directly.
                return any(merged for (_c, merged, _u) in pr_rows if merged)

            # Check if any of the linked PRs has merged_at set.
            for pr_node_id in linked_pr_node_ids:
                parsed = _parse_repo_number(pr_node_id.replace("pr:", "", 1))
                if parsed is None:
                    continue
                pr_repo, pr_num = parsed
                row = github_conn.execute(
                    "SELECT merged_at FROM pull_requests "
                    "WHERE repo = ? AND pr_number = ?",
                    (pr_repo, pr_num),
                ).fetchone()
                if row and row[0]:
                    return True
            return False

        # Determine dominant state_reason across anchor issues.
        # Priority: NOT_PLANNED > COMPLETED > empty (legacy)
        has_not_planned = False
        has_completed = False
        for _state, _created, _closed, _updated, sr in issue_rows:
            sr_upper = (sr or "").upper()
            if sr_upper == "NOT_PLANNED":
                has_not_planned = True
            elif sr_upper == "COMPLETED":
                has_completed = True

        if has_not_planned:
            # NOT_PLANNED takes precedence regardless of PR linkage.
            status = "not-planned"
        else:
            # Evaluate merged-PR linkage once (avoid double call).
            has_linked_merged_pr = _has_merged_linked_pr()
            if has_linked_merged_pr:
                # A merged linked PR means "shipped" — covers both COMPLETED
                # and legacy empty stateReason issues.
                status = "shipped"
            elif has_completed:
                # Explicitly COMPLETED but no merged linked PR.
                status = "completed-no-pr"
            else:
                # All closed, empty/NULL state_reason, no merged linked PR.
                status = "closed-unknown"

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
        # Epic #93 / Slice 2: union-find only walks ownership ('own') edges.
        # Cross-reference edges (commit_refs_issue, timeline_ref) are
        # excluded so unrelated units don't get spuriously merged via
        # informational links.
        edge_rows = gh.execute(
            "SELECT src_node_id, dst_node_id FROM graph_edges "
            "WHERE week_start = ? AND traversal = 'own' "
            "ORDER BY src_node_id, dst_node_id",
            (week_start,),
        ).fetchall()

        if not node_rows:
            return 0

        # --- compute root eligibility (issue #100 follow-up) ---------
        # An issue/PR node may be in this week's graph as *context* — e.g.
        # an in-week session commented on a past-closed issue, or an in-week
        # PR linked to a past-closed issue.  Those past-closed anchors must
        # not promote to unit roots: a "shipped this week" unit anchored on
        # an issue that closed three weeks ago inflates this week's velocity
        # and double-counts work already attributed to the earlier week.
        #
        # Eligibility rule: an issue/PR may anchor a unit for this week iff
        # its terminal state is unset (still open) or >= week_start. Past
        # closures/merges are kept in the component's node membership (so
        # session attribution and metrics are unaffected) but are excluded
        # from root selection and from the "has anchor" component test.
        eligible_anchors: set[str] = set()
        for node_id, node_type, node_ref in node_rows:
            if node_type not in ("issue", "pr"):
                continue
            parsed = _parse_repo_number(node_ref)
            if parsed is None:
                continue
            repo, num = parsed
            if node_type == "issue":
                row = gh.execute(
                    "SELECT closed_at FROM issues "
                    "WHERE repo = ? AND issue_number = ?",
                    (repo, num),
                ).fetchone()
                terminal_ts = row[0] if row else None
            else:  # pr
                row = gh.execute(
                    "SELECT merged_at FROM pull_requests "
                    "WHERE repo = ? AND pr_number = ?",
                    (repo, num),
                ).fetchone()
                terminal_ts = row[0] if row else None
            if terminal_ts is None or terminal_ts >= week_start:
                eligible_anchors.add(node_id)

        # --- build components (Epic #93 / Slice 2 — directed) ---------
        # Components are now computed as directed downward-reachability
        # from each eligible anchor. Two issues sharing a CHILD PR (the
        # original spurious-merge case Scenario 3 protects against) end
        # up in SEPARATE components: the PR is in both, but neither
        # issue can reach the other through the PR (edges are followed
        # src→dst only). When two anchors *can* reach each other (e.g. a
        # PR closes an issue, so issue→PR→… and the issue is reachable
        # downward from itself), they are merged via union-find on
        # anchors.
        node_set = {nid for nid, *_ in node_rows}
        node_info = {nid: (nt, nr) for nid, nt, nr in node_rows}

        # Directed adjacency: src → set(dsts).
        adj: dict[str, set[str]] = {}
        for src, dst in edge_rows:
            if src in node_set and dst in node_set:
                adj.setdefault(src, set()).add(dst)

        def _downward_reachable(seed: str) -> set[str]:
            seen = {seed}
            stack = [seed]
            while stack:
                cur = stack.pop()
                for nxt in adj.get(cur, ()):
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)
            return seen


        # Build a map of (session_uuid, issue_node_id) → True for every
        # session_issue_attribution row in this week.  Used below to detect
        # PR-only components whose sessions are fully accounted for by
        # issue-rooted components (AS-2, Shape A).
        # Key: session_uuid → set of issue node ids ("issue:<repo>#<N>")
        session_issue_nodes: dict[str, set[str]] = {}
        if week_start:
            try:
                for (uuid, repo_val, issue_num) in gh.execute(
                    "SELECT session_uuid, repo, issue_number "
                    "FROM session_issue_attribution "
                    "WHERE week_start = ?",
                    (week_start,),
                ).fetchall():
                    session_issue_nodes.setdefault(uuid, set()).add(
                        f"issue:{repo_val}#{issue_num}"
                    )
            except Exception:
                # Table absent (first-run or test DB without schema) — fall
                # back to empty dict; no components will be suppressed.
                pass

        # Pre-compute the set of issue node IDs that anchor their own
        # issue-rooted component.  A PR-only component is only dropped when
        # EVERY session in it is attributed exclusively to issues that are
        # already anchors of issue-rooted components.  Only *eligible*
        # issues (in-window or still-open) count as anchors — past-closed
        # issues stay in the graph as context but never anchor a unit.
        issue_anchor_nodes: set[str] = {
            nid for nid in eligible_anchors if node_info[nid][0] == "issue"
        }

        # --- compute directed components from anchors -----------------
        # For each eligible anchor, the component is the set of nodes
        # downward-reachable from it (via 'own' edges). Two anchors
        # share a component only when each can reach the other (mutual
        # reachability) — so two issues sharing a child PR end up in
        # SEPARATE components even though both contain the PR (Epic #93
        # Scenario 3). Non-anchor nodes (sessions, commits, child PRs)
        # may appear in multiple components — that's intentional, the
        # ``unit_id`` hash dedupes when two anchors produce identical
        # node sets.
        anchor_reach: dict[str, set[str]] = {}
        for nid in sorted(eligible_anchors):
            anchor_reach[nid] = _downward_reachable(nid)

        anchor_uf = _UnionFind()
        for nid in anchor_reach:
            anchor_uf.add(nid)
        anchor_list = list(anchor_reach.keys())
        # Compute, for each anchor, the set of OTHER anchors that can
        # reach it. An anchor with exactly one parent anchor merges into
        # that parent's component; an anchor with multiple parents stays
        # separate (Epic #93 Scenario 3 — two issues sharing a child PR
        # do NOT merge into one unit).
        parents_of: dict[str, list[str]] = {a: [] for a in anchor_list}
        for parent in anchor_list:
            for child in anchor_list:
                if parent == child:
                    continue
                if child in anchor_reach[parent]:
                    parents_of[child].append(parent)
        for child, parents in parents_of.items():
            if len(parents) == 1:
                anchor_uf.union(parents[0], child)
            # 0 parents: child is its own root. >1 parents: child stays
            # in its own component to prevent spurious merges.

        # One component per anchor-group. Group's nodes = union of all
        # anchors' downward-reachable sets in the group.
        anchor_groups: dict[str, list[str]] = {}
        for nid in anchor_reach:
            anchor_groups.setdefault(anchor_uf.find(nid), []).append(nid)

        comp_map: dict[str, set[str]] = {}
        for root, anchors in anchor_groups.items():
            group_nodes: set[str] = set()
            for a in anchors:
                group_nodes |= anchor_reach[a]
            comp_map[root] = group_nodes

        # Add orphan nodes (no anchor reaches them) as singletons so
        # the existing "drop components without an eligible anchor"
        # logic below still drops pure-session components.
        anchor_covered = set().union(*comp_map.values()) if comp_map else set()
        for nid in node_set:
            if nid not in anchor_covered:
                comp_map.setdefault(nid, set()).add(nid)

        components = sorted(
            (sorted(group) for group in comp_map.values()),
            key=lambda g: g[0],
        )

        # --- write one row per component -----------------------------
        inserted = 0
        for comp in components:
            # Drop components that have no eligible issue or PR anchor.
            # Components without any issue/PR are pure session noise (Issue
            # #66).  Components whose only issue/PR anchors are past-closed
            # — pulled in as context for an in-week session/PR — also drop
            # here: those represent prior-week work and would double-count
            # if promoted to a unit attributed to this week (Issue #100
            # follow-up).
            eligible_in_comp = [nid for nid in comp if nid in eligible_anchors]
            eligible_types_in_comp = {
                node_info[nid][0] for nid in eligible_in_comp
            }
            if not eligible_in_comp:
                continue

            # Drop PR-only components (no eligible issue anchor) when every
            # session in the component is attributed *only* to issues that
            # are already anchors of issue-rooted components this week.
            # Uses eligible-anchor types so a past-closed issue in the
            # component does not falsely satisfy the "has issue anchor"
            # condition.
            if "issue" not in eligible_types_in_comp and session_issue_nodes:
                session_nids_in_comp = {
                    nid for nid in comp if node_info[nid][0] == "session"
                }
                if session_nids_in_comp:
                    comp_session_uuids = {
                        node_info[nid][1] for nid in session_nids_in_comp
                    }
                    all_attributed_to_anchors = all(
                        uuid in session_issue_nodes
                        and session_issue_nodes[uuid].issubset(issue_anchor_nodes)
                        for uuid in comp_session_uuids
                    )
                    if all_attributed_to_anchors:
                        continue

            unit_id = _unit_id_from_nodes(comp)
            nodes_in_unit = [(nid, *node_info[nid]) for nid in comp]
            # Restrict root selection to eligible anchors. Past-closed
            # issues/PRs remain in nodes_in_unit (so summaries still see
            # the full membership for metrics) but cannot become the root.
            eligible_nodes_for_root = [
                (nid, ntype, nref)
                for (nid, ntype, nref) in nodes_in_unit
                if nid in eligible_anchors
            ]
            root_type, root_id = _pick_root(eligible_nodes_for_root)
            summary = _summarise_unit(
                nodes_in_unit,
                github_conn=gh,
                sessions_conn=sess,
                abandonment_days=abandonment_days,
                now=now,
                week_start=week_start,
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
