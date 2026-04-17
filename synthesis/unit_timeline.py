"""G-3 unit timeline renderer (Epic #17 — Issue #37).

Pure function: given a unit's membership (derived by walking
``graph_edges`` from the unit's ``root_node_id`` or by looking up the
unit's component from the identifier), return a chronological list of
events — session start/end, issue opened/closed, PR opened/merged,
commits — suitable for LLM prompt assembly in Sub-Issue 5.

Shape
-----
Each event is a dict::

    {
        "timestamp": "2025-01-06T09:00:00Z",   # ISO-8601 string
        "type": "session_start",                # see TYPES below
        "node_id": "n-u1-sess-a",               # the node this event is about
        "description": "Session 00000000...101 started",
    }

TYPES
-----
``session_start`` / ``session_end``
    From ``sessions.session_started_at`` / ``session_ended_at``.
``issue_opened`` / ``issue_closed``
    From ``issues.created_at`` / ``issues.closed_at``.
``pr_opened`` / ``pr_merged``
    From ``pull_requests.created_at`` / ``pull_requests.merged_at``.
``commit_authored``
    From ``commits.authored_at``.

Ordering is by ``(timestamp, node_id, type)`` so ties are stable across
runs. Events with no timestamp are dropped — the renderer is a
timeline, not an inventory.
"""

from __future__ import annotations

import sqlite3
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# Node-ref parsing (mirrors unit_identifier)
# ---------------------------------------------------------------------------


def _parse_repo_number(node_ref: Optional[str]) -> Optional[tuple[str, int]]:
    if not node_ref or "#" not in node_ref:
        return None
    repo, _, num = node_ref.rpartition("#")
    try:
        return repo, int(num)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Per-type event collectors
# ---------------------------------------------------------------------------


def _session_events(
    node_id: str,
    uuid: str,
    sessions_conn: sqlite3.Connection,
) -> list[dict]:
    cur = sessions_conn.execute(
        "SELECT session_started_at, session_ended_at "
        "FROM sessions WHERE session_uuid = ?",
        (uuid,),
    )
    row = cur.fetchone()
    if row is None:
        return []
    started, ended = row
    events: list[dict] = []
    if started:
        events.append({
            "timestamp": started,
            "type": "session_start",
            "node_id": node_id,
            "description": f"Session {uuid} started",
        })
    if ended:
        events.append({
            "timestamp": ended,
            "type": "session_end",
            "node_id": node_id,
            "description": f"Session {uuid} ended",
        })
    return events


def _issue_events(
    node_id: str,
    repo: str,
    number: int,
    github_conn: sqlite3.Connection,
) -> list[dict]:
    cur = github_conn.execute(
        "SELECT title, created_at, closed_at FROM issues "
        "WHERE repo = ? AND issue_number = ?",
        (repo, number),
    )
    row = cur.fetchone()
    if row is None:
        return []
    title, created, closed = row
    events: list[dict] = []
    if created:
        events.append({
            "timestamp": created,
            "type": "issue_opened",
            "node_id": node_id,
            "description": f"Issue {repo}#{number} opened: {title or ''}".strip(),
        })
    if closed:
        events.append({
            "timestamp": closed,
            "type": "issue_closed",
            "node_id": node_id,
            "description": f"Issue {repo}#{number} closed",
        })
    return events


def _pr_events(
    node_id: str,
    repo: str,
    number: int,
    github_conn: sqlite3.Connection,
) -> list[dict]:
    cur = github_conn.execute(
        "SELECT title, created_at, merged_at FROM pull_requests "
        "WHERE repo = ? AND pr_number = ?",
        (repo, number),
    )
    row = cur.fetchone()
    if row is None:
        return []
    title, created, merged = row
    events: list[dict] = []
    if created:
        events.append({
            "timestamp": created,
            "type": "pr_opened",
            "node_id": node_id,
            "description": f"PR {repo}#{number} opened: {title or ''}".strip(),
        })
    if merged:
        events.append({
            "timestamp": merged,
            "type": "pr_merged",
            "node_id": node_id,
            "description": f"PR {repo}#{number} merged",
        })
    return events


def _commit_events(
    node_id: str,
    sha: str,
    github_conn: sqlite3.Connection,
) -> list[dict]:
    cur = github_conn.execute(
        "SELECT authored_at, message FROM commits WHERE sha = ?",
        (sha,),
    )
    row = cur.fetchone()
    if row is None:
        return []
    authored, message = row
    if not authored:
        return []
    # Truncate commit messages aggressively — the timeline is meant for
    # LLM consumption, and squash-merge commits can carry kilobytes of
    # body text (see ``commits.message`` note in db.py).
    summary = (message or "").splitlines()[0] if message else ""
    return [{
        "timestamp": authored,
        "type": "commit_authored",
        "node_id": node_id,
        "description": f"Commit {sha[:8]}: {summary}".strip().rstrip(":"),
    }]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def render_timeline(
    unit_nodes: Iterable[tuple[str, str, Optional[str]]],
    github_conn: sqlite3.Connection,
    sessions_conn: sqlite3.Connection,
) -> list[dict]:
    """Return chronological events for the nodes in *unit_nodes*.

    *unit_nodes* is an iterable of ``(node_id, node_type, node_ref)``
    tuples — same shape as the rows produced by
    ``unit_identifier._summarise_unit``. Callers typically resolve the
    nodes for a specific ``unit_id`` first (e.g. by re-running
    union-find over the graph for that week_start) and then pass the
    component in here.

    Returned dicts are sorted by ``(timestamp, node_id, type)`` so the
    output is deterministic — same inputs → byte-identical output,
    suitable for snapshot testing.
    """
    events: list[dict] = []
    for node_id, node_type, node_ref in unit_nodes:
        if node_type == "session" and node_ref:
            events.extend(_session_events(node_id, node_ref, sessions_conn))
        elif node_type == "issue":
            parsed = _parse_repo_number(node_ref)
            if parsed:
                repo, num = parsed
                events.extend(_issue_events(node_id, repo, num, github_conn))
        elif node_type == "pr":
            parsed = _parse_repo_number(node_ref)
            if parsed:
                repo, num = parsed
                events.extend(_pr_events(node_id, repo, num, github_conn))
        elif node_type == "commit" and node_ref:
            # ``node_ref`` for commits is ``"repo@sha"``; extract the
            # sha after the last ``@``.
            _, _, sha = node_ref.rpartition("@")
            if sha:
                events.extend(_commit_events(node_id, sha, github_conn))

    events.sort(key=lambda e: (e["timestamp"], e["node_id"], e["type"]))
    return events
