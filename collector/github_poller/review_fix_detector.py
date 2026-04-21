"""Issue #86: detect ``/review-fix`` cycles on polled PRs.

A ``/review-fix`` invocation posts a comment to the PR whose body starts
with the HTML marker ``<!-- review-fix-summary -->``. The skill then
pushes one or more fix commits. This module recognises that pair as a
single "review-fix cycle" event and emits one row per PR into the
``pr_review_fix_events`` table.

The detector runs after ``upsert_pr`` in :mod:`collector.github_poller.run`
so the ``comments_json`` column is already populated for the PR.

Design
------
* Offline / pure detection — no network calls. The comments list is
  handed in by the caller (scanned from ``pr["comments"]`` in the
  poller pipeline).
* ``fix_commit_count`` is counted from commits whose ``authored_at`` is
  strictly after the summary comment's ``createdAt``. When ``commits``
  is not supplied, the count falls back to ``0`` — a missing count is
  explicitly distinct from "no review-fix cycle" (the row is only
  emitted when a marker comment exists).
* When multiple marker comments are present on a PR (rare, but possible
  if ``/review-fix`` was re-run) we use the FIRST one. The later cycle's
  commits are still included in ``fix_commit_count`` because the cutoff
  is the earliest marker timestamp.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from loguru import logger


REVIEW_FIX_SUMMARY_MARKER = "<!-- review-fix-summary -->"


def find_review_fix_marker_comment(
    comments: Iterable[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return the earliest comment whose body contains the review-fix marker.

    ``comments`` is the list stored in ``pull_requests.comments_json`` —
    each entry is a dict with ``id`` / ``body`` / ``createdAt`` keys
    (GraphQL camelCase shape, matching what ``fetch_pr_comments`` emits).
    Returns ``None`` when no marker comment is present.

    When multiple markers are present, the one with the earliest
    ``createdAt`` wins. Comments lacking a ``createdAt`` sort last so a
    well-timestamped marker always beats an undated one.
    """
    matches: List[Dict[str, Any]] = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        body = c.get("body") or ""
        if REVIEW_FIX_SUMMARY_MARKER in body:
            matches.append(c)
    if not matches:
        return None
    # Empty createdAt sorts after populated timestamps via the two-tuple key.
    return sorted(
        matches,
        key=lambda c: (not bool(c.get("createdAt")), c.get("createdAt") or ""),
    )[0]


def count_fix_commits_after(
    commits: Optional[Iterable[Dict[str, Any]]],
    cutoff_iso: Optional[str],
) -> int:
    """Count commits whose ``authored_at`` is strictly greater than *cutoff_iso*.

    ISO-8601 lexicographic comparison works because our collector stores
    timestamps with a fixed ``YYYY-MM-DDTHH:MM:SSZ`` layout. Returns ``0``
    for empty or None inputs.
    """
    if not commits or not cutoff_iso:
        return 0
    count = 0
    for c in commits:
        if not isinstance(c, dict):
            continue
        authored_at = c.get("authored_at") or c.get("pushed_at") or ""
        if authored_at and authored_at > cutoff_iso:
            count += 1
    return count


def detect_review_fix_event(
    pr: Dict[str, Any],
    commits: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """Return a ``pr_review_fix_events`` row dict for *pr*, or ``None``.

    *pr* is the poller's PR dict with ``comments`` attached (the same
    list that will be serialised into ``comments_json`` by ``upsert_pr``).
    *commits* is the optional pre-fetched commit list from E-1 — when
    supplied, ``fix_commit_count`` is populated; when ``None``, the count
    is ``0`` (a row is still emitted so the metric can recognise the
    cycle, even if the count is unknown).

    The returned dict has keys: ``repo`` (caller fills this in),
    ``pr_number``, ``summary_comment_id``, ``posted_at``, ``fix_commit_count``.
    """
    marker = find_review_fix_marker_comment(pr.get("comments") or [])
    if marker is None:
        return None
    posted_at = marker.get("createdAt") or None
    # GraphQL comment ids are opaque strings (e.g. "IC_kwD..."); SQLite
    # stores them as TEXT-compatible INTEGER field only when they are
    # numeric. We've declared ``summary_comment_id INTEGER``; coerce if
    # possible, else store NULL with the body-detected presence being
    # enough to key the row.
    raw_id = marker.get("id")
    summary_id: Optional[int] = None
    if isinstance(raw_id, int):
        summary_id = raw_id
    elif isinstance(raw_id, str) and raw_id.isdigit():
        summary_id = int(raw_id)
    fix_count = count_fix_commits_after(commits, posted_at)
    return {
        "pr_number": pr["number"],
        "summary_comment_id": summary_id,
        "posted_at": posted_at,
        "fix_commit_count": fix_count,
    }


def upsert_pr_review_fix_event(
    repo: str,
    event: Dict[str, Any],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or replace a ``pr_review_fix_events`` row.

    Uses ``INSERT OR REPLACE`` keyed on ``(repo, pr_number)`` — re-polling
    the same PR refreshes ``fix_commit_count`` as more commits land.
    """
    if not repo:
        raise ValueError("repo is required")
    if "pr_number" not in event or event["pr_number"] is None:
        raise ValueError("pr_number is required")

    own_conn = conn is None
    if own_conn:
        from am_i_shipping.db import init_github_db

        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        init_github_db(db_path)
        conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO pr_review_fix_events (
                repo, pr_number, summary_comment_id, posted_at, fix_commit_count
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(repo, pr_number) DO UPDATE SET
                summary_comment_id = excluded.summary_comment_id,
                posted_at          = excluded.posted_at,
                fix_commit_count   = excluded.fix_commit_count
            """,
            (
                repo,
                event["pr_number"],
                event.get("summary_comment_id"),
                event.get("posted_at"),
                event.get("fix_commit_count") or 0,
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def process_pr(
    repo: str,
    pr: Dict[str, Any],
    commits: Optional[List[Dict[str, Any]]],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> bool:
    """High-level helper: detect + persist in one call.

    Returns ``True`` when a row was emitted, ``False`` when the PR had no
    marker comment. Logs at DEBUG on emit.
    """
    event = detect_review_fix_event(pr, commits)
    if event is None:
        return False
    try:
        upsert_pr_review_fix_event(repo, event, db_path, conn=conn)
    except Exception as exc:  # noqa: BLE001 — don't break the poll cycle
        logger.warning(
            "{}  pr_review_fix_event insert failed (PR #{}): {}",
            repo, pr.get("number"), exc,
        )
        return False
    logger.debug(
        "{}  pr_review_fix_event emitted (PR #{}, fix_commit_count={})",
        repo, pr.get("number"), event.get("fix_commit_count"),
    )
    return True
