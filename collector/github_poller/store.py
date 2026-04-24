"""SQLite persistence for GitHub issues, PRs, and linkage tables.

Provides idempotent upsert operations that use ``INSERT OR REPLACE``
(issues/PRs have composite primary keys on (repo, number)).

Validates required fields before writing — raises ``ValueError`` on
missing repo, number, or other mandatory columns rather than silently
inserting NULLs.

All public functions accept an optional ``conn`` parameter.  When
provided, the caller owns the connection lifecycle (open/commit/close)
and the function skips ``_connect()``/commit/close.  When ``None``
(default), each function opens its own connection, commits, and closes
— preserving backward compatibility.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def _connect(db_path: Path) -> sqlite3.Connection:
    """Open a connection and ensure the github.db schema exists."""
    from am_i_shipping.db import init_github_db

    db_path.parent.mkdir(parents=True, exist_ok=True)
    init_github_db(db_path)
    return sqlite3.connect(str(db_path))


def upsert_issue(
    repo: str,
    issue: Dict[str, Any],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or update a single issue row.

    Raises ``ValueError`` if *repo* or *issue["number"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    if "number" not in issue or issue["number"] is None:
        raise ValueError("issue number is required")

    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO issues (
                repo, issue_number, title, type_label, state,
                body, comments_json, created_at, closed_at, updated_at,
                state_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, issue_number) DO UPDATE SET
                title = excluded.title,
                type_label = excluded.type_label,
                state = excluded.state,
                body = excluded.body,
                comments_json = excluded.comments_json,
                created_at = excluded.created_at,
                closed_at = excluded.closed_at,
                updated_at = excluded.updated_at,
                state_reason = excluded.state_reason
            """,
            (
                repo,
                issue["number"],
                issue.get("title", ""),
                issue.get("type_label"),
                issue.get("state", ""),
                issue.get("body", ""),
                json.dumps(issue.get("comments", []), ensure_ascii=False),
                issue.get("created_at"),
                issue.get("closed_at"),
                issue.get("updated_at"),
                issue.get("state_reason", ""),
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def upsert_pr(
    repo: str,
    pr: Dict[str, Any],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or update a single PR row.

    Raises ``ValueError`` if *repo* or *pr["number"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    if "number" not in pr or pr["number"] is None:
        raise ValueError("pr number is required")

    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO pull_requests (
                repo, pr_number, head_ref, title, body,
                comments_json, review_comments_json, review_comment_count,
                push_count, created_at, merged_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, pr_number) DO UPDATE SET
                head_ref = excluded.head_ref,
                title = excluded.title,
                body = excluded.body,
                comments_json = excluded.comments_json,
                review_comments_json = excluded.review_comments_json,
                review_comment_count = excluded.review_comment_count,
                push_count = excluded.push_count,
                created_at = excluded.created_at,
                merged_at = excluded.merged_at,
                updated_at = excluded.updated_at
            """,
            (
                repo,
                pr["number"],
                pr.get("head_ref", ""),
                pr.get("title", ""),
                pr.get("body", ""),
                json.dumps(pr.get("comments", []), ensure_ascii=False),
                json.dumps(pr.get("review_comments", []), ensure_ascii=False),
                pr.get("review_comment_count", 0),
                pr.get("push_count", 0),
                pr.get("created_at"),
                pr.get("merged_at"),
                pr.get("updated_at"),
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def upsert_pr_issue_link(
    repo: str,
    pr_number: int,
    issue_number: int,
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Link a PR to an issue.  Idempotent (INSERT OR IGNORE)."""
    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO pr_issues (repo, pr_number, issue_number)
            VALUES (?, ?, ?)
            """,
            (repo, pr_number, issue_number),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def insert_issue_body_edit(
    repo: str,
    issue_number: int,
    edited_at: str,
    diff: Optional[str],
    editor: Optional[str],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record an issue body edit.  Idempotent (INSERT OR IGNORE)."""
    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO issue_body_edits
                (repo, issue_number, edited_at, diff, editor)
            VALUES (?, ?, ?, ?, ?)
            """,
            (repo, issue_number, edited_at, diff, editor),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def insert_issue_comment_edit(
    repo: str,
    issue_number: int,
    comment_id: int,
    edited_at: str,
    diff: Optional[str],
    editor: Optional[str],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record an issue comment edit.  Idempotent (INSERT OR IGNORE)."""
    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO issue_comment_edits
                (repo, issue_number, comment_id, edited_at, diff, editor)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (repo, issue_number, comment_id, edited_at, diff, editor),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def insert_pr_body_edit(
    repo: str,
    pr_number: int,
    edited_at: str,
    diff: Optional[str],
    editor: Optional[str],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record a PR body edit.  Idempotent (INSERT OR IGNORE)."""
    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO pr_body_edits
                (repo, pr_number, edited_at, diff, editor)
            VALUES (?, ?, ?, ?, ?)
            """,
            (repo, pr_number, edited_at, diff, editor),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def insert_pr_review_comment_edit(
    repo: str,
    pr_number: int,
    comment_id: int,
    edited_at: str,
    diff: Optional[str],
    editor: Optional[str],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Record a PR review comment edit.  Idempotent (INSERT OR IGNORE)."""
    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO pr_review_comment_edits
                (repo, pr_number, comment_id, edited_at, diff, editor)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (repo, pr_number, comment_id, edited_at, diff, editor),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Epic #17 — Sub-Issue 2/7 (#35): synthesis-table upserts
# ---------------------------------------------------------------------------
#
# ``commits`` and ``timeline_events`` rows are keyed on natural identifiers
# (``(repo, sha)`` and ``(repo, issue_number, event_id)`` respectively), so
# ``INSERT OR REPLACE`` is safe and idempotent — re-polling a PR that already
# has its commits in the table updates the message / pushed_at fields in place
# without creating duplicates. The same semantics hold for timeline events.


def upsert_commit(
    repo: str,
    commit: Dict[str, Any],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or update a single commit row.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    commit:
        Dict with keys: ``sha`` (required), ``author``, ``authored_at``,
        ``message``, ``pr_number``, ``pushed_at``. Missing optional keys are
        stored as NULL.
    db_path, conn:
        Same conventions as :func:`upsert_pr` — if *conn* is given the caller
        owns the lifecycle.

    Raises
    ------
    ValueError
        If *repo* or *commit["sha"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    sha = commit.get("sha")
    if not sha:
        raise ValueError("commit sha is required")

    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO commits (
                repo, sha, author, authored_at, message, pr_number, pushed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, sha) DO UPDATE SET
                author      = excluded.author,
                authored_at = excluded.authored_at,
                message     = excluded.message,
                pr_number   = excluded.pr_number,
                pushed_at   = excluded.pushed_at
            """,
            (
                repo,
                sha,
                commit.get("author"),
                commit.get("authored_at"),
                commit.get("message"),
                commit.get("pr_number"),
                commit.get("pushed_at"),
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()


def upsert_timeline_event(
    repo: str,
    event: Dict[str, Any],
    db_path: Union[str, Path],
    conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Insert or update a single timeline event row.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    event:
        Dict with keys: ``issue_number`` (required), ``event_id`` (required),
        ``event_type``, ``actor``, ``created_at``, ``payload_json``. Missing
        optional keys are stored as NULL; ``payload_json`` is expected to be a
        JSON string already (the fetcher serializes GraphQL payloads before
        handing them off).
    db_path, conn:
        Same conventions as :func:`upsert_pr`.

    Raises
    ------
    ValueError
        If *repo*, *event["issue_number"]*, or *event["event_id"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    issue_number = event.get("issue_number")
    if issue_number is None:
        raise ValueError("issue_number is required")
    event_id = event.get("event_id")
    if event_id is None:
        raise ValueError("event_id is required")

    own_conn = conn is None
    if own_conn:
        db_path = Path(db_path)
        conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO timeline_events (
                repo, issue_number, event_id, event_type,
                actor, created_at, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, issue_number, event_id) DO UPDATE SET
                event_type   = excluded.event_type,
                actor        = excluded.actor,
                created_at   = excluded.created_at,
                payload_json = excluded.payload_json
            """,
            (
                repo,
                issue_number,
                event_id,
                event.get("event_type"),
                event.get("actor"),
                event.get("created_at"),
                event.get("payload_json"),
            ),
        )
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()
