"""SQLite persistence for GitHub issues, PRs, and linkage tables.

Provides idempotent upsert operations that use ``INSERT OR REPLACE``
(issues/PRs have composite primary keys on (repo, number)).

Validates required fields before writing — raises ``ValueError`` on
missing repo, number, or other mandatory columns rather than silently
inserting NULLs.
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
) -> None:
    """Insert or update a single issue row.

    Raises ``ValueError`` if *repo* or *issue["number"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    if "number" not in issue or issue["number"] is None:
        raise ValueError("issue number is required")

    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO issues (
                repo, issue_number, title, type_label, state,
                body, comments_json, created_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, issue_number) DO UPDATE SET
                title = excluded.title,
                type_label = excluded.type_label,
                state = excluded.state,
                body = excluded.body,
                comments_json = excluded.comments_json,
                created_at = excluded.created_at,
                closed_at = excluded.closed_at
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
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_pr(
    repo: str,
    pr: Dict[str, Any],
    db_path: Union[str, Path],
) -> None:
    """Insert or update a single PR row.

    Raises ``ValueError`` if *repo* or *pr["number"]* is missing.
    """
    if not repo:
        raise ValueError("repo is required")
    if "number" not in pr or pr["number"] is None:
        raise ValueError("pr number is required")

    db_path = Path(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO pull_requests (
                repo, pr_number, head_ref, title, body,
                review_comments_json, review_comment_count,
                push_count, created_at, merged_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, pr_number) DO UPDATE SET
                head_ref = excluded.head_ref,
                title = excluded.title,
                body = excluded.body,
                review_comments_json = excluded.review_comments_json,
                review_comment_count = excluded.review_comment_count,
                push_count = excluded.push_count,
                created_at = excluded.created_at,
                merged_at = excluded.merged_at
            """,
            (
                repo,
                pr["number"],
                pr.get("head_ref", ""),
                pr.get("title", ""),
                pr.get("body", ""),
                json.dumps(pr.get("review_comments", []), ensure_ascii=False),
                pr.get("review_comment_count", 0),
                pr.get("push_count", 0),
                pr.get("created_at"),
                pr.get("merged_at"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_pr_issue_link(
    repo: str,
    pr_number: int,
    issue_number: int,
    db_path: Union[str, Path],
) -> None:
    """Link a PR to an issue.  Idempotent (INSERT OR IGNORE)."""
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
        conn.commit()
    finally:
        conn.close()
