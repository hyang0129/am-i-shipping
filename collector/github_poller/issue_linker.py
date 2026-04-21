"""Link sessions to issues via session_gh_events.

After issue upsert, queries ``session_gh_events`` for rows where
``event_type IN ('issue_create', 'issue_comment')`` and ``repo`` matches.
Inserts matched ``(repo, issue_number, session_uuid)`` into ``issue_sessions``
(idempotent via ``INSERT OR IGNORE``).

Unlike PR linking (which needs branch matching), issue linking is direct:
a session that ran ``gh issue create`` or ``gh issue comment`` for a repo
already has an explicit row in ``session_gh_events`` with the issue number
as ``ref``.

When ``sessions.db`` does not exist or is empty, exits cleanly — does not
block the poller.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Union

from loguru import logger


def link_issues(
    repo: str,
    github_db_path: Union[str, Path],
    sessions_db_path: Union[str, Path],
) -> int:
    """Link sessions to issues for a given repo.

    Returns total number of issue-session links for this repo.
    """
    sessions_db_path = Path(sessions_db_path)
    github_db_path = Path(github_db_path)

    if not sessions_db_path.exists():
        logger.warning("sessions.db not found at {} — issue linkage skipped for {}", sessions_db_path, repo)
        return 0

    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        rows = gh_conn.execute(
            """
            SELECT DISTINCT session_uuid, CAST(ref AS INTEGER) AS issue_number
            FROM session_gh_events
            WHERE repo = ?
              AND event_type IN ('issue_create', 'issue_comment')
              AND ref != 'pending'
              AND ref != ''
              AND CAST(ref AS INTEGER) > 0
            """,
            (repo,),
        ).fetchall()

        if not rows:
            return 0

        for session_uuid, issue_number in rows:
            try:
                gh_conn.execute(
                    """
                    INSERT OR IGNORE INTO issue_sessions
                        (repo, issue_number, session_uuid)
                    VALUES (?, ?, ?)
                    """,
                    (repo, issue_number, session_uuid),
                )
            except sqlite3.IntegrityError:
                pass
        gh_conn.commit()

        count = gh_conn.execute(
            "SELECT COUNT(*) FROM issue_sessions WHERE repo = ?",
            (repo,),
        ).fetchone()[0]
    finally:
        gh_conn.close()

    logger.info(
        "{}  issue-session links: {} events → {} total links",
        repo, len(rows), count,
    )
    return count
