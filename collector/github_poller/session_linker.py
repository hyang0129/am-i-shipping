"""Link PRs to Claude Code sessions via matching head_ref to git_branch.

After PR upsert, queries ``sessions.db`` for sessions whose
``git_branch`` matches the PR's ``head_ref`` AND which have an entry
in ``github.db::session_gh_events`` for the same repo.  Inserts
matched pairs into ``pr_sessions`` (idempotent via ``INSERT OR IGNORE``).

Using ``session_gh_events`` as the repo signal (instead of the old
``working_directory`` substring check) is strictly more accurate: it
requires observed evidence that the session issued a ``gh`` command
targeting the repo, rather than guessing from the local checkout path.
This means local clone names (e.g. ``claude-rts`` for the GitHub repo
``supreme-claudemander``) no longer cause missed links. See issue #83.

When ``sessions.db`` does not exist or is empty, exits cleanly with
zero rows inserted — it does not block the poller.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Union

from loguru import logger


def link_sessions(
    repo: str,
    github_db_path: Union[str, Path],
    sessions_db_path: Union[str, Path],
) -> int:
    """Link PRs to sessions for a given repo.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    github_db_path:
        Path to github.db.
    sessions_db_path:
        Path to sessions.db.

    Returns
    -------
    Total number of PR-session links for this repo (0 if sessions.db
    is absent or empty, or if no matches are found).
    """
    sessions_db_path = Path(sessions_db_path)
    github_db_path = Path(github_db_path)

    # Exit cleanly if sessions.db does not exist
    if not sessions_db_path.exists():
        logger.warning("sessions.db not found at {} — session linkage skipped for {}", sessions_db_path, repo)
        return 0

    # Get all PRs for this repo that have a head_ref
    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        prs = gh_conn.execute(
            "SELECT pr_number, head_ref FROM pull_requests WHERE repo = ? AND head_ref != ''",
            (repo,),
        ).fetchall()
    finally:
        gh_conn.close()

    if not prs:
        return 0

    # Query sessions.db for sessions indexed by branch
    sess_conn = sqlite3.connect(str(sessions_db_path))
    try:
        sessions_by_branch: dict = {}
        try:
            rows = sess_conn.execute(
                "SELECT session_uuid, git_branch FROM sessions WHERE git_branch IS NOT NULL"
            ).fetchall()
        except sqlite3.OperationalError:
            # Table doesn't exist yet
            return 0

        for uuid, branch in rows:
            sessions_by_branch.setdefault(branch, []).append(uuid)
    finally:
        sess_conn.close()

    if not sessions_by_branch:
        return 0

    # Build set of session_uuids known to have touched this repo via gh events.
    # This replaces the old working_directory substring heuristic (issue #83):
    # observed gh CLI activity is a direct signal, path matching is a guess.
    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        repo_sessions = {
            r[0]
            for r in gh_conn.execute(
                "SELECT DISTINCT session_uuid FROM session_gh_events WHERE repo = ?",
                (repo,),
            ).fetchall()
        }
    finally:
        gh_conn.close()

    # Match and insert links
    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        for pr_number, head_ref in prs:
            for session_uuid in sessions_by_branch.get(head_ref, []):
                if session_uuid not in repo_sessions:
                    continue
                try:
                    gh_conn.execute(
                        """
                        INSERT OR IGNORE INTO pr_sessions
                            (repo, pr_number, session_uuid)
                        VALUES (?, ?, ?)
                        """,
                        (repo, pr_number, session_uuid),
                    )
                except sqlite3.IntegrityError:
                    pass  # already linked
        gh_conn.commit()
    finally:
        gh_conn.close()

    # Re-count actual rows to be precise
    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        count = gh_conn.execute(
            "SELECT COUNT(*) FROM pr_sessions WHERE repo = ?",
            (repo,),
        ).fetchone()[0]
    finally:
        gh_conn.close()

    pr_count = len(prs)
    session_count = sum(len(v) for v in sessions_by_branch.values())
    logger.info(
        "{}  session links: {} PRs queried, {} sessions in DB, {} gh-event-confirmed sessions, {} inserted",
        repo, pr_count, session_count, len(repo_sessions), count,
    )
    return count
