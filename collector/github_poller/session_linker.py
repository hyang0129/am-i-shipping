"""Link PRs to Claude Code sessions via matching head_ref to git_branch.

After PR upsert, queries ``sessions.db`` for sessions whose
``git_branch`` matches the PR's ``head_ref`` AND whose
``working_directory`` contains the repo name.  Inserts matched pairs
into ``pr_sessions`` (idempotent via ``INSERT OR IGNORE``).

When ``sessions.db`` does not exist or is empty, exits cleanly with
zero rows inserted — it does not block the poller.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Union


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
    Number of new links inserted (0 if sessions.db is absent or empty,
    or if no matches are found).
    """
    sessions_db_path = Path(sessions_db_path)
    github_db_path = Path(github_db_path)

    # Exit cleanly if sessions.db does not exist
    if not sessions_db_path.exists():
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

    # Extract the repo name (last component) for working_directory matching
    repo_name = repo.split("/")[-1]

    # Query sessions.db for matching sessions
    sess_conn = sqlite3.connect(str(sessions_db_path))
    try:
        sessions_by_branch = {}
        try:
            rows = sess_conn.execute(
                "SELECT session_uuid, git_branch, working_directory FROM sessions"
            ).fetchall()
        except sqlite3.OperationalError:
            # Table doesn't exist yet
            return 0

        for uuid, branch, workdir in rows:
            if branch:
                sessions_by_branch.setdefault(branch, []).append(
                    (uuid, workdir or "")
                )
    finally:
        sess_conn.close()

    if not sessions_by_branch:
        return 0

    # Match and insert links
    gh_conn = sqlite3.connect(str(github_db_path))
    try:
        for pr_number, head_ref in prs:
            matching_sessions = sessions_by_branch.get(head_ref, [])
            for session_uuid, workdir in matching_sessions:
                # Optional: check working_directory contains repo name
                if repo_name and repo_name not in workdir:
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

    return count
