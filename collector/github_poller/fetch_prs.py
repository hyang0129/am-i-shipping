"""Fetch pull requests from GitHub via ``gh`` CLI.

Wraps ``gh pr list`` + ``gh api`` for review comments.  Returns normalized
dicts with: number, created_at, merged_at, review_comment_count, head_ref,
body, review_comments (list of {author, body, created_at}).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, run_gh_json, gh_api


def fetch_prs(
    repo: str,
    *,
    since: Optional[str] = None,
    state: str = "all",
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Fetch pull requests for *repo* (``owner/repo``).

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    since:
        ISO date string; only return PRs updated after this date.
    state:
        PR state filter: ``open``, ``closed``, ``merged``, or ``all``.
    limit:
        Maximum number of PRs to fetch (default 500).

    Returns
    -------
    List of normalized PR dicts.
    """
    args = [
        "pr", "list",
        "--repo", repo,
        "--state", state,
        "--limit", str(limit),
        "--json", "number,createdAt,mergedAt,headRefName,body,title",
    ]
    if since:
        args.extend(["--search", f"updated:>{since}"])

    raw_prs = run_gh_json(args)
    if not isinstance(raw_prs, list):
        raw_prs = [raw_prs]

    results: List[Dict[str, Any]] = []
    for pr in raw_prs:
        review_comments = _fetch_review_comments(repo, pr["number"])

        results.append({
            "number": pr["number"],
            "title": pr.get("title", ""),
            "created_at": pr.get("createdAt"),
            "merged_at": pr.get("mergedAt"),
            "head_ref": pr.get("headRefName", ""),
            "body": pr.get("body", ""),
            "review_comment_count": len(review_comments),
            "review_comments": review_comments,
        })

    return results


def _fetch_review_comments(
    repo: str,
    pr_number: int,
) -> List[Dict[str, str]]:
    """Fetch review comments for a single PR via ``gh api``."""
    owner, name = repo.split("/", 1)
    endpoint = f"/repos/{owner}/{name}/pulls/{pr_number}/comments"

    try:
        raw = gh_api(endpoint, paginate=True)
    except (GhCliError, Exception):
        return []

    if not isinstance(raw, list):
        return []

    comments: List[Dict[str, str]] = []
    for c in raw:
        comments.append({
            "author": (c.get("user") or {}).get("login", ""),
            "body": c.get("body", ""),
            "created_at": c.get("created_at", ""),
        })
    return comments
