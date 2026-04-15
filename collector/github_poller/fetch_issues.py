"""Fetch issues from GitHub via ``gh`` CLI.

Wraps ``gh issue list`` + ``gh api`` for comments.  Returns normalized
dicts with: number, title, type_label, created_at, closed_at, state,
body, comments (list of {author, body, created_at}).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, run_gh_json, gh_api


def fetch_issues(
    repo: str,
    *,
    since: Optional[str] = None,
    state: str = "all",
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Fetch issues for *repo* (``owner/repo``).

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    since:
        ISO date string; only return issues updated after this date.
        Passed as ``--search "updated:>YYYY-MM-DD"`` filter.
    state:
        Issue state filter: ``open``, ``closed``, or ``all`` (default).
    limit:
        Maximum number of issues to fetch (default 500).

    Returns
    -------
    List of normalized issue dicts.
    """
    args = [
        "issue", "list",
        "--repo", repo,
        "--state", state,
        "--limit", str(limit),
        "--json", "number,title,labels,createdAt,closedAt,state,body",
    ]
    if since:
        args.extend(["--search", f"updated:>{since}"])

    raw_issues = run_gh_json(args)
    if not isinstance(raw_issues, list):
        raw_issues = [raw_issues]

    results: List[Dict[str, Any]] = []
    for issue in raw_issues:
        comments = _fetch_issue_comments(repo, issue["number"])
        type_label = _extract_type_label(issue.get("labels", []))

        results.append({
            "number": issue["number"],
            "title": issue.get("title", ""),
            "type_label": type_label,
            "created_at": issue.get("createdAt"),
            "closed_at": issue.get("closedAt"),
            "state": issue.get("state", "").upper(),
            "body": issue.get("body", ""),
            "comments": comments,
        })

    return results


def _fetch_issue_comments(
    repo: str,
    issue_number: int,
) -> List[Dict[str, str]]:
    """Fetch comments for a single issue via ``gh api``."""
    owner, name = repo.split("/", 1)
    endpoint = f"/repos/{owner}/{name}/issues/{issue_number}/comments"

    try:
        raw = gh_api(endpoint, paginate=True)
    except GhCliError:
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


def _extract_type_label(labels: List[Any]) -> Optional[str]:
    """Extract a type label (bug, feature, etc.) from the labels list.

    Returns the first label name that looks like a type classification,
    or None if none found.
    """
    type_prefixes = ("bug", "feature", "enhancement", "documentation",
                     "question", "task", "chore", "epic")
    for label in labels:
        name = ""
        if isinstance(label, dict):
            name = label.get("name", "").lower()
        elif isinstance(label, str):
            name = label.lower()
        if name in type_prefixes or name.startswith("type:"):
            return name
    return None
