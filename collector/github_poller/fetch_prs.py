"""Fetch pull requests from GitHub via ``gh`` CLI.

Wraps ``gh pr list`` + ``gh api`` for review comments.  Returns normalized
dicts with: number, created_at, merged_at, review_comment_count, head_ref,
body, review_comments (list of {author, body, created_at}).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, run_gh_json, gh_api, gh_graphql


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
        "--json", "number,createdAt,mergedAt,headRefName,body,title,updatedAt",
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
            "updated_at": pr.get("updatedAt"),
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
    except GhCliError:
        return []

    if not isinstance(raw, list):
        return []

    comments: List[Dict[str, Any]] = []
    for c in raw:
        comments.append({
            "id": c.get("id"),
            "author": (c.get("user") or {}).get("login", ""),
            "body": c.get("body", ""),
            "created_at": c.get("created_at", ""),
        })
    return comments


_PR_EDIT_HISTORY_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      userContentEdits(first: 100) {
        nodes {
          editedAt
          diff
          editor { login }
        }
      }
      reviews(first: 100) {
        nodes {
          comments(first: 100) {
            nodes {
              databaseId
              userContentEdits(first: 100) {
                nodes {
                  editedAt
                  diff
                  editor { login }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


def fetch_pr_edit_history(repo: str, pr_number: int) -> Dict[str, Any]:
    """Fetch edit history for a single PR via GraphQL.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    pr_number:
        Pull request number to query.

    Returns
    -------
    Dict with keys ``body_edits`` and ``review_comment_edits``, each a
    list of edit dicts.  Returns an empty dict on GraphQL error.
    """
    owner, name = repo.split("/", 1)
    try:
        response = gh_graphql(
            _PR_EDIT_HISTORY_QUERY,
            {"owner": owner, "name": name, "number": pr_number},
        )
    except Exception:
        return {}

    if response.get("errors"):
        return {}

    pr_data = (
        (response.get("data") or {})
        .get("repository", {})
        .get("pullRequest")
    )
    if not pr_data:
        return {"body_edits": [], "review_comment_edits": []}

    body_edits: List[Dict[str, Any]] = []
    for node in (pr_data.get("userContentEdits") or {}).get("nodes") or []:
        editor_login = None
        editor = node.get("editor")
        if editor:
            editor_login = editor.get("login")
        body_edits.append({
            "edited_at": node.get("editedAt"),
            "diff": node.get("diff"),
            "editor": editor_login,
        })

    review_comment_edits: List[Dict[str, Any]] = []
    for review_node in (pr_data.get("reviews") or {}).get("nodes") or []:
        for comment_node in (review_node.get("comments") or {}).get("nodes") or []:
            comment_id = comment_node.get("databaseId")
            for node in (comment_node.get("userContentEdits") or {}).get("nodes") or []:
                editor_login = None
                editor = node.get("editor")
                if editor:
                    editor_login = editor.get("login")
                review_comment_edits.append({
                    "comment_id": comment_id,
                    "edited_at": node.get("editedAt"),
                    "diff": node.get("diff"),
                    "editor": editor_login,
                })

    return {"body_edits": body_edits, "review_comment_edits": review_comment_edits}
