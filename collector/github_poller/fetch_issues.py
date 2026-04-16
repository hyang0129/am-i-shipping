"""Fetch issues from GitHub via ``gh`` CLI.

Wraps ``gh issue list`` + ``gh api`` for comments.  Returns normalized
dicts with: number, title, type_label, created_at, closed_at, state,
body, comments (list of {author, body, created_at}).
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, run_gh_json, gh_api, gh_graphql


def fetch_issues(
    repo: str,
    *,
    since: Optional[str] = None,
    state: str = "all",
    limit: int = 500,
    include_comments: bool = False,
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
    include_comments:
        If True, fetch comments for every issue in the returned list.
        Leave False (default) when the caller will apply a cap first and
        then fetch comments only for the kept subset via
        :func:`fetch_issue_comments`.

    Returns
    -------
    List of normalized issue dicts.  When *include_comments* is False the
    ``"comments"`` key is present but set to an empty list.
    """
    args = [
        "issue", "list",
        "--repo", repo,
        "--state", state,
        "--limit", str(limit),
        "--json", "number,title,labels,createdAt,closedAt,state,body,updatedAt",
    ]
    if since:
        args.extend(["--search", f"updated:>{since}"])

    raw_issues = run_gh_json(args)
    if not isinstance(raw_issues, list):
        raw_issues = [raw_issues]

    results: List[Dict[str, Any]] = []
    for issue in raw_issues:
        type_label = _extract_type_label(issue.get("labels", []))
        comments = fetch_issue_comments(repo, issue["number"]) if include_comments else []

        results.append({
            "number": issue["number"],
            "title": issue.get("title", ""),
            "type_label": type_label,
            "created_at": issue.get("createdAt"),
            "closed_at": issue.get("closedAt"),
            "updated_at": issue.get("updatedAt"),
            "state": issue.get("state", "").upper(),
            "body": issue.get("body", ""),
            "comments": comments,
        })

    return results


def fetch_issue_comments(
    repo: str,
    issue_number: int,
) -> List[Dict[str, Any]]:
    """Fetch comments for a single issue via ``gh api``.

    Exported so callers can fetch comments after applying an item cap,
    avoiding wasted API calls for items that will be discarded.
    """
    owner, name = repo.split("/", 1)
    endpoint = f"/repos/{owner}/{name}/issues/{issue_number}/comments"

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


_ISSUE_EDIT_HISTORY_QUERY = """
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    issue(number: $number) {
      userContentEdits(first: 100) {
        nodes {
          editedAt
          diff
          editor { login }
        }
      }
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
"""


def _parse_issue_edit_history(issue_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Parse a single issue node from GraphQL into body_edits + comment_edits."""
    if not issue_data:
        return {"body_edits": [], "comment_edits": []}

    body_edits: List[Dict[str, Any]] = []
    for node in (issue_data.get("userContentEdits") or {}).get("nodes") or []:
        editor_login = None
        editor = node.get("editor")
        if editor:
            editor_login = editor.get("login")
        body_edits.append({
            "edited_at": node.get("editedAt"),
            "diff": node.get("diff"),
            "editor": editor_login,
        })

    comment_edits: List[Dict[str, Any]] = []
    for comment_node in (issue_data.get("comments") or {}).get("nodes") or []:
        comment_id = comment_node.get("databaseId")
        for node in (comment_node.get("userContentEdits") or {}).get("nodes") or []:
            editor_login = None
            editor = node.get("editor")
            if editor:
                editor_login = editor.get("login")
            comment_edits.append({
                "comment_id": comment_id,
                "edited_at": node.get("editedAt"),
                "diff": node.get("diff"),
                "editor": editor_login,
            })

    return {"body_edits": body_edits, "comment_edits": comment_edits}


def fetch_issue_edit_history(repo: str, issue_number: int) -> Dict[str, Any]:
    """Fetch edit history for a single issue via GraphQL.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    issue_number:
        Issue number to query.

    Returns
    -------
    Dict with keys ``body_edits`` and ``comment_edits``, each a list of
    edit dicts.  Returns an empty dict on GraphQL error.
    """
    owner, name = repo.split("/", 1)
    try:
        response = gh_graphql(
            _ISSUE_EDIT_HISTORY_QUERY,
            {"owner": owner, "name": name, "number": issue_number},
        )
    except Exception as exc:
        print(f"  warning: edit history fetch failed for issue {issue_number}: {exc}", file=sys.stderr)
        return {}

    if response.get("errors"):
        return {}

    issue_data = (
        (response.get("data") or {})
        .get("repository", {})
        .get("issue")
    )
    return _parse_issue_edit_history(issue_data)


def fetch_issue_edit_history_batch(
    repo: str,
    issue_numbers: List[int],
) -> Dict[int, Dict[str, Any]]:
    """Fetch edit history for up to 20 issues per GraphQL call using aliases.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    issue_numbers:
        List of issue numbers to query (max 20 per call; larger lists are
        processed in chunks of 20).

    Returns
    -------
    Mapping of issue_number -> edit history dict (same shape as
    :func:`fetch_issue_edit_history`).  Issues that fail are omitted.
    """
    owner, name = repo.split("/", 1)
    results: Dict[int, Dict[str, Any]] = {}

    CHUNK_SIZE = 20
    for chunk_start in range(0, len(issue_numbers), CHUNK_SIZE):
        chunk = issue_numbers[chunk_start : chunk_start + CHUNK_SIZE]

        # Build aliased query for this chunk
        alias_blocks = []
        for i, num in enumerate(chunk):
            alias_blocks.append(f"""
    issue{i}: issue(number: {num}) {{
      number
      userContentEdits(first: 100) {{
        nodes {{ editedAt diff editor {{ login }} }}
      }}
      comments(first: 100) {{
        nodes {{
          databaseId
          userContentEdits(first: 100) {{
            nodes {{ editedAt diff editor {{ login }} }}
          }}
        }}
      }}
    }}""")

        query = (
            "query($owner: String!, $name: String!) {\n"
            "  repository(owner: $owner, name: $name) {"
            + "".join(alias_blocks)
            + "\n  }\n}"
        )

        try:
            response = gh_graphql(query, {"owner": owner, "name": name})
        except Exception:
            continue

        if response.get("errors") and not response.get("data"):
            continue

        repo_data = (response.get("data") or {}).get("repository") or {}
        for i, num in enumerate(chunk):
            issue_data = repo_data.get(f"issue{i}")
            results[num] = _parse_issue_edit_history(issue_data)

    return results


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
