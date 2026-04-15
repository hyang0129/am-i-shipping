"""Derive push-after-review count for a pull request.

Queries PR commits and reviews via ``gh api``, counts commits pushed
after the first review event timestamp.  Returns 0 if there are no
reviews.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, gh_api


def count_pushes_after_review(
    repo: str,
    pr_number: int,
) -> int:
    """Count commits pushed after the first review on a PR.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    pr_number:
        The pull request number.

    Returns
    -------
    Number of commits with a date strictly after the first review's
    ``submitted_at`` timestamp.  Returns 0 if there are no reviews.
    """
    owner, name = repo.split("/", 1)

    # Fetch reviews
    reviews = _fetch_reviews(owner, name, pr_number)
    if not reviews:
        return 0

    # Find first review timestamp
    first_review_at = _earliest_review_time(reviews)
    if first_review_at is None:
        return 0

    # Fetch commits
    commits = _fetch_commits(owner, name, pr_number)
    if not commits:
        return 0

    # Count commits after first review
    count = 0
    for commit in commits:
        commit_date = _parse_commit_date(commit)
        if commit_date is not None and commit_date > first_review_at:
            count += 1

    return count


def _fetch_reviews(owner: str, name: str, pr_number: int) -> List[Dict[str, Any]]:
    """Fetch reviews for a PR."""
    endpoint = f"/repos/{owner}/{name}/pulls/{pr_number}/reviews"
    try:
        result = gh_api(endpoint, paginate=True)
        return result if isinstance(result, list) else []
    except GhCliError:
        return []


def _fetch_commits(owner: str, name: str, pr_number: int) -> List[Dict[str, Any]]:
    """Fetch commits for a PR."""
    endpoint = f"/repos/{owner}/{name}/pulls/{pr_number}/commits"
    try:
        result = gh_api(endpoint, paginate=True)
        return result if isinstance(result, list) else []
    except GhCliError:
        return []


def _earliest_review_time(reviews: List[Dict[str, Any]]) -> Optional[datetime]:
    """Find the earliest submitted_at among reviews."""
    times: List[datetime] = []
    for review in reviews:
        submitted = review.get("submitted_at")
        if submitted:
            try:
                dt = datetime.fromisoformat(submitted.replace("Z", "+00:00"))
                times.append(dt)
            except ValueError:
                pass
    return min(times) if times else None


def _parse_commit_date(commit: Dict[str, Any]) -> Optional[datetime]:
    """Parse the committer date from a commit object."""
    commit_data = commit.get("commit", {})
    committer = commit_data.get("committer", {})
    date_str = committer.get("date")
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None
