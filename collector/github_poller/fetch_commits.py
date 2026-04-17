"""Fetch per-PR commits for the Phase-2 synthesis engine (Epic #17, E-1).

This module absorbs the ``/repos/{owner}/{repo}/pulls/{number}/commits`` calls
that ``push_counter.py`` already makes, widening their use so the commits end
up persisted in ``github.db``. It deliberately does NOT add a repo-wide
``/repos/{owner}/{repo}/commits`` enumeration — the epic scope requires PR-bound
commits only so we stay within the existing primary-rate-limit envelope.

The normalized dict returned per commit has this shape::

    {
        "sha":         str,                 # full commit SHA
        "author":      Optional[str],       # login, falls back to committer name
        "authored_at": Optional[str],       # ISO-8601 timestamp from the commit
        "message":     Optional[str],       # commit message (full, not first line)
        "pr_number":   int,                 # the PR this commit was fetched for
        "pushed_at":   Optional[str],       # GitHub's ``commit.committer.date``
                                            # (commits pushed to the PR after
                                            # the initial author's authored_at
                                            # typically have a later committer
                                            # date, which is our best proxy for
                                            # a push timestamp without paging
                                            # PushEvents).
    }

Consumers (``run.py`` and ``push_counter.py``) take the returned list as-is
and decide what to do with it — persistence happens in ``store.upsert_commit``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .gh_client import GhCliError, gh_api


def _normalize_commit(raw: Dict[str, Any], pr_number: int) -> Dict[str, Any]:
    """Convert the REST commit shape into the flat dict used by ``store``."""
    commit_data = raw.get("commit") or {}
    author_block = commit_data.get("author") or {}
    committer_block = commit_data.get("committer") or {}

    # Prefer the GitHub login (which is nested under the top-level ``author``
    # object on the REST response — separate from the commit-metadata author)
    # and fall back to the name stored inside the commit metadata. Either may
    # be absent on detached-author commits.
    login = (raw.get("author") or {}).get("login") if raw.get("author") else None
    author = login or author_block.get("name")

    return {
        "sha": raw.get("sha"),
        "author": author,
        "authored_at": author_block.get("date"),
        "message": commit_data.get("message"),
        "pr_number": pr_number,
        "pushed_at": committer_block.get("date"),
    }


def fetch_pr_commits(repo: str, pr_number: int) -> List[Dict[str, Any]]:
    """Fetch commits for a single pull request.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    pr_number:
        The pull request number.

    Returns
    -------
    A list of normalized commit dicts.

    Raises
    ------
    GhCliError
        On API error. Callers that want silent-empty behaviour should catch
        explicitly — historical "return [] on error" was removed because it
        caused ``count_pushes_after_review`` to collapse push_count to 0
        indistinguishable from a genuine empty-PR response (see F-2).
        ``BudgetExhausted`` also propagates.
    """
    owner, name = repo.split("/", 1)
    endpoint = f"/repos/{owner}/{name}/pulls/{pr_number}/commits"

    raw = gh_api(endpoint, paginate=True)

    if not isinstance(raw, list):
        return []

    return [_normalize_commit(c, pr_number) for c in raw if c.get("sha")]


def fetch_and_store_pr_commits(
    repo: str,
    pr_number: int,
    github_db,  # Path; kept untyped to avoid circular Path import cost
    conn=None,
    *,
    commits: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Fetch commits for *pr_number* and persist each via ``store.upsert_commit``.

    This is the single function ``_poll_repo`` calls when the
    ``github.fetch_commits`` flag is on. It returns the commit list so the
    caller can pass it to ``push_counter.count_pushes_after_review`` to avoid a
    second ``/commits`` round-trip (the ``commits`` kwarg there is the exact
    same shape this function returns, minus the REST wrapper).

    Parameters
    ----------
    repo, pr_number, github_db, conn:
        Passed through to ``store.upsert_commit``.
    commits:
        If the caller already has the normalized commit list (e.g. from a
        prior ``fetch_pr_commits`` call), it can pass it here and skip the
        network round-trip. Persistence still happens.

    Raises
    ------
    GhCliError, BudgetExhausted
        Propagated from ``fetch_pr_commits`` so the caller can distinguish a
        transient API failure (retry / fall back) from a genuine zero-commit
        PR. See F-2.
    """
    # Defer the import so this module can be imported without pulling the full
    # store layer (and its transitive imports) at module load.
    from .store import upsert_commit

    fetched = commits if commits is not None else fetch_pr_commits(repo, pr_number)

    for commit in fetched:
        if not commit.get("sha"):
            continue
        try:
            upsert_commit(repo, commit, github_db, conn=conn)
        except ValueError:
            # Missing required field — skip the row but keep processing the
            # rest of the list. The fetch-layer normaliser already filters
            # SHA-less rows, so this is a defence in depth only.
            continue

    return fetched
