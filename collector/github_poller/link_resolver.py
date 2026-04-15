"""PR-to-issue linkage resolver.

Two strategies, applied in order:

1. **Branch name regex**: matches patterns like ``fix/123-slug``,
   ``feature/123-slug``, ``issue-123``, etc.
2. **Body scan**: matches ``closes #N``, ``fixes #N``, ``resolves #N``
   (case-insensitive) in the PR body.

Strategy 1 wins when both match.  Returns ``None`` when neither matches.
"""

from __future__ import annotations

import re
from typing import Optional


# Patterns for extracting issue numbers from branch names.
# Matches: fix/123-slug, feature/123, issue-123, 123-slug, etc.
_BRANCH_PATTERNS = [
    re.compile(r"^(?:fix|feature|bugfix|hotfix|issue|closes|resolve)[/-](\d+)"),
    re.compile(r"^(\d+)[/-]"),
]

# Patterns for extracting issue numbers from PR body text.
# Matches: closes #123, fixes #123, resolves #123 (case-insensitive).
_BODY_PATTERN = re.compile(
    r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s+#(\d+)",
    re.IGNORECASE,
)


def resolve_link(
    head_ref: str,
    body: str,
) -> Optional[int]:
    """Resolve the issue number linked to a PR.

    Parameters
    ----------
    head_ref:
        The PR's head branch name (e.g. ``fix/42-add-logging``).
    body:
        The PR body/description text.

    Returns
    -------
    The issue number as an int, or ``None`` if no link could be resolved.
    Strategy 1 (branch name) takes priority over strategy 2 (body scan).
    """
    # Strategy 1: branch name
    issue = _resolve_from_branch(head_ref)
    if issue is not None:
        return issue

    # Strategy 2: body scan
    return _resolve_from_body(body)


def _resolve_from_branch(head_ref: str) -> Optional[int]:
    """Extract issue number from a branch name."""
    if not head_ref:
        return None

    for pattern in _BRANCH_PATTERNS:
        m = pattern.search(head_ref)
        if m:
            return int(m.group(1))
    return None


def _resolve_from_body(body: str) -> Optional[int]:
    """Extract the first issue number from PR body keywords."""
    if not body:
        return None

    m = _BODY_PATTERN.search(body)
    if m:
        return int(m.group(1))
    return None
