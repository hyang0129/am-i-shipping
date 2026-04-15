"""Thin wrapper around the ``gh`` CLI for GitHub API calls.

Handles subprocess invocation, JSON deserialization, pagination via
``--paginate``, and rate-limit back-off (simple retry with exponential
delay on 403/429 exit codes).
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Dict, List, Optional


_inter_request_delay: float = 0.0


def configure_limiter(delay: float) -> None:
    """Set the inter-request delay applied after each successful ``gh`` call."""
    global _inter_request_delay
    _inter_request_delay = delay


class GhCliError(Exception):
    """Raised when a ``gh`` subprocess exits with a non-zero code."""

    def __init__(self, cmd: List[str], returncode: int, stderr: str) -> None:
        self.cmd = cmd
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"gh exited {returncode}: {' '.join(cmd)}\nstderr: {stderr}"
        )


def run_gh(
    args: List[str],
    *,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> str:
    """Run ``gh`` with *args* and return stdout.

    Retries up to *max_retries* times with exponential back-off when the
    process exits non-zero (covers transient rate-limit 403/429 errors).

    Raises
    ------
    GhCliError
        After exhausting retries.
    """
    cmd = ["gh"] + args
    last_err: Optional[GhCliError] = None

    for attempt in range(max_retries + 1):
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            if _inter_request_delay > 0:
                time.sleep(_inter_request_delay)
            return result.stdout

        last_err = GhCliError(cmd, result.returncode, result.stderr)

        if attempt < max_retries:
            time.sleep(backoff_base ** attempt)

    # Should never be None here, but satisfy type checker
    assert last_err is not None
    raise last_err


def run_gh_json(
    args: List[str],
    *,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> Any:
    """Run ``gh`` and parse the output as JSON.

    Returns the deserialized JSON (dict or list).
    """
    stdout = run_gh(args, max_retries=max_retries, backoff_base=backoff_base)
    return json.loads(stdout)


def gh_graphql(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    *,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> Dict[str, Any]:
    """Call the GitHub GraphQL API via ``gh api graphql``.

    Parameters
    ----------
    query:
        GraphQL query string.
    variables:
        Dict of variable name -> value.  String values are passed with
        ``-f`` (untyped); all other types are serialized with ``-F``
        (typed) so that integers, booleans, etc. are interpreted correctly
        by the ``gh`` CLI.
    max_retries:
        Number of retries on failure (same semantics as ``run_gh``).
    backoff_base:
        Base for exponential back-off between retries.

    Returns
    -------
    Parsed JSON dict (the full GraphQL response, including ``data`` and
    any ``errors`` keys).

    Raises
    ------
    GhCliError
        After exhausting retries.
    """
    args: List[str] = ["api", "graphql", "-f", f"query={query}"]

    for k, v in (variables or {}).items():
        if isinstance(v, str):
            args.extend(["-f", f"{k}={v}"])
        else:
            args.extend(["-F", f"{k}={v}"])

    stdout = run_gh(args, max_retries=max_retries, backoff_base=backoff_base)
    return json.loads(stdout)


def gh_api(
    endpoint: str,
    *,
    method: str = "GET",
    paginate: bool = False,
    jq: Optional[str] = None,
    max_retries: int = 3,
) -> Any:
    """Call the GitHub REST/GraphQL API via ``gh api``.

    Parameters
    ----------
    endpoint:
        API path, e.g. ``/repos/{owner}/{repo}/issues``.
    method:
        HTTP method (default GET).
    paginate:
        If True, passes ``--paginate`` so ``gh`` follows Link headers.
    jq:
        Optional jq expression to filter the response.
    max_retries:
        Number of retries on failure.
    """
    args = ["api", endpoint, "--method", method]
    if paginate:
        args.append("--paginate")
    if jq:
        args.extend(["--jq", jq])

    stdout = run_gh(args, max_retries=max_retries)

    # When using --paginate, gh may concatenate multiple JSON arrays.
    # Try parsing as a single JSON value first; if that fails, parse
    # each line as a separate JSON array and merge.
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        merged: List[Any] = []
        for line in stdout.strip().splitlines():
            line = line.strip()
            if line:
                parsed = json.loads(line)
                if isinstance(parsed, list):
                    merged.extend(parsed)
                else:
                    merged.append(parsed)
        return merged
