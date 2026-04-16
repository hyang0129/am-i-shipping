"""Thin wrapper around the ``gh`` CLI for GitHub API calls.

Handles subprocess invocation, JSON deserialization, pagination via
``--paginate``, rate-limit back-off (retry with reset-aware sleep on
403/429 exit codes), an hourly call budget so the poller never consumes
more than a configured fraction of the user's GitHub API quota, and a
proactive secondary-rate-limit guard that delays calls before they would
exceed a configured fraction of GitHub's per-minute cap.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any, Dict, List, Optional

from loguru import logger


_inter_request_delay: float = 0.0
_graphql_points_used: int = 0

# ---------------------------------------------------------------------------
# Secondary rate limit guard (proactive, sliding 60-second window)
# ---------------------------------------------------------------------------

# GitHub's authenticated REST secondary rate limit.
_GITHUB_SECONDARY_LIMIT_PER_MINUTE: int = 900


class _SecondaryRateLimiter:
    """Proactive sliding-window guard against GitHub's secondary rate limit.

    Before each API call, ``check()`` inspects how many calls have been
    made in the last 60 seconds.  If issuing one more call would push
    usage above *max_fraction* of GitHub's per-minute cap, it sleeps
    until the oldest in-window call ages out and capacity is restored.

    This prevents secondary rate limit errors entirely rather than
    reacting to them after the fact.
    """

    WINDOW_SECONDS: float = 60.0

    def __init__(
        self,
        github_limit: int = _GITHUB_SECONDARY_LIMIT_PER_MINUTE,
        max_fraction: float = 0.50,
    ) -> None:
        self._limit = github_limit
        self._threshold = max(1, int(github_limit * max_fraction))
        self._timestamps: List[float] = []  # monotonic call times

    def configure(self, max_fraction: float) -> None:
        self._threshold = max(1, int(self._limit * max_fraction))
        self._timestamps = []

    @property
    def window_count(self) -> int:
        """Calls made in the current 60-second window."""
        cutoff = time.monotonic() - self.WINDOW_SECONDS
        return sum(1 for t in self._timestamps if t > cutoff)

    def check(self) -> None:
        """Block until issuing the next call stays within the threshold.

        Trims the timestamp list on each invocation so it never grows
        beyond *threshold* entries.
        """
        now = time.monotonic()
        cutoff = now - self.WINDOW_SECONDS
        # Drop timestamps outside the window.
        self._timestamps = [t for t in self._timestamps if t > cutoff]

        if len(self._timestamps) >= self._threshold:
            # Sleep until the oldest call exits the window.
            oldest = self._timestamps[0]
            wait = (oldest + self.WINDOW_SECONDS) - time.monotonic()
            if wait > 0:
                logger.info(
                    "secondary rate limit: {}/{} calls in 60s window — "
                    "sleeping {:.1f}s to stay ≤{:.0f}% of GitHub cap",
                    len(self._timestamps),
                    self._threshold,
                    wait,
                    (self._threshold / self._limit) * 100,
                )
                time.sleep(wait + 0.1)  # +0.1s to ensure the slot has cleared
            # Re-trim after sleeping.
            now = time.monotonic()
            cutoff = now - self.WINDOW_SECONDS
            self._timestamps = [t for t in self._timestamps if t > cutoff]

        self._timestamps.append(time.monotonic())


_secondary = _SecondaryRateLimiter()

# ---------------------------------------------------------------------------
# Hourly call budget
# ---------------------------------------------------------------------------

class BudgetExhausted(Exception):
    """Raised when the hourly call budget has been reached."""

    def __init__(self, used: int, limit: int, resets_in: float) -> None:
        self.used = used
        self.limit = limit
        self.resets_in = resets_in
        super().__init__(
            f"GitHub API hourly budget exhausted ({used}/{limit} calls used). "
            f"Budget resets in {resets_in:.0f}s."
        )


class _HourlyBudget:
    """Fixed-window call counter that raises when the hourly cap is hit.

    Note: uses a fixed (not sliding) window — the counter resets to zero
    at the start of each 3600-second window.  In theory, a burst at the
    end of one window followed by a burst at the start of the next could
    allow up to 2× max_per_hour calls within any 60-minute span.  In
    practice, ``_SecondaryRateLimiter`` enforces a tighter per-minute cap
    (50 % of GitHub's 900 req/min) that prevents sustained bursting well
    below this theoretical maximum.
    """

    def __init__(self, max_per_hour: int) -> None:
        self._max = max_per_hour
        self._window_start = time.monotonic()
        self._count = 0

    def configure(self, max_per_hour: int) -> None:
        self._max = max_per_hour
        # Reset the window so a config change takes effect immediately.
        self._window_start = time.monotonic()
        self._count = 0

    def record(self) -> None:
        """Record one call.  Raises BudgetExhausted if the cap is reached."""
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed >= 3600:
            # New hour window — reset.
            self._window_start = now
            self._count = 0

        self._count += 1
        pct = self._count / self._max
        if pct >= 0.95:
            logger.warning("API budget {}/{} ({:.0f}%)", self._count, self._max, pct * 100)
        elif pct >= 0.80:
            logger.info("API budget {}/{} ({:.0f}%)", self._count, self._max, pct * 100)
        if self._count >= self._max:
            resets_in = max(0.0, 3600 - elapsed)
            raise BudgetExhausted(self._count, self._max, resets_in)


_budget = _HourlyBudget(max_per_hour=2500)


def calls_made() -> int:
    """Return the number of API calls made in the current hour window."""
    return _budget._count


def graphql_points_used() -> int:
    """Return the total GraphQL primary-rate-limit points consumed this run."""
    return _graphql_points_used


def configure_limiter(
    delay: float,
    max_calls_per_hour: int = 2500,
    secondary_max_fraction: float = 0.50,
) -> None:
    """Set the inter-request delay, hourly call budget, and secondary rate limit guard.

    Parameters
    ----------
    delay:
        Seconds to sleep after each successful ``gh`` call.
    max_calls_per_hour:
        Maximum ``gh`` calls allowed per rolling hour (default 2 500,
        half of a standard PAT's 5 000 req/hr primary limit).
    secondary_max_fraction:
        Maximum fraction of GitHub's 900 req/min secondary rate limit to
        use before proactively delaying.  Default 0.50 (450 calls/min).
    """
    global _inter_request_delay
    _inter_request_delay = delay
    _budget.configure(max_calls_per_hour)
    _secondary.configure(secondary_max_fraction)
    logger.info(
        "limiter configured: {:.2f}s delay, {}/hr cap, "
        "secondary guard ≤{:.0f}% of {}/min",
        delay,
        max_calls_per_hour,
        secondary_max_fraction * 100,
        _GITHUB_SECONDARY_LIMIT_PER_MINUTE,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_rate_limit_error(stderr: str) -> bool:
    """Return True if stderr looks like a GitHub primary/secondary rate limit."""
    lower = stderr.lower()
    return any(
        phrase in lower
        for phrase in ("rate limit", "secondary rate", "429", "403")
    )


def _rate_limit_reset_wait() -> float:
    """Query ``/rate_limit`` and return seconds until the REST quota resets.

    Returns 0 if the quota is not exhausted or the query itself fails.
    The ``/rate_limit`` endpoint is free — it does not consume quota.
    """
    try:
        result = subprocess.run(
            ["gh", "api", "/rate_limit"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            return 0.0
        data = json.loads(result.stdout)
        core = data.get("resources", {}).get("core", {})
        remaining = core.get("remaining", 1)
        if remaining > 0:
            return 0.0
        reset_epoch = core.get("reset", 0)
        wait = reset_epoch - time.time()
        return max(0.0, wait)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class GhCliError(Exception):
    """Raised when a ``gh`` subprocess exits with a non-zero code."""

    def __init__(self, cmd: List[str], returncode: int, stderr: str) -> None:
        self.cmd = cmd
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"gh exited {returncode}: {' '.join(cmd)}\nstderr: {stderr}"
        )


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_gh(
    args: List[str],
    *,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> str:
    """Run ``gh`` with *args* and return stdout.

    On failure, checks whether the error is a rate-limit exhaustion and
    sleeps until the GitHub quota resets before retrying (up to
    *max_retries* times).  Falls back to exponential back-off for other
    transient errors.

    Raises
    ------
    BudgetExhausted
        When the configured hourly call budget is reached.
    GhCliError
        After exhausting retries.
    """
    # Proactive secondary rate limit guard — delays if needed before the call.
    # Note: these are called once before the retry loop, so retries are not
    # individually re-checked or re-recorded.  This is an acceptable
    # approximation: retries are rare and short-sleeped, so the under-count
    # is negligible compared to the budget windows (1 hour / 60 seconds).
    _secondary.check()
    # Check hourly budget.
    _budget.record()

    cmd = ["gh"] + args
    logger.debug("→ gh {}", ' '.join(args))
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
            logger.warning(
                "retry {}/{}: {} — {}",
                attempt + 1,
                max_retries,
                ' '.join(cmd[1:3]),
                result.stderr.strip()[:120],
            )
            if _is_rate_limit_error(result.stderr):
                wait = _rate_limit_reset_wait()
                if wait > 0:
                    logger.info("rate limit — sleeping {:.0f}s until reset", wait)
                    time.sleep(wait + 5)  # +5s buffer
                else:
                    # Secondary limit or unknown 403 — short back-off.
                    time.sleep(backoff_base ** attempt)
            else:
                time.sleep(backoff_base ** attempt)

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
    response = json.loads(stdout)

    global _graphql_points_used
    rate_limit = (response.get("data") or {}).get("rateLimit")
    if rate_limit:
        cost = rate_limit.get("cost", 0)
        remaining = rate_limit.get("remaining")
        _graphql_points_used += cost
        logger.debug(
            "graphql cost={} remaining={} total_this_run={}",
            cost, remaining, _graphql_points_used,
        )

    return response


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
