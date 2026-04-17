"""Per-unit metric calculators (Epic #17 — Sub-Issue 4 / Issue #37).

Each function is pure — it takes already-fetched rows and returns a
number — so the unit identifier (``synthesis/unit_identifier.py``) can
wire them up after a single scan over the github/sessions tables.

Metric semantics follow the epic ADR:

* ``elapsed_days``     — wall-clock span from the first event observed in
  the unit to the last event, across *all* nodes (sessions, issues, PRs).
  Units with a single timestamp return ``0.0``.
* ``dark_time_pct``    — sessions-only gap ratio,
  ``1 - sum(session_ended_at - session_started_at) / (max(ended) - min(started))``.
  Single-session units return ``0.0`` (ADR Decision 3).
* ``total_reprompts``  — sum of ``sessions.reprompt_count`` across the
  unit's sessions. Missing / NULL values count as ``0``.
* ``review_cycles``    — per-PR review activity across the unit. Uses
  ``len(review_comments_json)`` when present, falling back to
  ``push_count`` when the review comments payload is empty. Missing PR
  rows count as 0.

Design
------
Functions accept *either* a sqlite3 connection (so the caller can pass a
github or sessions DB handle) *or* already-materialised row iterables.
Keeping both surfaces lets the unit identifier build one SQL query per
DB while ``test_metrics.py`` can exercise the logic with plain tuples.

No network, no LLM. All functions are offline by design.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 string into a naive UTC ``datetime``.

    The collectors store timestamps in ``YYYY-MM-DDTHH:MM:SSZ`` form. We
    accept any ``fromisoformat``-compatible variant and drop the ``Z``
    suffix, because ``datetime.fromisoformat`` on Python 3.10 cannot
    parse it. Returns ``None`` for falsy / unparseable input so callers
    can skip missing values uniformly.
    """
    if not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# elapsed_days
# ---------------------------------------------------------------------------


def elapsed_days(timestamps: Iterable[Optional[str]]) -> float:
    """Return span from earliest to latest timestamp in *timestamps*, in days.

    Callers collect every timestamp associated with the unit's nodes —
    session start/end, issue created/closed/updated, PR
    created/merged/updated, commit authored/pushed — and pass them in a
    single iterable. ``None`` / empty strings are ignored. If fewer than
    two valid timestamps remain, the result is ``0.0``.
    """
    parsed = [t for t in (_parse_ts(v) for v in timestamps) if t is not None]
    if len(parsed) < 2:
        return 0.0
    delta = max(parsed) - min(parsed)
    return delta.total_seconds() / 86400.0


# ---------------------------------------------------------------------------
# dark_time_pct
# ---------------------------------------------------------------------------


def dark_time_pct(
    session_intervals: Iterable[tuple[Optional[str], Optional[str]]],
) -> float:
    """Return sessions-only gap ratio across *session_intervals*.

    Each entry is a ``(session_started_at, session_ended_at)`` pair. The
    metric answers: "during the span from the first session's start to
    the last session's end, what fraction of time had no active
    session?". Formally::

        1 - sum(ended - started) / (max(ended) - min(started))

    Sessions missing a start or end are dropped (can't be part of an
    interval). Single-session units return ``0.0`` even if the
    denominator would collapse to zero — this is Decision 3 in the epic
    ADR: "dark time has no meaning for a single session".
    """
    parsed: list[tuple[datetime, datetime]] = []
    for started, ended in session_intervals:
        s = _parse_ts(started)
        e = _parse_ts(ended)
        if s is None or e is None:
            continue
        if e < s:
            # Defensive: reject malformed rows rather than producing
            # negative durations that would break the ratio.
            continue
        parsed.append((s, e))
    if len(parsed) < 2:
        return 0.0
    active = sum((e - s).total_seconds() for s, e in parsed)
    span = (max(e for _, e in parsed) - min(s for s, _ in parsed)).total_seconds()
    if span <= 0:
        return 0.0
    ratio = 1.0 - (active / span)
    # Clamp to [0, 1] — overlapping sessions could in theory produce a
    # negative ratio; abandonment with no gap span doesn't happen here
    # because ``span <= 0`` is handled above.
    if ratio < 0.0:
        return 0.0
    if ratio > 1.0:
        return 1.0
    return ratio


# ---------------------------------------------------------------------------
# total_reprompts
# ---------------------------------------------------------------------------


def total_reprompts(
    session_ids: Iterable[str],
    sessions_conn: sqlite3.Connection,
) -> int:
    """Return sum of ``sessions.reprompt_count`` for the given *session_ids*.

    Missing rows and NULL counts are treated as ``0`` — the metric is a
    *running total* and should not fail just because a session row was
    pruned or never got a reprompt tally.
    """
    ids = [sid for sid in session_ids if sid]
    if not ids:
        return 0
    placeholders = ",".join("?" * len(ids))
    cur = sessions_conn.execute(
        f"SELECT COALESCE(SUM(reprompt_count), 0) FROM sessions "
        f"WHERE session_uuid IN ({placeholders})",
        ids,
    )
    row = cur.fetchone()
    return int(row[0] or 0)


# ---------------------------------------------------------------------------
# review_cycles
# ---------------------------------------------------------------------------


def _count_review_comments(payload: Optional[str]) -> int:
    """Return len of the JSON array in *payload*, 0 on any failure."""
    if not payload:
        return 0
    try:
        parsed = json.loads(payload)
    except (ValueError, TypeError):
        return 0
    if isinstance(parsed, list):
        return len(parsed)
    return 0


def review_cycles(
    pr_refs: Iterable[tuple[str, int]],
    github_conn: sqlite3.Connection,
) -> int:
    """Return total review activity across the unit's PRs.

    *pr_refs* is an iterable of ``(repo, pr_number)`` tuples. For each
    PR, we prefer the length of ``review_comments_json`` (the raw
    payload count); if that's empty we fall back to ``push_count`` as a
    coarse proxy for "how many times the author iterated". Missing rows
    contribute ``0``.

    The two signals are additive within a PR's row but mutually
    exclusive in practice — ``review_comments_json`` is populated only
    for PRs with review activity, and ``push_count`` captures the
    "author-iteration-without-review" case. The fallback avoids double-
    counting when reviews happened (the richer signal wins).
    """
    pairs = list(pr_refs)
    if not pairs:
        return 0
    total = 0
    for repo, pr_number in pairs:
        cur = github_conn.execute(
            "SELECT review_comments_json, push_count FROM pull_requests "
            "WHERE repo = ? AND pr_number = ?",
            (repo, pr_number),
        )
        row = cur.fetchone()
        if row is None:
            continue
        review_comments_json, push_count = row
        n = _count_review_comments(review_comments_json)
        if n == 0:
            n = int(push_count or 0)
        total += n
    return total
