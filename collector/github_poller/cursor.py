"""Poll cursor management for incremental GitHub fetching.

On first run (no cursor), uses a 90-day backfill window.
On subsequent runs, uses ``updated:>last_polled_at`` delta.
The cursor is advanced only after a successful batch.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union


def read_cursor(
    repo: str,
    db_path: Union[str, Path],
) -> Optional[str]:
    """Read the last_polled_at timestamp for *repo*.

    Returns an ISO date string (``YYYY-MM-DD``) or ``None`` if no
    cursor exists (first run).
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return None

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT last_polled_at FROM poll_cursor WHERE repo = ?",
            (repo,),
        ).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def compute_since(
    cursor_value: Optional[str],
    backfill_days: int = 90,
) -> str:
    """Compute the ``since`` date for fetching.

    If *cursor_value* is set, returns it directly (delta mode).
    Otherwise, returns a date *backfill_days* ago (backfill mode).

    Returns
    -------
    ISO date string ``YYYY-MM-DD``.
    """
    if cursor_value:
        return cursor_value
    backfill_date = datetime.now(timezone.utc) - timedelta(days=backfill_days)
    return backfill_date.strftime("%Y-%m-%d")


def advance_cursor(
    repo: str,
    db_path: Union[str, Path],
) -> None:
    """Set the cursor to today's date after a successful poll.

    Uses ``INSERT OR REPLACE`` so it works for both first and
    subsequent runs.
    """
    db_path = Path(db_path)
    from am_i_shipping.db import init_github_db

    db_path.parent.mkdir(parents=True, exist_ok=True)
    init_github_db(db_path)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO poll_cursor (repo, last_polled_at)
            VALUES (?, ?)
            ON CONFLICT(repo) DO UPDATE SET
                last_polled_at = excluded.last_polled_at
            """,
            (repo, today),
        )
        conn.commit()
    finally:
        conn.close()
