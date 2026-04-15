"""Nightly export: query ActivityWatch REST API, deduplicate, upsert into appswitch.db.

Queries the prior day's events from ActivityWatch, deduplicates on
``(timestamp_bucket, window_hash)``, and upserts into the ``app_events``
table in ``appswitch.db``.  Writes ``health.json`` on success.

Deduplication key:
    timestamp_bucket = unix_ts // 30 * 30
    window_hash      = sha256(app + title)[:8]

Usage:
    python collector/appswitch/export.py [--config path/to/config.yaml]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Union
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _window_hash(app: str, title: str) -> str:
    """Return first 8 hex chars of SHA-256 of ``app + title``."""
    h = hashlib.sha256((app + title).encode("utf-8")).hexdigest()
    return h[:8]


def _timestamp_bucket(unix_ts: float, interval: int = 30) -> int:
    """Snap a Unix timestamp to a bucket boundary."""
    return int(unix_ts) // interval * interval


def fetch_events(
    aw_endpoint: str,
    bucket_id: str = "aw-watcher-window-appswitch",
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> list[dict[str, Any]]:
    """Fetch events from ActivityWatch REST API for a time range.

    Parameters
    ----------
    aw_endpoint:
        Base URL of the AW server (e.g. ``http://localhost:5600``).
    bucket_id:
        Bucket to query.
    start:
        Start of time range (inclusive). Defaults to midnight yesterday (UTC).
    end:
        End of time range (exclusive). Defaults to midnight today (UTC).

    Returns
    -------
    List of raw event dicts from the AW API.
    """
    now_utc = datetime.now(timezone.utc)
    if start is None:
        start = (now_utc - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    if end is None:
        end = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = (
        f"{aw_endpoint}/api/0/buckets/{bucket_id}/events"
        f"?start={start_str}&end={end_str}&limit=-1"
    )

    req = Request(url, method="GET")
    req.add_header("Accept", "application/json")

    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, OSError) as exc:
        print(f"ERROR: Failed to fetch events from {url}: {exc}", file=sys.stderr)
        raise

    if not isinstance(data, list):
        print(
            f"WARNING: AW API returned non-list response ({type(data).__name__}), "
            f"treating as empty",
            file=sys.stderr,
        )
        return []
    return data


def deduplicate(events: list[dict[str, Any]], interval: int = 30) -> list[dict[str, Any]]:
    """Deduplicate events on ``(timestamp_bucket, window_hash)``.

    For duplicate keys, the first occurrence (by list order) wins.
    Each returned dict has keys: ``timestamp_bucket``, ``window_hash``,
    ``app_name``, ``window_title``, ``duration_seconds``.

    Parameters
    ----------
    events:
        Raw AW event dicts with ``timestamp``, ``duration``, and
        ``data.app`` / ``data.title`` fields.
    interval:
        Bucket size in seconds.

    Returns
    -------
    Deduplicated list of event dicts ready for DB insertion.
    """
    seen: set[tuple[int, str]] = set()
    result: list[dict[str, Any]] = []

    for event in events:
        # Parse timestamp
        ts_str = event.get("timestamp", "")
        try:
            # AW uses ISO 8601 with variable precision
            ts_str_clean = ts_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts_str_clean)
            unix_ts = dt.timestamp()
        except (ValueError, TypeError):
            continue  # skip malformed timestamps

        data = event.get("data", {})
        app = data.get("app", "")
        title = data.get("title", "")
        duration = event.get("duration", 0.0)

        bucket = _timestamp_bucket(unix_ts, interval)
        w_hash = _window_hash(app, title)
        key = (bucket, w_hash)

        if key not in seen:
            seen.add(key)
            result.append(
                {
                    "timestamp_bucket": bucket,
                    "window_hash": w_hash,
                    "app_name": app,
                    "window_title": title,
                    "duration_seconds": float(duration),
                }
            )

    return result


def upsert_events(
    events: list[dict[str, Any]],
    db_path: Union[str, Path],
) -> int:
    """Insert deduplicated events into ``appswitch.db``.

    Uses ``INSERT OR IGNORE`` for idempotency — duplicate keys are
    silently skipped.

    Parameters
    ----------
    events:
        Deduplicated event dicts from ``deduplicate()``.
    db_path:
        Path to ``appswitch.db``.

    Returns
    -------
    Number of rows actually inserted (excluding duplicates).
    """
    from am_i_shipping.db import init_appswitch_db

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    init_appswitch_db(db_path)

    conn = sqlite3.connect(str(db_path), isolation_level="DEFERRED")
    inserted = 0
    try:
        with conn:
            for ev in events:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO app_events
                        (timestamp_bucket, window_hash, app_name, window_title, duration_seconds)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        ev["timestamp_bucket"],
                        ev["window_hash"],
                        ev["app_name"],
                        ev["window_title"],
                        ev["duration_seconds"],
                    ),
                )
                inserted += cursor.rowcount
    finally:
        conn.close()

    return inserted


def run(
    config_path: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
) -> tuple[int, bool]:
    """Run the full export pipeline.

    Parameters
    ----------
    config_path:
        Path to config.yaml. If None, uses the default location.
    start:
        Override start of time range.
    end:
        Override end of time range.

    Returns
    -------
    A tuple of ``(record_count, success)`` where *record_count* is the
    number of rows inserted and *success* is True if the pipeline
    completed without error.
    """
    from am_i_shipping.config_loader import load_config
    from am_i_shipping.health_writer import write_health

    config = load_config(config_path)
    data_dir = config.data_path
    data_dir.mkdir(parents=True, exist_ok=True)

    aw_endpoint = config.appswitch.aw_endpoint
    db_path = data_dir / "appswitch.db"

    try:
        events = fetch_events(aw_endpoint, start=start, end=end)
        print(f"Fetched {len(events)} events from ActivityWatch", file=sys.stderr)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 0, False

    deduped = deduplicate(events)
    print(
        f"After dedup: {len(deduped)} unique events (from {len(events)} raw)",
        file=sys.stderr,
    )

    inserted = upsert_events(deduped, db_path)
    print(f"Inserted {inserted} new rows into appswitch.db", file=sys.stderr)

    write_health("appswitch_export", len(deduped), data_dir=data_dir)

    return len(deduped), True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export ActivityWatch events to appswitch.db"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml",
    )
    args = parser.parse_args()

    _count, ok = run(config_path=args.config)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
