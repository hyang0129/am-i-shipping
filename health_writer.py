"""Atomically update data/health.json for a single collector.

Usage:
    from health_writer import write_health
    write_health("session_parser", 42)
    write_health("session_parser", 42, data_dir="data")
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Union


def write_health(
    collector_name: str,
    record_count: int,
    data_dir: Union[str, Path, None] = None,
) -> None:
    """Create or update the collector's entry in health.json.

    Merges into the existing file rather than overwriting, so other
    collectors' entries are preserved. The write is atomic: a temp file
    is written first, then renamed over the target.

    Parameters
    ----------
    collector_name:
        One of ``session_parser``, ``github_poller``, ``appswitch_export``.
    record_count:
        Number of records processed in this run.
    data_dir:
        Directory containing health.json. Defaults to ``data/`` relative
        to the repo root (directory containing this file).
    """
    if data_dir is None:
        data_dir = Path(__file__).resolve().parent / "data"
    else:
        data_dir = Path(data_dir)

    data_dir.mkdir(parents=True, exist_ok=True)
    health_path = data_dir / "health.json"

    # Read existing data (if any)
    # TODO: This read-modify-write is not process-safe. If two collectors
    #   call write_health concurrently (e.g. hook-triggered session_parser
    #   overlapping with a nightly batch run), one update can be lost.
    #   Add file locking (fcntl/msvcrt) when concurrent execution becomes
    #   a real scenario. For now collectors run sequentially and a lost
    #   health update is re-written on the next run.
    existing: dict = {}
    if health_path.exists():
        try:
            with open(health_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            # Corrupted file — start fresh but preserve what we can
            existing = {}

    # Update the single collector entry
    existing[collector_name] = {
        "last_success": datetime.now(timezone.utc).isoformat(),
        "last_record_count": record_count,
    }

    # Atomic write: write to temp file in the same directory, then rename
    fd, tmp_path = tempfile.mkstemp(dir=str(data_dir), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
            f.write("\n")
        # os.replace is atomic on POSIX; on Windows it is atomic if
        # the destination is on the same volume (which it is here).
        os.replace(tmp_path, str(health_path))
    except BaseException:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
