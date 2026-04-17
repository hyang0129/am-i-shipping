"""Check collector health by reading data/health.json.

Standalone usage:
    python -m am_i_shipping.health_check [--data-dir path/to/data]
    Exit code 0 = all healthy, 1 = stale or missing.

Importable:
    from am_i_shipping.health_check import check_health
    results = check_health()  # no side effects
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import List, Tuple, Union


EXPECTED_COLLECTORS = ["session_parser", "github_poller", "synthesis"]
# Per-collector staleness threshold. Session parser and GitHub poller run
# daily, so 48h catches a missed run. Synthesis runs weekly (Sundays only)
# so its threshold has to allow for a full cadence plus a one-day slack
# window for clock skew / DST / missed-run catch-up.
#
# If you change STALE_THRESHOLDS["synthesis"] (currently 8 days), also
# update setup.md §"Weekly synthesis cadence" which documents the same
# value to human readers. F-8 in the PR-48 review-fix cycle.
#
# Wrapped in MappingProxyType so importers cannot mutate the map in
# place. ``.get(key, default)`` still works on the proxy. F-6 in the
# same review-fix cycle.
STALE_THRESHOLDS = MappingProxyType({
    "session_parser": timedelta(hours=48),
    "github_poller": timedelta(hours=48),
    "synthesis": timedelta(days=8),
})
# Default kept for backwards compatibility — any collector not in the map
# above falls back to the original 48h threshold. Callers that import
# STALE_THRESHOLD directly continue to see the same value.
STALE_THRESHOLD = timedelta(hours=48)


def _format_duration(delta: timedelta) -> str:
    """Render a timedelta as a human-readable "Xh" or "Xd" string.

    Durations strictly below 72h are shown in hours ("48h"); anything
    above that is shown in days with one decimal of precision ("8d",
    "10.0d"). F-7 in the PR-48 review-fix cycle.
    """
    total_hours = delta.total_seconds() / 3600
    if total_hours < 72:
        return f"{total_hours:.0f}h"
    total_days = total_hours / 24
    # Use an integer render when the value is clean (no fractional day),
    # otherwise one decimal. Avoids "8.0d" in favour of "8d" for the
    # expected 8-day synthesis threshold.
    if abs(total_days - round(total_days)) < 0.05:
        return f"{int(round(total_days))}d"
    return f"{total_days:.1f}d"


def check_health(
    data_dir: Union[str, Path, None] = None,
) -> Tuple[bool, List[str]]:
    """Check whether all collectors have reported recently.

    Returns ``(healthy, messages)`` where *healthy* is True only if
    every expected collector has a ``last_success`` within the staleness
    threshold. *messages* contains human-readable warnings for any
    problems found.

    This function has no side effects — it only reads health.json.
    """
    if data_dir is None:
        data_dir = Path(__file__).resolve().parent.parent / "data"
    else:
        data_dir = Path(data_dir)

    health_path = data_dir / "health.json"
    messages: List[str] = []

    if not health_path.exists():
        return False, ["health.json not found — no collector has reported yet"]

    try:
        with open(health_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        return False, [f"Failed to read health.json: {exc}"]

    if not isinstance(data, dict):
        return False, ["health.json is not a JSON object"]

    now = datetime.now(timezone.utc)
    all_healthy = True

    for collector in EXPECTED_COLLECTORS:
        entry = data.get(collector)
        if entry is None:
            messages.append(f"WARNING: {collector} has never reported")
            all_healthy = False
            continue

        last_success_str = entry.get("last_success")
        if not last_success_str:
            messages.append(f"WARNING: {collector} has no last_success timestamp")
            all_healthy = False
            continue

        try:
            last_success = datetime.fromisoformat(last_success_str)
            # Ensure timezone-aware comparison
            if last_success.tzinfo is None:
                last_success = last_success.replace(tzinfo=timezone.utc)
        except ValueError:
            messages.append(
                f"WARNING: {collector} has invalid timestamp: {last_success_str}"
            )
            all_healthy = False
            continue

        age = now - last_success
        threshold = STALE_THRESHOLDS.get(collector, STALE_THRESHOLD)
        if age > threshold:
            messages.append(
                f"WARNING: {collector} is stale — last success "
                f"{_format_duration(age)} ago "
                f"(threshold: {_format_duration(threshold)})"
            )
            all_healthy = False
        else:
            count = entry.get("last_record_count", "?")
            messages.append(f"OK: {collector} — {count} records")

    return all_healthy, messages


def main() -> None:
    parser = argparse.ArgumentParser(description="Check collector health")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Path to data directory containing health.json",
    )
    args = parser.parse_args()

    healthy, messages = check_health(data_dir=args.data_dir)

    for msg in messages:
        print(msg, file=sys.stderr if msg.startswith("WARNING") else sys.stdout)

    sys.exit(0 if healthy else 1)


if __name__ == "__main__":
    main()
