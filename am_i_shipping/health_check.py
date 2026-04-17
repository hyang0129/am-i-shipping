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
from typing import List, Tuple, Union


EXPECTED_COLLECTORS = ["session_parser", "github_poller", "synthesis"]
# Per-collector staleness threshold. Session parser and GitHub poller run
# daily, so 48h catches a missed run. Synthesis runs weekly (Sundays only)
# so its threshold has to allow for a full cadence plus a one-day slack
# window for clock skew / DST / missed-run catch-up.
STALE_THRESHOLDS = {
    "session_parser": timedelta(hours=48),
    "github_poller": timedelta(hours=48),
    "synthesis": timedelta(days=8),
}
# Default kept for backwards compatibility — any collector not in the map
# above falls back to the original 48h threshold. Callers that import
# STALE_THRESHOLD directly continue to see the same value.
STALE_THRESHOLD = timedelta(hours=48)


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
            hours_ago = age.total_seconds() / 3600
            messages.append(
                f"WARNING: {collector} is stale — last success {hours_ago:.1f}h ago "
                f"(threshold: {threshold.total_seconds() / 3600:.0f}h)"
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
