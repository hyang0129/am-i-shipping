#!/usr/bin/env bash
# Install crontab entries to run am-i-shipping collectors daily at 02:00,
# with a boot-time fallback so missed runs are recovered on next login.
# Idempotent: re-running this script will not create duplicate entries.
#
# Usage:
#   bash scripts/install-cron.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Ensure logs directory exists (cron command redirects output there)
mkdir -p "$REPO_ROOT/logs"

CRON_TAG="am-i-shipping-collectors"
CRON_CMD="cd \"$REPO_ROOT\" && bash run_collectors.sh >> logs/cron.log 2>&1"
CRON_LINE="0 2 * * * $CRON_CMD # $CRON_TAG"
# Boot-time fallback: runs 60s after login in case the 02:00 window was missed
# (e.g. PC was off overnight). Collectors are idempotent so a same-day double-run
# produces no duplicate rows.
REBOOT_LINE="@reboot sleep 60 && $CRON_CMD # $CRON_TAG"

# Check if the entries already exist
if crontab -l 2>/dev/null | grep -qF "$CRON_TAG"; then
    echo "Crontab entries already exist — skipping (idempotent)."
    echo "To update, run uninstall-cron.sh first, then re-run this script."
    exit 0
fi

# Append both entries
(crontab -l 2>/dev/null || true; echo "$CRON_LINE"; echo "$REBOOT_LINE") | crontab -

echo "Crontab entries installed:"
echo "  - daily at 02:00"
echo "  - at boot (60s delay) to recover missed runs"
echo "Verify with: crontab -l | grep am-i-shipping"
