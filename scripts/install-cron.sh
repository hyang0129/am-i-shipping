#!/usr/bin/env bash
# Install a crontab entry to run am-i-shipping collectors daily at 02:00.
# Idempotent: re-running this script will not create duplicate entries.
#
# Usage:
#   bash scripts/install-cron.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CRON_TAG="am-i-shipping-collectors"
CRON_CMD="cd \"$REPO_ROOT\" && bash run_collectors.sh >> logs/cron.log 2>&1"
CRON_LINE="0 2 * * * $CRON_CMD # $CRON_TAG"

# Check if the entry already exists
if crontab -l 2>/dev/null | grep -qF "$CRON_TAG"; then
    echo "Crontab entry already exists — skipping (idempotent)."
    echo "To update, run uninstall-cron.sh first, then re-run this script."
    exit 0
fi

# Append the new entry
(crontab -l 2>/dev/null || true; echo "$CRON_LINE") | crontab -

echo "Crontab entry installed: daily at 02:00"
echo "Verify with: crontab -l | grep am-i-shipping"
