#!/usr/bin/env bash
# Remove the am-i-shipping crontab entry.
# Idempotent: safe to run even if no entry exists.
#
# Usage:
#   bash scripts/uninstall-cron.sh

set -euo pipefail

CRON_TAG="am-i-shipping-collectors"

if ! crontab -l 2>/dev/null | grep -qF "$CRON_TAG"; then
    echo "No am-i-shipping crontab entry found — nothing to remove."
    exit 0
fi

crontab -l 2>/dev/null | grep -vF "$CRON_TAG" | crontab -

echo "Crontab entry removed."
echo "Verify with: crontab -l"
