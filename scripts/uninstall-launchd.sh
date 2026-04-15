#!/usr/bin/env bash
# Remove the am-i-shipping macOS launchd agent.
# Idempotent: safe to run even if the agent is not installed.
#
# Usage:
#   bash scripts/uninstall-launchd.sh

set -euo pipefail

LABEL="com.am-i-shipping.collectors"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"

# Unload if currently loaded
if launchctl list 2>/dev/null | grep -qF "$LABEL"; then
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    echo "launchd agent unloaded: $LABEL"
else
    echo "launchd agent not loaded — skipping unload."
fi

# Remove the plist file
if [ -f "$PLIST_PATH" ]; then
    rm "$PLIST_PATH"
    echo "Plist removed: $PLIST_PATH"
else
    echo "Plist not found — nothing to remove."
fi

echo "Done. Verify with: launchctl list | grep am-i-shipping"
