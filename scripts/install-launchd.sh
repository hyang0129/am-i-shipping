#!/usr/bin/env bash
# Install a macOS launchd agent to run am-i-shipping collectors daily at 02:00,
# with a login-time trigger so missed runs (e.g. Mac was off overnight) are
# recovered on next login. Collectors are idempotent so same-day double-runs are safe.
# Idempotent: re-running this script will update the plist and reload the agent.
#
# Usage:
#   bash scripts/install-launchd.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

LABEL="com.am-i-shipping.collectors"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
AGENTS_DIR="$HOME/Library/LaunchAgents"

# Ensure LaunchAgents directory exists
mkdir -p "$AGENTS_DIR"

# Ensure logs directory exists
mkdir -p "$REPO_ROOT/logs"

# Unload existing agent if present (ignore errors if not loaded)
if launchctl list 2>/dev/null | grep -qF "$LABEL"; then
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

# Write the plist
cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$REPO_ROOT/run_collectors.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$REPO_ROOT</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>$REPO_ROOT/logs/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string>$REPO_ROOT/logs/launchd.err.log</string>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
PLIST

# Load the agent
launchctl load "$PLIST_PATH"

echo "launchd agent installed: $LABEL"
echo "Schedule: daily at 02:00"
echo "Plist: $PLIST_PATH"
echo "Verify with: launchctl list | grep am-i-shipping"
