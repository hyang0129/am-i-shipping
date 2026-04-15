#!/usr/bin/env bash
# Run all collectors in sequence, log output, and check health.
# macOS/Linux equivalent of run_collectors.ps1.
#
# Usage:
#   bash run_collectors.sh
#   bash run_collectors.sh --config /path/to/config.yaml

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$SCRIPT_DIR"

# Parse optional --config argument
CONFIG_ARG=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            CONFIG_ARG=("--config" "$2")
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

# Setup log directory
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP="$(date +%Y-%m-%d_%H%M%S)"
LOG_FILE="$LOG_DIR/run_$TIMESTAMP.log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG_FILE"
}

run_collector() {
    local name="$1"
    shift
    log "--- Starting: $name ---"
    if python3 "$@" "${CONFIG_ARG[@]}" >> "$LOG_FILE" 2>&1; then
        log "OK: $name completed successfully"
    else
        log "ERROR: $name exited with code $?"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    log "--- Finished: $name ---"
}

log "=== Collector run started ==="
FAIL_COUNT=0

run_collector "Session Parser" -m collector.session_parser --mode batch
run_collector "GitHub Poller"  -m collector.github_poller.run
run_collector "App-Switch Export" collector/appswitch/export.py

log "=== Running health check ==="
if python3 -m am_i_shipping.health_check >> "$LOG_FILE" 2>&1; then
    log "OK: All collectors healthy"
else
    log "WARNING: Health check reports stale or missing collectors"
fi

log "=== Collector run finished ($FAIL_COUNT failures) ==="
exit $((FAIL_COUNT > 0 ? 1 : 0))
