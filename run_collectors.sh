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

# Weekly synthesis: only run on Sundays, or when AMIS_FORCE_SYNTHESIS=1 is set.
# `date +%u` returns 1..7 where 7 = Sunday (POSIX). Both GNU date and BSD date
# support `+%u` the same way, so no per-OS branching is needed here.
#
# Week-start semantics: synthesis covers the most recently *completed* week,
# so WEEK_START is always the Sunday seven days before today when today IS
# Sunday, and the most recent past Sunday on every other day. GNU
# `date -d 'last sunday'` and BSD `date -v-sun` both implement that
# "previous Sunday, never today" semantic natively, so the output is
# consistent across OSes and across weekdays. See F-1 / F-9 in the PR-48
# review-fix cycle: the PowerShell counterpart is aligned to the same
# convention explicitly.
#
# Exit semantics: a non-zero exit from `am-synthesize` is logged as a
# WARNING and does NOT increment FAIL_COUNT. The overall scheduler's
# exit code should reflect the daily collectors' health; synthesis is a
# once-a-week add-on that may legitimately skip (missing API key,
# AMIS_SYNTHESIS_LIVE=1 without credentials, empty DB, etc.). Only
# daily-collector failures should flip the scheduler red. See F-5 in the
# PR-48 review-fix cycle.
if [ "$(date +%u)" = "7" ] || [ "${AMIS_FORCE_SYNTHESIS:-0}" = "1" ]; then
    WEEK_START="$(date -d 'last sunday' +%Y-%m-%d 2>/dev/null || date -v-sun +%Y-%m-%d)"
    log "--- Starting: Weekly Synthesis (week=$WEEK_START) ---"
    if am-synthesize --week "$WEEK_START" "${CONFIG_ARG[@]}" >> "$LOG_FILE" 2>&1; then
        log "OK: Weekly Synthesis completed successfully"
    else
        rc=$?
        log "WARNING: Weekly Synthesis exited with code $rc (not counted as a failure)"
    fi
    log "--- Finished: Weekly Synthesis ---"
fi

log "=== Running health check ==="
if python3 -m am_i_shipping.health_check >> "$LOG_FILE" 2>&1; then
    log "OK: All collectors healthy"
else
    log "WARNING: Health check reports stale or missing collectors"
fi

log "=== Collector run finished ($FAIL_COUNT failures) ==="
exit $((FAIL_COUNT > 0 ? 1 : 0))
