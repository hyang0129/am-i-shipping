<#
.SYNOPSIS
    Run all three collectors in sequence, log output, and check health.

.DESCRIPTION
    Entry point for the workflow monitor data collection pipeline.
    Runs each collector independently — a failure in one does not abort
    the others. Output is logged to a dated file under logs/.

.EXAMPLE
    .\run_collectors.ps1
    .\run_collectors.ps1 -ConfigPath .\config.yaml
#>

param(
    [string]$ConfigPath = ""
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# --- Setup log directory and file ---
$LogDir = Join-Path $ScriptDir "logs"
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
}
$Timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$LogFile = Join-Path $LogDir "run_$Timestamp.log"

function Write-Log {
    param([string]$Message)
    $entry = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | $Message"
    $entry | Tee-Object -FilePath $LogFile -Append
}

Write-Log "=== Collector run started ==="

# --- Build config argument ---
$ConfigArg = @()
if ($ConfigPath -ne "") {
    $ConfigArg = @("--config", $ConfigPath)
}

# --- Collector definitions ---
# Each collector is a hashtable with Name and Command.
# Commands are Python modules/scripts; adjust paths as collectors are implemented.
$Collectors = @(
    @{
        Name = "Session Parser"
        Command = @("python", "-m", "collector.session_parser", "--mode", "batch") + $ConfigArg
    },
    @{
        Name = "GitHub Poller"
        Command = @("python", "-m", "collector.github_poller.run") + $ConfigArg
    },
)

$FailCount = 0

foreach ($Collector in $Collectors) {
    Write-Log "--- Starting: $($Collector.Name) ---"
    try {
        $output = & $Collector.Command[0] $Collector.Command[1..($Collector.Command.Length - 1)] 2>&1
        $output | ForEach-Object { Write-Log "  $_" }
        if ($LASTEXITCODE -ne 0) {
            Write-Log "ERROR: $($Collector.Name) exited with code $LASTEXITCODE"
            $FailCount++
        } else {
            Write-Log "OK: $($Collector.Name) completed successfully"
        }
    } catch {
        Write-Log "ERROR: $($Collector.Name) threw exception: $_"
        $FailCount++
    }
    Write-Log "--- Finished: $($Collector.Name) ---"
}

# --- Weekly synthesis ---
# Only run on Sundays or when AMIS_FORCE_SYNTHESIS=1 is set.
#
# Week-start semantics: synthesis covers the most recently *completed* week.
# WEEK_START is ALWAYS the Sunday that began that completed week — i.e. the
# most recent past Sunday, never today. On Sunday, DayOfWeek=0 so the naive
# `-DayOfWeek` arithmetic would return today; we explicitly subtract a full
# 7 days in that case to match the shell-side `date -d 'last sunday'`
# semantic. This keeps the SH and PS1 schedulers in lock-step (see F-1 in
# the PR-48 review-fix cycle).
#
# .NET DayOfWeek: Sunday=0, Monday=1, ..., Saturday=6.
#   today - DayOfWeek    = this-week Sunday (== today when today is Sunday)
#   today - DayOfWeek - 7 (when today is Sunday) = last-week Sunday
#
# Exit semantics: a non-zero exit from `am-synthesize` is logged as a
# WARNING and does NOT increment $FailCount. The overall scheduler's exit
# code should reflect the daily collectors' health; synthesis is a
# once-a-week add-on that may legitimately skip (missing API key,
# AMIS_SYNTHESIS_LIVE=1 without credentials, empty DB, etc.). See F-5.
$Today = (Get-Date).DayOfWeek
$ForceSynthesis = $env:AMIS_FORCE_SYNTHESIS -eq "1"
if ($Today -eq [DayOfWeek]::Sunday -or $ForceSynthesis) {
    $DayOfWeekInt = [int](Get-Date).DayOfWeek
    if ($DayOfWeekInt -eq 0) {
        # Sunday: go back a full 7 days to land on the previous Sunday.
        $WeekStart = (Get-Date).AddDays(-7).ToString("yyyy-MM-dd")
    } else {
        # Any other day: go back to this week's Sunday.
        $WeekStart = (Get-Date).AddDays(-$DayOfWeekInt).ToString("yyyy-MM-dd")
    }
    Write-Log "--- Starting: Weekly Synthesis (week=$WeekStart) ---"
    try {
        $synthArgs = @("--week", $WeekStart) + $ConfigArg
        $synthOutput = & am-synthesize @synthArgs 2>&1
        $synthOutput | ForEach-Object { Write-Log "  $_" }
        if ($LASTEXITCODE -ne 0) {
            Write-Log "WARNING: Weekly Synthesis exited with code $LASTEXITCODE (not counted as a failure)"
        } else {
            Write-Log "OK: Weekly Synthesis completed successfully"
        }
    } catch {
        Write-Log "WARNING: Weekly Synthesis threw exception: $_ (not counted as a failure)"
    }
    Write-Log "--- Finished: Weekly Synthesis ---"
}

# --- Health check ---
Write-Log "=== Running health check ==="
try {
    $healthOutput = & python -m am_i_shipping.health_check 2>&1
    $healthOutput | ForEach-Object { Write-Log "  $_" }
    if ($LASTEXITCODE -ne 0) {
        Write-Log "WARNING: Health check reports stale or missing collectors"
    } else {
        Write-Log "OK: All collectors healthy"
    }
} catch {
    Write-Log "ERROR: Health check failed: $_"
}

Write-Log "=== Collector run finished ($FailCount failures) ==="

if ($FailCount -gt 0) {
    exit 1
} else {
    exit 0
}
