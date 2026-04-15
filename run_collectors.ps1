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
    @{
        Name = "App-Switch Export"
        Command = @("python", (Join-Path $ScriptDir "collector" "appswitch" "export.py")) + $ConfigArg
    }
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
