<#
.SYNOPSIS
    Register bridge and nightly export as Windows Task Scheduler tasks.

.DESCRIPTION
    Creates two scheduled tasks:
    1. "AppSwitch-Bridge" — runs bridge.ps1 at user logon, indefinitely.
    2. "AppSwitch-Export" — runs export.py daily at 02:00.

    Both tasks run under the current user's context. Safe to re-run —
    existing tasks with the same name are unregistered first.

.PARAMETER ScriptDir
    Directory containing bridge.ps1 and export.py.
    Default: the directory this script lives in.

.PARAMETER PythonPath
    Path to the Python executable. Default: "python"

.EXAMPLE
    .\install_task.ps1
    .\install_task.ps1 -PythonPath "C:\Python311\python.exe"
#>

param(
    [string]$ScriptDir = (Split-Path -Parent $MyInvocation.MyCommand.Path),
    [string]$PythonPath = "python"
)

$ErrorActionPreference = "Stop"

$BridgeTaskName = "AppSwitch-Bridge"
$ExportTaskName = "AppSwitch-Export"

# --- Helper: remove existing task if present ---
function Remove-ExistingTask {
    param([string]$TaskName)
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Removed existing task: $TaskName"
    }
}

# --- Bridge task: at-logon, indefinite ---
Remove-ExistingTask -TaskName $BridgeTaskName

$bridgeScript = Join-Path $ScriptDir "bridge.ps1"
$bridgeAction = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$bridgeScript`""

$bridgeTrigger = New-ScheduledTaskTrigger -AtLogOn

$bridgeSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $BridgeTaskName `
    -Action $bridgeAction `
    -Trigger $bridgeTrigger `
    -Settings $bridgeSettings `
    -Description "App-switch bridge: polls foreground window and posts heartbeats to ActivityWatch" `
    | Out-Null

Write-Host "Registered task: $BridgeTaskName (at-logon, indefinite)"

# --- Export task: daily at 02:00 ---
Remove-ExistingTask -TaskName $ExportTaskName

$exportScript = Join-Path $ScriptDir "export.py"
$exportAction = New-ScheduledTaskAction `
    -Execute $PythonPath `
    -Argument "`"$exportScript`""

$exportTrigger = New-ScheduledTaskTrigger -Daily -At "02:00"

$exportSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)

Register-ScheduledTask `
    -TaskName $ExportTaskName `
    -Action $exportAction `
    -Trigger $exportTrigger `
    -Settings $exportSettings `
    -Description "App-switch export: queries ActivityWatch and upserts into appswitch.db" `
    | Out-Null

Write-Host "Registered task: $ExportTaskName (daily at 02:00)"
Write-Host "Done. Both tasks installed."
