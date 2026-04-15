<#
.SYNOPSIS
    Register a Windows Task Scheduler task to run am-i-shipping collectors daily at 02:00.

.DESCRIPTION
    Idempotent: if the task already exists it is removed and re-created.

.EXAMPLE
    .\scripts\install-task.ps1
#>

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir "..")).Path

$TaskName = "am-i-shipping-collectors"
$RunScript = Join-Path $RepoRoot "run_collectors.ps1"

# Remove existing task if present (idempotent)
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Existing task removed — will re-create."
}

# Create the task
$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -File `"$RunScript`"" `
    -WorkingDirectory $RepoRoot

$Trigger = New-ScheduledTaskTrigger -Daily -At "02:00"

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Run am-i-shipping collectors daily at 02:00" | Out-Null

Write-Host "Task Scheduler task installed: $TaskName"
Write-Host "Schedule: daily at 02:00"
Write-Host "Verify with: schtasks /query /tn $TaskName"
