<#
.SYNOPSIS
    Remove the am-i-shipping Windows Task Scheduler task.

.DESCRIPTION
    Idempotent: safe to run even if the task does not exist.

.EXAMPLE
    .\scripts\uninstall-task.ps1
#>

$ErrorActionPreference = "Stop"

$TaskName = "am-i-shipping-collectors"

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Task Scheduler task removed: $TaskName"
} else {
    Write-Host "Task '$TaskName' not found — nothing to remove."
}

Write-Host "Verify with: schtasks /query /tn $TaskName"
