<#
.SYNOPSIS
    Remove the AppSwitch-Bridge and AppSwitch-Export scheduled tasks.

.DESCRIPTION
    Cleanly unregisters both tasks created by install_task.ps1.
    Safe to run when tasks do not exist — missing tasks are silently skipped.

.EXAMPLE
    .\uninstall_task.ps1
#>

$ErrorActionPreference = "Continue"

$TaskNames = @("AppSwitch-Bridge", "AppSwitch-Export")

foreach ($TaskName in $TaskNames) {
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        # Stop the task if running
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Removed task: $TaskName"
    } else {
        Write-Host "Task not found (already removed): $TaskName"
    }
}

Write-Host "Done. All app-switch tasks uninstalled."
