<#
.SYNOPSIS
    Foreground window polling bridge — posts heartbeats to ActivityWatch.

.DESCRIPTION
    Polls the foreground window title and process name at a configurable
    interval (default 30 seconds). Each sample is sent as a heartbeat to
    the ActivityWatch REST API. POST failures are logged to stderr but
    never crash the loop.

    Designed to run indefinitely as a background/scheduled task.
    Stop with Ctrl-C or by ending the process.

.PARAMETER AwEndpoint
    ActivityWatch REST API base URL. Default: http://localhost:5600

.PARAMETER BucketId
    Target bucket. Default: aw-watcher-window-appswitch

.PARAMETER PollIntervalSeconds
    Seconds between samples. Default: 30

.EXAMPLE
    .\bridge.ps1
    .\bridge.ps1 -PollIntervalSeconds 10
#>

param(
    [string]$AwEndpoint = "http://localhost:5600",
    [string]$BucketId = "aw-watcher-window-appswitch",
    [int]$PollIntervalSeconds = 30
)

$ErrorActionPreference = "Continue"

# Load the required Win32 API for GetForegroundWindow
Add-Type @"
using System;
using System.Runtime.InteropServices;
using System.Text;

public class ForegroundWindow {
    [DllImport("user32.dll")]
    public static extern IntPtr GetForegroundWindow();

    [DllImport("user32.dll", SetLastError = true)]
    public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int count);

    [DllImport("user32.dll", SetLastError = true)]
    public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
}
"@

$heartbeatUrl = "$AwEndpoint/api/0/buckets/$BucketId/heartbeat?pulsetime=$($PollIntervalSeconds + 1)"

Write-Host "App-switch bridge started. Polling every ${PollIntervalSeconds}s -> $BucketId"
Write-Host "Press Ctrl-C to stop."

while ($true) {
    try {
        # Get foreground window handle
        $hwnd = [ForegroundWindow]::GetForegroundWindow()

        # Get window title
        $sb = New-Object System.Text.StringBuilder 512
        [void][ForegroundWindow]::GetWindowText($hwnd, $sb, $sb.Capacity)
        $windowTitle = $sb.ToString()

        # Get process name
        $processId = 0
        [void][ForegroundWindow]::GetWindowThreadProcessId($hwnd, [ref]$processId)
        $processName = ""
        if ($processId -gt 0) {
            try {
                $proc = Get-Process -Id $processId -ErrorAction Stop
                $processName = $proc.ProcessName
            } catch {
                $processName = "unknown"
            }
        }

        # Build heartbeat payload
        $timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        $payload = @{
            timestamp = $timestamp
            duration  = 0
            data      = @{
                app   = $processName
                title = $windowTitle
            }
        } | ConvertTo-Json -Depth 3

        # POST heartbeat to ActivityWatch
        try {
            Invoke-RestMethod -Uri $heartbeatUrl -Method Post -Body $payload `
                -ContentType "application/json" -ErrorAction Stop | Out-Null
        } catch {
            Write-Error "POST failed: $_ (app=$processName, title=$windowTitle)"
        }
    } catch {
        Write-Error "Poll cycle error: $_"
    }

    Start-Sleep -Seconds $PollIntervalSeconds
}
