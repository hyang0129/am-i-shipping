<#
.SYNOPSIS
    End-to-end smoke test for the app-switch collector pipeline.

.DESCRIPTION
    Validates the full pipeline:
    1. Confirms ActivityWatch is running
    2. Switches windows programmatically
    3. Waits 35 seconds for bridge to capture events
    4. Asserts events exist in AW API
    5. Triggers export.py
    6. Asserts rows in appswitch.db
    7. Asserts health.json is current

    Exits 0 on success, non-zero with a clear failure message on any error.

.PARAMETER AwEndpoint
    ActivityWatch REST API base URL. Default: http://localhost:5600

.PARAMETER BucketId
    Bucket to check. Default: aw-watcher-window-appswitch

.PARAMETER ExportScript
    Path to export.py. Default: sibling directory's export.py

.EXAMPLE
    .\smoke_test.ps1
#>

param(
    [string]$AwEndpoint = "http://localhost:5600",
    [string]$BucketId = "aw-watcher-window-appswitch",
    [string]$ExportScript = (Join-Path (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)) "export.py")
)

$ErrorActionPreference = "Stop"
$failures = @()

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        $script:failures += $Message
        Write-Error "FAIL: $Message"
    } else {
        Write-Host "PASS: $Message"
    }
}

# 1. Check ActivityWatch is running
Write-Host "`n=== Step 1: Check ActivityWatch ==="
try {
    $info = Invoke-RestMethod -Uri "$AwEndpoint/api/0/info" -Method Get -ErrorAction Stop
    Assert-True ($null -ne $info) "ActivityWatch is running"
} catch {
    Write-Error "FATAL: ActivityWatch is not running at $AwEndpoint. Cannot continue."
    exit 1
}

# 2. Check bucket exists
Write-Host "`n=== Step 2: Check bucket ==="
try {
    $bucket = Invoke-RestMethod -Uri "$AwEndpoint/api/0/buckets/$BucketId" -Method Get -ErrorAction Stop
    Assert-True ($null -ne $bucket) "Bucket '$BucketId' exists"
} catch {
    Write-Error "FATAL: Bucket '$BucketId' not found. Run setup.ps1 first."
    exit 1
}

# 3. Switch windows to generate events
Write-Host "`n=== Step 3: Generate window events ==="
Write-Host "Opening Notepad and switching focus..."
$notepad = Start-Process notepad -PassThru
Start-Sleep -Seconds 2

# Switch back to PowerShell
[void][System.Runtime.InteropServices.Marshal]::GetActiveObject("Shell.Application")
$shell = New-Object -ComObject WScript.Shell
$shell.AppActivate($PID) | Out-Null
Start-Sleep -Seconds 2

# Switch to Notepad again
$shell.AppActivate($notepad.Id) | Out-Null
Start-Sleep -Seconds 2

Write-Host "Waiting 35 seconds for bridge to capture events..."
Start-Sleep -Seconds 35

# Clean up Notepad
Stop-Process -Id $notepad.Id -Force -ErrorAction SilentlyContinue

# 4. Check events in AW API
Write-Host "`n=== Step 4: Check AW events ==="
$now = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$oneHourAgo = (Get-Date).AddHours(-1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$eventsUrl = "$AwEndpoint/api/0/buckets/$BucketId/events?start=$oneHourAgo&end=$now"

try {
    $events = Invoke-RestMethod -Uri $eventsUrl -Method Get -ErrorAction Stop
    $eventCount = @($events).Count
    Assert-True ($eventCount -gt 0) "Found $eventCount events in AW bucket"
} catch {
    Assert-True $false "Failed to query events: $_"
}

# 5. Run export
Write-Host "`n=== Step 5: Run export ==="
try {
    $exportOutput = & python $ExportScript 2>&1
    $exportOutput | ForEach-Object { Write-Host "  $_" }
    Assert-True ($LASTEXITCODE -eq 0) "export.py exited successfully"
} catch {
    Assert-True $false "export.py failed: $_"
}

# 6. Check appswitch.db
Write-Host "`n=== Step 6: Check appswitch.db ==="
$dataDir = Join-Path (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $ExportScript))) "data"
$dbPath = Join-Path $dataDir "appswitch.db"

if (Test-Path $dbPath) {
    $rowCount = & python -c "import sqlite3; c=sqlite3.connect('$($dbPath -replace '\\','/')'); print(c.execute('SELECT COUNT(*) FROM app_events').fetchone()[0])"
    $rowCountInt = [int]$rowCount
    Assert-True ($rowCountInt -gt 0) "appswitch.db contains $rowCountInt rows"
} else {
    Assert-True $false "appswitch.db not found at $dbPath"
}

# 7. Check health.json
Write-Host "`n=== Step 7: Check health.json ==="
$healthPath = Join-Path $dataDir "health.json"

if (Test-Path $healthPath) {
    $health = Get-Content $healthPath | ConvertFrom-Json
    $lastSuccess = $health.appswitch_export.last_success
    Assert-True ($null -ne $lastSuccess) "health.json has appswitch_export.last_success = $lastSuccess"

    # Check that last_success is within the last 5 minutes
    $lastDt = [DateTime]::Parse($lastSuccess).ToUniversalTime()
    $ageSec = ((Get-Date).ToUniversalTime() - $lastDt).TotalSeconds
    Assert-True ($ageSec -lt 300) "health.json last_success is recent (${ageSec}s ago)"
} else {
    Assert-True $false "health.json not found at $healthPath"
}

# Summary
Write-Host "`n=== Summary ==="
if ($failures.Count -eq 0) {
    Write-Host "All checks passed."
    exit 0
} else {
    Write-Host "FAILURES ($($failures.Count)):"
    $failures | ForEach-Object { Write-Host "  - $_" }
    exit 1
}
