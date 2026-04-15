<#
.SYNOPSIS
    One-time setup: create an ActivityWatch bucket for app-switch tracking.

.DESCRIPTION
    Checks whether the target bucket exists on the local ActivityWatch
    instance. If not, creates it via the REST API. Safe to run multiple
    times — a 304 or "already exists" response is handled gracefully.

.PARAMETER AwEndpoint
    ActivityWatch REST API base URL. Default: http://localhost:5600

.PARAMETER BucketId
    Bucket identifier. Default: aw-watcher-window-appswitch

.EXAMPLE
    .\setup.ps1
    .\setup.ps1 -AwEndpoint http://localhost:5600 -BucketId my-bucket
#>

param(
    [string]$AwEndpoint = "http://localhost:5600",
    [string]$BucketId = "aw-watcher-window-appswitch"
)

$ErrorActionPreference = "Stop"

$bucketUrl = "$AwEndpoint/api/0/buckets/$BucketId"

# Check if bucket already exists
try {
    $response = Invoke-RestMethod -Uri $bucketUrl -Method Get -ErrorAction Stop
    Write-Host "Bucket '$BucketId' already exists."
    exit 0
} catch {
    $statusCode = $_.Exception.Response.StatusCode.value__
    if ($statusCode -ne 404) {
        Write-Error "Unexpected error checking bucket: $_"
        exit 1
    }
    # 404 = bucket does not exist, proceed to create
}

# Create bucket
$body = @{
    client  = "appswitch-bridge"
    type    = "currentwindow"
    hostname = $env:COMPUTERNAME
} | ConvertTo-Json -Depth 2

try {
    Invoke-RestMethod -Uri $bucketUrl -Method Post -Body $body `
        -ContentType "application/json" -ErrorAction Stop
    Write-Host "Bucket '$BucketId' created successfully."
} catch {
    $statusCode = $_.Exception.Response.StatusCode.value__
    if ($statusCode -eq 304) {
        Write-Host "Bucket '$BucketId' already exists (304)."
    } else {
        Write-Error "Failed to create bucket: $_"
        exit 1
    }
}
