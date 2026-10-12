param()

. "$PSScriptRoot/sim_common.ps1"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "           Simulation Runner             " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Performing pre-checks..." -ForegroundColor Yellow
if (-not (Test-SimulatorReady)) {
    Write-Host "Simulator is not running. Starting the system (npm run dev) in a new window..." -ForegroundColor Yellow
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"npm run dev`"" -WorkingDirectory "$PSScriptRoot\.."

    Write-Host "Waiting for simulator backend to become ready..." -ForegroundColor Yellow
    $retries = 90
    while ($retries -gt 0 -and -not (Test-SimulatorReady)) {
        Start-Sleep -Seconds 2
        $retries--
    }
    if (-not (Test-SimulatorReady)) {
        Write-Host "The simulator did not become ready within 3 minutes." -ForegroundColor Red
        exit 1
    }
}
Write-Host "Simulator is ready." -ForegroundColor Green
Write-Host ""

Write-Host "Architectural Approaches:" -ForegroundColor Yellow
Write-Host "  1. Database Triggers (DBA Outbox)"
Write-Host "  2. Transactional Outbox (Recommended)"
Write-Host "  3. Stream-to-Stream Join (Flink)"
Write-Host "  4. JDBC SMT (Interceptor)"
Write-Host "  5. CDC Push + Consumer Enrichment"
Write-Host ""

$approach = $null
while ([string]::IsNullOrWhiteSpace($approach) -or $approach -notin 1,2,3,4,5) {
    $approach = Read-Host "Select Approach (1-5)"
}

$numSubscribers = 0
if ($approach -eq 5) {
    $numSubscribersStr = $null
    while ([string]::IsNullOrWhiteSpace($numSubscribersStr) -or -not [int]::TryParse($numSubscribersStr, [ref]$null)) {
        $numSubscribersStr = Read-Host "Enter Number of Subscribers (e.g. 2)"
    }
    $numSubscribers = [int]$numSubscribersStr
}

$rps = $null
while ([string]::IsNullOrWhiteSpace($rps) -or -not [int]::TryParse($rps, [ref]$null)) {
    $rps = Read-Host "Enter RPS (e.g. 100)"
}

$timeoutMs = $null
while ([string]::IsNullOrWhiteSpace($timeoutMs) -or -not [int]::TryParse($timeoutMs, [ref]$null)) {
    $timeoutMs = Read-Host "Enter SLA timeout in ms (e.g. 3000)"
}

$insertsOnlyStr = $null
while ([string]::IsNullOrWhiteSpace($insertsOnlyStr) -or $insertsOnlyStr -notin 'y','n','Y','N') {
    $insertsOnlyStr = Read-Host "Inserts Only? (y/n)"
}
$insertsOnly = ($insertsOnlyStr -eq 'y' -or $insertsOnlyStr -eq 'Y')

$duration = $null
while ([string]::IsNullOrWhiteSpace($duration) -or -not [int]::TryParse($duration, [ref]$null)) {
    $duration = Read-Host "Duration in seconds (e.g. 60)"
}

Write-Host "`nProvisioning Approach $approach (fresh data, only this approach's trigger/CDC/connector/jobs)..." -ForegroundColor Yellow
try {
    Start-SimulationRun @{
        approach = [int]$approach
        rps = [int]$rps
        timeoutMs = [int]$timeoutMs
        insertsOnly = $insertsOnly
        numSubscribers = [int]$numSubscribers
        resetData = $true
    }
} catch {
    Write-Host "`nFailed to start simulation: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

$outputDir = Join-Path $PSScriptRoot "../output"
if (-not (Test-Path -Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$csvFile = Join-Path $outputDir "sim_app${approach}_rps${rps}_$timestamp.csv"
Out-File -FilePath $csvFile -InputObject $script:CsvHeader -Encoding UTF8

Write-Host "`nSimulation running for $duration seconds. Recording stats per second..." -ForegroundColor Cyan
for ($i = 0; $i -lt $duration; $i++) {
    Start-Sleep -Seconds 1
    try {
        $stats = Invoke-RestMethod -Uri "$script:BaseUrl/stats" -Method Get -ErrorAction Stop
        Add-Content -Path $csvFile -Value (ConvertTo-StatsRow $stats)
        $timeLeft = $duration - $i - 1
        Write-Host -NoNewline "`r[Time Left: $($timeLeft)s] RPS: $($stats.actualRps)/$($stats.rps) | Lag: $($stats.lag) | DB p95: $($stats.p95)ms | DB CPU: $($stats.dbStats.cpu)%    "
    } catch {
        Write-Host -NoNewline "`rError fetching stats at second $i...                                        "
    }
}

Write-Host "`n`nStopping simulation (draining the pipeline)..." -ForegroundColor Yellow
try {
    $summary = Stop-SimulationRun
    Write-RunSummary $summary
    Write-Host "Stats saved to: $csvFile" -ForegroundColor Green
} catch {
    Write-Host "Simulation finished, but an error occurred while stopping: $($_.Exception.Message)" -ForegroundColor Red
}
