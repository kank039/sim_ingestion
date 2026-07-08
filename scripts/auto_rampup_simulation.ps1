param()

$ErrorActionPreference = 'SilentlyContinue'

$BaseUrl = "http://localhost:3001/api"
$HealthUrl = "http://localhost:3001/health"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "       Automated Simulation Rampup       " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

# Check if simulator is running
$simulatorRunning = $false
try {
    $health = Invoke-RestMethod -Uri $HealthUrl -Method Get -ErrorAction Stop
    if ($health.status -eq 'ok') {
        $simulatorRunning = $true
        Write-Host "Simulator is running." -ForegroundColor Green
    }
} catch {
    Write-Host "Simulator is not running. Please start it with 'npm run dev' first." -ForegroundColor Red
    exit 1
}

$outputDir = ".\output"
if (-not (Test-Path -Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

$approaches = 2..2

foreach ($app in $approaches) {
    Write-Host "`n--- Starting Approach $app ---" -ForegroundColor Yellow

    Write-Host "Cleaning state..."
    try {
        Invoke-RestMethod -Uri "$BaseUrl/simulate/clean" -Method Post -ErrorAction Stop | Out-Null
    } catch {
        Write-Host "Failed to clean state for Approach $app" -ForegroundColor Red
        continue
    }

    $numSubscribers = if ($app -eq 5) { 10 } else { 0 }
    
    $body = @{
        approach = [int]$app
        rps = 10
        gradual = $true
        endRps = 20000
        timeoutMs = 5000
        insertsOnly = $false
        numSubscribers = $numSubscribers
    } | ConvertTo-Json

    Write-Host "Starting simulation with gradual ramp-up..."
    try {
        Invoke-RestMethod -Uri "$BaseUrl/simulate/start" -Method Post -Body $body -ContentType "application/json" -ErrorAction Stop | Out-Null
    } catch {
        Write-Host "Failed to start simulation for Approach $app" -ForegroundColor Red
        continue
    }

    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $csvFile = "$outputDir\sim_app${app}_rampup_$timestamp.csv"
    $headers = "Time,Approach,RPS,ActualRPS,SuccessRate(%),AppLatency(ms),QueueLatency(ms),p95(ms),p99(ms),RecordsModified,RecordsFailed,RecordsLate,RecordsInKafka,Lag,ConsumerE2ELatency(ms),ConsumerEnrichmentLatency(ms),DB_CPU,DB_IO,DB_WaitTasks,DB_ActiveLocks,App_CPU,App_Mem,Debezium_CPU,Debezium_Mem,Kafka_CPU,Kafka_Mem,FlawAlert"
    Out-File -FilePath $csvFile -InputObject $headers -Encoding UTF8

    $prevRecordsModified = 0
    $limitReached = $false
    $limitReason = ""

    while (-not $limitReached) {
        Start-Sleep -Seconds 1
        
        $stats = Invoke-RestMethod -Uri "$BaseUrl/stats" -Method Get -ErrorAction SilentlyContinue
        if ($null -eq $stats) {
            continue
        }

        # Parse metrics
        $dbCpu = if ($null -ne $stats.dbStats -and $null -ne $stats.dbStats.cpu) { $stats.dbStats.cpu } else { 0 }
        $dbIo = if ($null -ne $stats.dbStats -and $null -ne $stats.dbStats.io) { $stats.dbStats.io } else { 0 }
        $dbWaits = if ($null -ne $stats.dbStats -and $null -ne $stats.dbStats.wait_tasks) { $stats.dbStats.wait_tasks } else { 0 }
        $dbLocks = if ($null -ne $stats.dbStats -and $null -ne $stats.dbStats.active_locks) { $stats.dbStats.active_locks } else { 0 }
        
        $appContainer = $null
        $debContainer = $null
        $kafContainer = $null
        
        if ($null -ne $stats.containerStats) {
            $appContainer = $stats.containerStats | Where-Object { $_.name -like '*sim_dataingestion-simulator*' } | Select-Object -First 1
            $debContainer = $stats.containerStats | Where-Object { $_.name -like '*debezium*' } | Select-Object -First 1
            $kafContainer = $stats.containerStats | Where-Object { $_.name -like '*kafka*' } | Select-Object -First 1
        }

        $appCpu = if ($appContainer) { $appContainer.cpu } else { "0%" }
        $appMem = if ($appContainer) { $appContainer.mem } else { "0%" }
        $debCpu = if ($debContainer) { $debContainer.cpu } else { "0%" }
        $debMem = if ($debContainer) { $debContainer.mem } else { "0%" }
        $kafCpu = if ($kafContainer) { $kafContainer.cpu } else { "0%" }
        $kafMem = if ($kafContainer) { $kafContainer.mem } else { "0%" }

        $time = (Get-Date).ToString("o")
        $actualRps = [Math]::Max(0, $stats.recordsModified - $prevRecordsModified)
        $prevRecordsModified = $stats.recordsModified
        
        $totalRecords = $stats.recordsModified + $stats.recordsFailed + $stats.recordsLate
        $successRate = 100
        if ($totalRecords -gt 0) {
            $successRate = [Math]::Round(($stats.recordsModified / $totalRecords) * 100, 2)
        }

        $consumerE2ELatency = 0
        $consumerEnrichmentLatency = 0
        if ($null -ne $stats.subscriberStats) {
            $consumerE2ELatency = $stats.subscriberStats.avgE2eLatency
            $consumerEnrichmentLatency = $stats.subscriberStats.avgEnrichmentLatency
        }

        $flawAlert = if ($null -ne $stats.flawAlert) { $stats.flawAlert } else { "" }

        $row = "$time,$($stats.approach),$($stats.rps),$actualRps,$successRate,$($stats.appLatency),$($stats.queueLatency),$($stats.p95),$($stats.p99),$($stats.recordsModified),$($stats.recordsFailed),$($stats.recordsLate),$($stats.recordsInKafka),$($stats.lag),$consumerE2ELatency,$consumerEnrichmentLatency,$dbCpu,$dbIo,$dbWaits,$dbLocks,$appCpu,$appMem,$debCpu,$debMem,$kafCpu,$kafMem,`"$flawAlert`""
        Add-Content -Path $csvFile -Value $row

        Write-Host -NoNewline "`rApproach $app | Target RPS: $($stats.rps) | Actual RPS: $actualRps | Success: $successRate% | Lag: $($stats.lag)         "

        # Check for failure conditions (limits)
        if ($stats.rps -gt 50) { # Give it a few seconds to warm up
            if ($successRate -lt 80) {
                $limitReached = $true
                $limitReason = "Success metric fell under 80% (Actual: $successRate%)"
            } elseif ($flawAlert -ne "") {
                $limitReached = $true
                $limitReason = "Flaw detected: $flawAlert"
            } elseif ($stats.appLatency -gt 5000) {
                $limitReached = $true
                $limitReason = "Service degrading: App Latency > 5000ms"
            }
        }
        
        # Stop condition if it maxes out at 20000 without failing
        if ($stats.rps -ge 20000) {
            $limitReached = $true
            $limitReason = "Reached max target RPS without failure."
        }
    }

    Write-Host "`nHard limit reached for Approach $app!" -ForegroundColor Red
    Write-Host "Reason: $limitReason" -ForegroundColor Red
    Write-Host "Stopping simulation..." -ForegroundColor Yellow

    try {
        Invoke-RestMethod -Uri "$BaseUrl/simulate/stop" -Method Post -ErrorAction SilentlyContinue | Out-Null
    } catch {}

    Write-Host "Stats saved to: $csvFile`n" -ForegroundColor Green
    
    # Wait a little bit for services to calm down before next approach
    Start-Sleep -Seconds 5
}

Write-Host "All approaches completed." -ForegroundColor Cyan
