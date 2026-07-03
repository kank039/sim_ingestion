param()

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "           Simulation Runner             " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

$BaseUrl = "http://localhost:3001/api"
$HealthUrl = "http://localhost:3001/health"

Write-Host "Performing pre-checks..." -ForegroundColor Yellow
$simulatorRunning = $false
try {
    $health = Invoke-RestMethod -Uri $HealthUrl -Method Get -ErrorAction Stop
    if ($health.status -eq 'ok') {
        $simulatorRunning = $true
        Write-Host "Simulator is already running." -ForegroundColor Green
    }
} catch {
    $simulatorRunning = $false
}

if (-not $simulatorRunning) {
    Write-Host "Simulator is not running. Starting the system (npm run dev) in a new window..." -ForegroundColor Yellow
    Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"npm run dev`"" -WorkingDirectory "$PSScriptRoot\.."
    
    Write-Host "Waiting for simulator backend to become ready..." -ForegroundColor Yellow
    $retries = 30
    $simulatorReady = $false
    while ($retries -gt 0 -and -not $simulatorReady) {
        Start-Sleep -Seconds 2
        try {
            $health = Invoke-RestMethod -Uri $HealthUrl -Method Get -ErrorAction Stop
            if ($health.status -eq 'ok') {
                $simulatorReady = $true
            }
        } catch {
            Write-Host "Waiting... ($retries attempts left)" -ForegroundColor Gray
            $retries--
        }
    }
    
    if (-not $simulatorReady) {
        Write-Host "Failed to start or connect to the simulator backend within 60 seconds." -ForegroundColor Red
        exit
    }
    Write-Host "Simulator is now up and running!" -ForegroundColor Green
}
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
    $timeoutMs = Read-Host "Enter Timeout in ms (e.g. 3000)"
}

$insertsOnlyStr = $null
while ([string]::IsNullOrWhiteSpace($insertsOnlyStr) -or $insertsOnlyStr -notin 'y','n','Y','N') {
    $insertsOnlyStr = Read-Host "Inserts Only? (y/n)"
}
$insertsOnly = $false
if ($insertsOnlyStr -eq 'y' -or $insertsOnlyStr -eq 'Y') { $insertsOnly = $true }

$duration = $null
while ([string]::IsNullOrWhiteSpace($duration) -or -not [int]::TryParse($duration, [ref]$null)) {
    $duration = Read-Host "Duration in seconds (e.g. 60)"
}


Write-Host "`nCleaning previous state..." -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "$BaseUrl/simulate/clean" -Method Post -ErrorAction Stop | Out-Null
    Write-Host "State cleaned." -ForegroundColor Green
} catch {
    Write-Host "Failed to clean state. Is the simulator running?" -ForegroundColor Red
    exit
}

Write-Host "Starting simulation (Approach: $approach, RPS: $rps, Timeout: ${timeoutMs}ms, InsertsOnly: $insertsOnly)..." -ForegroundColor Yellow
$body = @{
    approach = [int]$approach
    rps = [int]$rps
    timeoutMs = [int]$timeoutMs
    insertsOnly = $insertsOnly
    numSubscribers = [int]$numSubscribers
} | ConvertTo-Json

try {
    Invoke-RestMethod -Uri "$BaseUrl/simulate/start" -Method Post -Body $body -ContentType "application/json" -ErrorAction Stop | Out-Null
} catch {
    Write-Host "Failed to start simulation." -ForegroundColor Red
    exit
}

$outputDir = ".\output"
if (-not (Test-Path -Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$csvFile = "$outputDir\sim_app${approach}_rps${rps}_$timestamp.csv"

$headers = "Time,Approach,RPS,ActualRPS,SuccessRate(%),AppLatency(ms),QueueLatency(ms),p95(ms),p99(ms),RecordsModified,RecordsFailed,RecordsLate,RecordsInKafka,Lag,ConsumerE2ELatency(ms),ConsumerEnrichmentLatency(ms),DB_CPU,DB_IO,DB_WaitTasks,DB_ActiveLocks,App_CPU,App_Mem,Debezium_CPU,Debezium_Mem,Kafka_CPU,Kafka_Mem"
Out-File -FilePath $csvFile -InputObject $headers -Encoding UTF8

$prevRecordsModified = 0

Write-Host "`nSimulation running for $duration seconds. Recording stats per second..." -ForegroundColor Cyan
Write-Host ""

for ($i = 0; $i -lt $duration; $i++) {
    Start-Sleep -Seconds 1
    try {
        $stats = Invoke-RestMethod -Uri "$BaseUrl/stats" -Method Get -ErrorAction SilentlyContinue
        if ($null -ne $stats) {
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

            $row = "$time,$($stats.approach),$($stats.rps),$actualRps,$successRate,$($stats.appLatency),$($stats.queueLatency),$($stats.p95),$($stats.p99),$($stats.recordsModified),$($stats.recordsFailed),$($stats.recordsLate),$($stats.recordsInKafka),$($stats.lag),$consumerE2ELatency,$consumerEnrichmentLatency,$dbCpu,$dbIo,$dbWaits,$dbLocks,$appCpu,$appMem,$debCpu,$debMem,$kafCpu,$kafMem"
            
            Add-Content -Path $csvFile -Value $row
            
            $timeLeft = $duration - $i - 1
            Write-Host -NoNewline "`r[Time Left: $($timeLeft)s] Lag: $($stats.lag) | App Latency: $($stats.appLatency)ms | DB CPU: $($dbCpu)%    "
        }
    } catch {
        Write-Host -NoNewline "`rError fetching stats at second $i...                                        "
    }
}

Write-Host "`n`nStopping simulation..." -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "$BaseUrl/simulate/stop" -Method Post -ErrorAction SilentlyContinue | Out-Null
    Write-Host "Simulation finished. Stats saved to: $csvFile" -ForegroundColor Green
} catch {
    Write-Host "Simulation finished, but error occurred while stopping." -ForegroundColor Red
}
