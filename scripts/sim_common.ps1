# Shared helpers for the PowerShell runners. Dot-source this file.

$script:BaseUrl = "http://localhost:3001/api"
$script:HealthUrl = "http://localhost:3001/health"

$script:CsvHeader = "Time,Phase,Approach,TargetRPS,ActualRPS,IssuedRPS,SLA(%),Success(%),DbLatencyAvg(ms),DbP95(ms),DbP99(ms),QueueLatency(ms),Committed,OnTime,Late,Failed,Dropped,Delivered,Lag,CommitToTopicAvg(ms),CommitToTopicP95(ms),ConsumerE2EAvg(ms),ConsumerEnrichAvg(ms),ConsumerLag,DB_CPU,DB_IO,DB_WaitTasks,DB_ActiveLocks,App_CPU,App_Mem,Debezium_CPU,Debezium_Mem,Kafka_CPU,Kafka_Mem,Flink_TM_CPU,Flink_TM_Mem,Alert"

function Test-SimulatorReady {
    try {
        $health = Invoke-RestMethod -Uri $script:HealthUrl -Method Get -ErrorAction Stop
        return $health.status -eq 'ok'
    } catch {
        return $false
    }
}

function Get-OutcomeRates($stats) {
    $total = $stats.recordsModified + $stats.recordsLate + $stats.recordsFailed + $stats.recordsDropped
    if ($total -le 0) { return @{ Sla = 100; Success = 100 } }
    return @{
        Sla = [Math]::Round(($stats.recordsModified / $total) * 100, 2)
        Success = [Math]::Round((($stats.recordsModified + $stats.recordsLate) / $total) * 100, 2)
    }
}

function Get-Container($stats, $pattern) {
    if ($null -eq $stats.containerStats) { return $null }
    return $stats.containerStats | Where-Object { $_.name -like $pattern } | Select-Object -First 1
}

function ConvertTo-StatsRow($stats) {
    $rates = Get-OutcomeRates $stats
    $db = $stats.dbStats
    $app = Get-Container $stats 'simulator'
    $deb = Get-Container $stats 'debezium'
    $kaf = Get-Container $stats 'kafka'
    $ftm = Get-Container $stats 'flink-taskmanager'
    $sub = $stats.subscriberStats
    $cpu = { param($c) if ($c) { $c.cpu } else { "" } }
    $mem = { param($c) if ($c) { $c.mem } else { "" } }
    $alert = if ($stats.flawAlert) { $stats.flawAlert -replace '"', "'" } else { "" }
    $fields = @(
        (Get-Date).ToString("o"), $stats.phase, $stats.approach, $stats.rps, $stats.actualRps, $stats.issuedRps,
        $rates.Sla, $rates.Success, $stats.appLatency, $stats.p95, $stats.p99, $stats.queueLatency,
        $stats.recordsCommitted, $stats.recordsModified, $stats.recordsLate, $stats.recordsFailed, $stats.recordsDropped,
        $stats.recordsInKafka, $stats.lag, $stats.pipelineLatency.avg, $stats.pipelineLatency.p95,
        $(if ($sub) { $sub.avgE2eLatency } else { "" }), $(if ($sub) { $sub.avgEnrichmentLatency } else { "" }), $(if ($sub) { $sub.consumerLag } else { "" }),
        $db.cpu, $db.io, $db.wait_tasks, $db.active_locks,
        (& $cpu $app), (& $mem $app), (& $cpu $deb), (& $mem $deb), (& $cpu $kaf), (& $mem $kaf), (& $cpu $ftm), (& $mem $ftm),
        "`"$alert`""
    )
    return ($fields -join ',')
}

# Starts a run and blocks until its pipeline is provisioned and load is running.
function Start-SimulationRun($body) {
    $json = $body | ConvertTo-Json
    try {
        Invoke-RestMethod -Uri "$script:BaseUrl/simulate/start" -Method Post -Body $json -ContentType "application/json" -TimeoutSec 900 -ErrorAction Stop | Out-Null
    } catch {
        $detail = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
        throw "Start rejected: $detail"
    }
    while ($true) {
        Start-Sleep -Seconds 1
        $s = Invoke-RestMethod -Uri "$script:BaseUrl/stats" -Method Get -ErrorAction Stop
        if ($s.phase -eq 'running') { return }
        if ($s.phase -eq 'idle') { throw "Provisioning failed: $($s.lastError)" }
        Write-Host -NoNewline "`r  $($s.phase): $($s.phaseDetail)                                        "
    }
}

# Stops the run, waits for the pipeline to drain and returns the final run summary.
function Stop-SimulationRun {
    $body = @{ waitUntilIdle = $true } | ConvertTo-Json
    Invoke-RestMethod -Uri "$script:BaseUrl/simulate/stop" -Method Post -Body $body -ContentType "application/json" -TimeoutSec 900 -ErrorAction Stop | Out-Null
    $s = Invoke-RestMethod -Uri "$script:BaseUrl/stats" -Method Get -ErrorAction Stop
    return $s.lastRunSummary
}

function Write-RunSummary($summary) {
    if ($null -eq $summary) { return }
    Write-Host ("Committed: {0} (on time {1}, late {2}) | Failed: {3} | Dropped: {4} | Delivered: {5} | Final lag: {6}" -f `
        $summary.recordsCommitted, $summary.recordsModified, $summary.recordsLate, $summary.recordsFailed, $summary.recordsDropped, $summary.recordsInKafka, $summary.lag)
    Write-Host ("DB latency p50/p95/p99: {0}/{1}/{2} ms | Commit->topic p95: {3} ms | Avg committed RPS: {4}" -f `
        $summary.p50, $summary.p95, $summary.p99, $summary.pipelineLatency.p95, $summary.avgCommittedRps)
}
