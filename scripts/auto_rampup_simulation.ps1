param(
    [int[]]$Approaches = @(1, 2, 3, 4, 5),
    [int]$StartRps = 10,
    [int]$MaxRps = 20000,
    [int]$RampStepRps = 50,
    [int]$TimeoutMs = 5000,
    [int]$Subscribers = 10
)

. "$PSScriptRoot/sim_common.ps1"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "       Automated Simulation Rampup       " -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan

if (-not (Test-SimulatorReady)) {
    Write-Host "Simulator is not running. Please start it with 'npm run dev' first." -ForegroundColor Red
    exit 1
}
Write-Host "Simulator is running." -ForegroundColor Green

$outputDir = Join-Path $PSScriptRoot "../output"
if (-not (Test-Path -Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

foreach ($app in $Approaches) {
    Write-Host "`n--- Approach $app ---" -ForegroundColor Yellow
    Write-Host "Provisioning (fresh data; previous approach fully torn down)..."
    try {
        Start-SimulationRun @{
            approach = [int]$app
            rps = $StartRps
            gradual = $true
            endRps = $MaxRps
            rampStepRps = $RampStepRps
            timeoutMs = $TimeoutMs
            insertsOnly = $false
            numSubscribers = $(if ($app -eq 5) { $Subscribers } else { 0 })
            resetData = $true
        }
    } catch {
        Write-Host "`nFailed to start Approach ${app}: $($_.Exception.Message)" -ForegroundColor Red
        continue
    }

    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $csvFile = Join-Path $outputDir "sim_app${app}_rampup_$timestamp.csv"
    Out-File -FilePath $csvFile -InputObject $script:CsvHeader -Encoding UTF8

    $limitReached = $false
    $limitReason = ""
    $atMaxSince = $null
    while (-not $limitReached) {
        Start-Sleep -Seconds 1
        $stats = $null
        try { $stats = Invoke-RestMethod -Uri "$script:BaseUrl/stats" -Method Get -ErrorAction Stop } catch { continue }

        Add-Content -Path $csvFile -Value (ConvertTo-StatsRow $stats)
        $rates = Get-OutcomeRates $stats
        Write-Host -NoNewline "`rApproach $app | Target RPS: $($stats.rps) | Actual RPS: $($stats.actualRps) | SLA: $($rates.Sla)% | Lag: $($stats.lag)         "

        if ($stats.phase -ne 'running') {
            $limitReached = $true
            $limitReason = "Run left the running phase ($($stats.phase))"
            continue
        }
        if ($stats.elapsedSec -le 5) { continue } # warm-up

        if ($rates.Success -lt 80) {
            $limitReached = $true
            $limitReason = "Success fell under 80% ($($rates.Success)%)"
        } elseif ($stats.flawAlert) {
            $limitReached = $true
            $limitReason = "Pipeline alert: $($stats.flawAlert)"
        } elseif ($stats.p95 -gt $TimeoutMs) {
            $limitReached = $true
            $limitReason = "DB p95 latency above the ${TimeoutMs}ms SLA"
        } elseif ($stats.rps -ge $MaxRps) {
            # Actual RPS is a 5s rolling rate, so give it time to settle at the final target.
            if ($null -eq $atMaxSince) { $atMaxSince = Get-Date }
            $atMax = ((Get-Date) - $atMaxSince).TotalSeconds
            if ($atMax -ge 10 -and $stats.actualRps -lt $MaxRps * 0.9) {
                $limitReached = $true
                $limitReason = "At max target but committing only $($stats.actualRps) RPS"
            } elseif ($atMax -ge 20) {
                $limitReached = $true
                $limitReason = "Sustained the max target RPS without failure."
            }
        }
    }

    Write-Host "`nLimit reached for Approach ${app}: $limitReason" -ForegroundColor Red
    Write-Host "Stopping and draining..." -ForegroundColor Yellow
    try {
        Write-RunSummary (Stop-SimulationRun)
    } catch {
        Write-Host "Error while stopping: $($_.Exception.Message)" -ForegroundColor Red
    }
    Write-Host "Stats saved to: $csvFile`n" -ForegroundColor Green
}

Write-Host "All approaches completed." -ForegroundColor Cyan
