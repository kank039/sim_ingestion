param()

$ConnectorsDir = "$PSScriptRoot\connectors"
$DebeziumUrl = "http://localhost:8083/connectors"

Write-Host "Registering Debezium connectors..." -ForegroundColor Cyan

$files = Get-ChildItem -Path $ConnectorsDir -Filter "*.json"

foreach ($file in $files) {
    Write-Host "Registering connector from $($file.Name)..."
    try {
        $json = Get-Content -Path $file.FullName -Raw
        $response = Invoke-RestMethod -Uri $DebeziumUrl -Method Post -Body $json -ContentType "application/json" -ErrorAction Stop
        Write-Host "Successfully registered $($file.Name)" -ForegroundColor Green
    } catch {
        Write-Host "Failed to register $($file.Name). Check if it already exists or if Debezium is running." -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
    }
}

Write-Host "Connector registration script completed." -ForegroundColor Cyan
