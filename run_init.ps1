$ErrorActionPreference = 'Stop'
Write-Host "Waiting for SQL Server to be healthy..."

$healthy = $false
while (-not $healthy) {
    $status = docker inspect -f '{{.State.Health.Status}}' sqlserver
    if ($status -eq 'healthy') {
        $healthy = $true
    } else {
        Write-Host "Current status: $status. Retrying in 5 seconds..."
        Start-Sleep -Seconds 5
    }
}

Write-Host "SQL Server is healthy! Running init.sql..."
docker cp .\scripts\init.sql sqlserver:/init.sql
docker exec -i sqlserver /opt/mssql-tools18/bin/sqlcmd -U sa -P Password123! -C -N -i /init.sql
Write-Host "Database initialization completed."
