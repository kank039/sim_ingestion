$ErrorActionPreference = 'Stop'

Write-Host "Starting Docker containers..."
docker-compose up -d

Write-Host "Waiting for database and kafka to be ready..."
.\run_init.ps1

Write-Host "Registering Debezium connectors..."
Start-Sleep -Seconds 10
.\register_connectors.ps1

Write-Host "Installing dependencies..."
npm install
Push-Location simulator
npm ci
Pop-Location
Push-Location frontend
npm install
Pop-Location

Write-Host "Setup complete. You can now run 'npm run dev' from the root directory."
