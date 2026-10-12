$ErrorActionPreference = 'Stop'

Write-Host "Building and starting infrastructure (SQL Server, Kafka, Debezium + SMT, Flink)..."
docker compose up -d --build

Write-Host "Installing Node dependencies..."
npm run install:all

Write-Host ""
Write-Host "Setup complete. Run 'npm run dev' from the repo root."
Write-Host "The simulator creates the database on first start and provisions the pipeline for each run;"
Write-Host "no manual SQL scripts or connector registration are needed."
