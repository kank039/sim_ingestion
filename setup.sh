#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "Building and starting infrastructure (SQL Server, Kafka, Debezium + SMT, Flink)..."
docker compose up -d --build

echo "Installing Node dependencies..."
npm run install:all

echo
echo "Setup complete. Run 'npm run dev' from the repo root."
echo "The simulator creates the database on first start and provisions the pipeline for each run."
