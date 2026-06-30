# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### ⚡ Backend Performance & Architecture
- **Concurrency Limiting**: Implemented a concurrency limit using an internal async queue in `simulator/src/simulation.ts` to restrict in-flight DB operations to 50 concurrent transactions per worker, mitigating cascading `TimeoutError` exceptions.
- **Latency Memory Optimization**: Refactored the `latencyHistory` array to use a circular buffer instead of `.shift()` for O(1) performance when tracking the last 500 records.
- **Docker Stats Refactor**: Shifted from spawning a child process (`docker stats`) to directly calling the Docker Engine API over the mounted Unix socket `/var/run/docker.sock` in `simulator/src/index.ts`.
- **Worker Latency**: Ensured accurate latency reporting by aggregating a per-worker latency map instead of overriding global latency values.
- **Graceful Shutdown**: Intercepted `SIGINT` to gracefully terminate worker threads, close the Kafka admin client, and safely shut down the MSSQL connection pool.
- **Kafka Resilience**: Introduced retry logic with a 5-second backoff for the Kafka Admin client connection.
- **Healthcheck**: Added a `/health` endpoint to monitor worker and DB connection health.

### 🖥️ Frontend Performance & UX
- **Real-Time Data Streaming**: Migrated the polling-based interval to Server-Sent Events (SSE) via the `/api/stats/stream` endpoint for real-time telemetry updates.
- **Component Decomposition**: Refactored the monolithic `App.tsx` into modular components (`ControlPanel`, `SystemHealth`, `MetricsGrid`, `TelemetryChart`, `SystemLogs`).
- **Custom Hooks**: Extracted state logic into `useSimulationStats` and `useSimulationControl`.
- **Chart Optimizations**: Wrapped Recharts components in `React.memo` and memoized `chartData` to prevent redundant cascading re-renders. Added a `Brush` component for zoom/pan capabilities.
- **Debounced Load Simulator**: Implemented debouncing (250ms) on the RPS range slider to avoid spamming the backend during drags.
- **UX Improvements**:
  - Added a connection status indicator (green/red dot) and toast notification if the backend goes down.
  - Added skeleton loading states for System Health when disconnected.
  - Added a confirmation modal prior to issuing the destructive `stop and clean` command.
  - Persisted the sidebar toggle and component visibility settings to `localStorage`.
  - Added "Download CSV" capability for telemetry history export.
- **Type Safety**: Converted `any` configurations to explicit interfaces (`ContainerStat`, `TelemetryPoint`, `SystemLog`, `SimulationStats`).

### 🏗️ Architecture, Tooling & DX
- **Database Idempotency**: Updated `scripts/init.sql` and `run_init.ps1` with `IF NOT EXISTS` guards for safe, repeatable database, table, and trigger creation.
- **Unified Dev Experience**: Introduced a root `package.json` with a `concurrently` script for launching both frontend and backend seamlessly via `npm run dev`.
- **Environment Automation**: Added a `.env.example` template and an exhaustive `setup.ps1` script to automate Docker initialization, SQL setup, dependency installation, and connector registration.
- **Container CI/CD**: Refactored `simulator/Dockerfile` to utilize `npm ci` ensuring deterministic builds, and injected a native `curl` based healthcheck into `docker-compose.yml`.
- **Testing & Linting**: Added `eslint` and `vitest` to `simulator/package.json` for base linting and unit testing setup.

### 📊 Observability
- **Log Retention & Typing**: Expanded system log retention from 50 to 500 entries with distinct log levels (`info`, `error`).
