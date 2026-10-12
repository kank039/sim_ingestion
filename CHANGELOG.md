# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - Exact, isolated approach runs

### Approach isolation
- Every run now provisions exactly one approach and tears it down afterwards: the Approach 1 trigger is only
  enabled for Approach 1, CDC is enabled only on the tables the approach needs, a fresh Debezium connector is
  created per run (`snapshot.mode=no_data`) and deleted after, the Flink job runs only for Approach 3, and
  Approach 5 consumer threads only exist during Approach 5 runs. Switching approaches (even mid-run) drains
  and tears down the previous one first.
- Startup removes leftovers from earlier sessions or older versions (legacy connectors, Flink jobs, CDC, trigger).
- A probe write must reach the approach's output topic before measurement starts.

### Approaches 3 and 4 implemented for real
- Approach 3: dual write with a correlation `change_token`, Debezium on both tables, Flink SQL interval join
  (`flink/sql/approach3.sql`, custom Flink image with the Kafka connector) writing `sim.approach3.enriched`.
- Approach 4: custom `InvoiceLookup` SMT (`debezium/smt`, custom Debezium image) enriching each CDC event via JDBC.
  The fake 5ms sleep + SELECT in the writer path is gone.

### Correct metrics
- Exact open-loop rate pacing (low and fractional RPS are honoured); issued vs committed RPS reported.
- True percentiles from merged histograms instead of averaged per-thread percentiles.
- Outcomes split into on time / late (committed) / failed / dropped; timed-out operations are no longer
  abandoned and miscounted; no phantom successes without a DB connection.
- Pipeline lag and delivered events counted per run on each approach's real output topic, from exact offsets.
- Commit → topic latency and consumer E2E latency use the source commit time (`source.ts_ms`).
- Consumer lag computed for Approach 5; pipeline alerts only for observed conditions (no fabricated "race condition").
- Verification rewritten: completeness, real duplicates, per-key ordering and per-entity partition spread.

### Workload data
- Reference data is seeded up to the selected cardinality (10k / 1M ids now join correctly).
- Indexes on `billing_record(batch_id)`, `subscriber_usage(batch_id)`, `rate_schedule(plan_id, usage_type)`.
- Approach 5 enriches by billing id instead of scanning every row of the batch; tombstones are skipped.
- Write path has no table variables (they caused tempdb metadata latch contention at high concurrency).

### Functional fixes
- Live changes use `/api/simulate/update` and no longer reset the run; approach / cardinality / subscribers are
  locked during a run. Pause stops the clock and the ramp.
- Run summary is computed by the server after draining, so the summary modal (save, verify) works again.
- Request body limit raised so long runs can be saved; clean/CDC/Kafka actions only run when idle and use
  TRUNCATE / disabling CDC instead of live deletes under Debezium.
- Telemetry polling no longer overlaps, uses one DMV batch and real container CPU limits; tempdb metric fixed;
  the Windows-only `d:\tmp` error log is gone.

### Tooling
- Central env-based config (`.env.example`), Docker socket auto-detected on Windows, configurable API base in the UI.
- Compiled production build on Node 22, SIGTERM handling, pinned kafka-ui, simulator container behind the `app` profile.
- Single schema source (`simulator/src/schema.ts`); removed `scripts/init.sql`, connector JSONs and registration scripts.
- Vitest unit tests, oxlint, GitHub Actions CI; PowerShell runners updated for the new API.
- Legacy CSV results moved to `archive/legacy_results/` (not comparable with new runs).

## [Previous]

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

## [2026-07-08] - Optimization of Transactional Outbox (Approach 2)
- **SQL Batching Fix:** Fixed the initial implementation of the Transactional Outbox pattern in `simulation.ts`. Replaced `mssql` client-side explicit transactions (which required multiple network round-trips while holding database locks) with a single batched T-SQL query (`BEGIN TRAN; ... COMMIT TRAN;`). This eliminated massive lock contention and connection pool exhaustion, increasing throughput from ~110 RPS to ~1,460 RPS.
- **CDC Connector Optimization:** Identified that Debezium was redundantly tracking both the `billing_record` and `outbox_events` tables for Approach 2, causing 2x the load on the CDC pipeline. Updated `register_connectors.ps1` and disabled CDC on unused tables using `sp_cdc_disable_table` so that Debezium strictly monitors `outbox_events`. This optimization more than doubled the throughput, taking the final limit to ~3,610 RPS.
- **Simulator CPU & Concurrency Doubled:** Doubled the Node.js application container's CPU allocation and increased internal worker threads from 4 to 8. This proved that the previous limit was heavily influenced by the simulator itself. With 8 workers, we pushed the database to its true physical limit, revealing lock pileups and connection pool saturation at around ~2,410 Target RPS (with actual physical throughput peaking at ~1,617 RPS).
- **Connection Multiplexing (Pool Expansion):** Replaced the original In-Memory OLTP plan (which was blocked because SQL Server forbids CDC on memory-optimized tables) with Connection Multiplexing. Massively increased the Node.js `mssql` connection pool from 200 to 2000. This offloaded connection queuing from the Node.js event loop directly to the database engine. SQL Server successfully absorbed the connections, raising the Target RPS limit by over 1,000 requests, pushing the final failure point to **~3,460 RPS**.
