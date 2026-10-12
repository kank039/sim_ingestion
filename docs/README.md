# Data Ingestion & Enrichment Simulation Documentation

## 1. Overview
The **Data Ingestion & Enrichment Simulation** is a comprehensive testing and benchmarking application designed to evaluate various data ingestion and enrichment patterns. The primary goal is to simulate high-throughput transactional loads, process them through different architectural patterns, and provide real-time telemetry on system performance, latency, and correctness.

This application is useful for architects and engineers comparing Event-Driven Architecture (EDA) approaches, particularly when integrating relational databases (SQL Server) with streaming platforms (Kafka) and stream processing frameworks (Flink).

## 2. Architecture

The system is composed of three main layers:

### 2.1 Infrastructure (Docker Compose)
The foundational layer is orchestrated via Docker Compose, running the following services:
- **SQL Server**: The primary relational database handling transactional inserts/updates/deletes.
- **Kafka**: The message broker used for streaming events.
- **Debezium**: A Change Data Capture (CDC) connector tailing SQL Server transaction logs and publishing changes to Kafka.
- **Apache Flink (JobManager & TaskManager)**: Used for stateful stream processing and stream-to-stream joins (custom image with the Kafka SQL connector and the Approach 3 job).

Debezium runs a custom image (`debezium/`) that adds the Approach 4 JDBC SMT. Connectors are **not** registered
by hand: the simulator creates exactly one connector per run for the selected approach and deletes it afterwards.

### 2.2 Backend Simulator (Node.js)
A Node.js backend acting as both the load generator and API server:
- **Worker Threads**: Uses `worker_threads` to generate concurrent database operations (Inserts, Updates, Deletes) based on user-defined RPS (Requests Per Second) and payload sizes.
- **Run Manager / Provisioner**: For each run, configures the database (trigger, CDC tables), Debezium connector,
  Flink job and consumer threads for the selected approach only, and tears everything down when the run ends.
  Switching approaches therefore never leaves the previous approach's machinery consuming resources.
- **Delivery Tracker**: Counts the run's events on the approach's output topic to measure pipeline lag and commit → topic latency.
- **Telemetry Server**: Exposes REST and Server-Sent Events (SSE) endpoints to stream real-time metrics (latency percentiles, queue depths, CPU/Memory usage via Docker socket) to the frontend.
- **Consumer Workers**: Dedicated worker threads for simulating consumer-side enrichment (used in Approach 5).

### 2.3 Frontend Dashboard (React + Vite)
A React-based web dashboard providing:
- **Control Panel**: Start, stop, pause, and configure the simulation (Rps, cardinality, approach selection).
- **Real-time Telemetry**: Live charts visualizing App Latency, Queue Latency, P95/P99 latencies, CPU/Memory utilization, and Kafka lags.
- **System Logs**: Real-time logging of provisioning steps, teardown, errors and pipeline alerts.
- **Run History & CSV Export**: Allows users to save simulation runs and export telemetry data for offline analysis.

## 3. Approaches Evaluated

The simulator implements and benchmarks five distinct data ingestion and enrichment strategies:

1. **Database Triggers (DBA Outbox)**: Relies on a native database trigger to populate a shadow/outbox table (`cdc_events_shadow`).
2. **Transactional Outbox (Recommended)**: The application writes to the domain table and an outbox table within a single ACID transaction. This is the most reliable pattern for guaranteed delivery without dual-write issues.
3. **Stream-to-Stream Join (Flink)**: The app dual-writes `billing_record` and `enrichment_requests`; a Flink interval join combines both CDC streams. (Note: late data outside the join window is dropped; the dual write is not atomic).
4. **JDBC SMT (Single Message Transform)**: A custom SMT inside the Debezium task enriches each CDC event with a synchronous JDBC lookup of `invoice_batch`. (Can cause race conditions if the lookup data changes rapidly).
5. **CDC Push + Consumer Enrichment (Multi-JOIN)**: Debezium pushes raw CDC events to Kafka, and downstream consumer workers enrich the events by performing non-blocking multi-table JOINs against read-replicas or the primary DB.

## 4. Getting Started

### Prerequisites
- Docker and Docker Compose
- Node.js 22+ and npm

### Installation & Execution
1. Build and start the infrastructure, then install dependencies (`./setup.sh` or `setup.ps1` do both):
   ```bash
   docker compose up -d --build
   npm run install:all
   ```
2. Run the simulator and dashboard:
   ```bash
   npm run dev
   ```
   On first start the simulator creates `sim_db`, applies the schema and resets the pipeline to a neutral state.
3. Open `http://localhost:5173`, pick an approach and press **Start**. The run first *provisions* (a few seconds,
   longer for Flink), then load starts. **Stop** drains the pipeline, shows the run summary and tears down.

To run the simulator in Docker instead of on the host: `docker compose --profile app up -d --build`.
Configuration (DB credentials, endpoints, thread counts) is read from `.env`; see `.env.example`.

### Scripts
- `scripts/run_simulation.ps1`: interactive single run, records one CSV row per second to `output/`.
- `scripts/auto_rampup_simulation.ps1 [-Approaches 1,2,3] [-MaxRps 20000]`: ramps each approach until it hits a limit.
- `npm test` (root): simulator unit tests + frontend lint.

See [metrics_calculation.md](metrics_calculation.md) for exactly how every metric is computed.

## 5. Suggested Improvements & Future Scope

While the current simulation provides a strong baseline, several enhancements could elevate its utility for enterprise evaluation:

### 5.1 Architecture & Observability
- **Prometheus/Grafana Integration**: Export metrics directly to Prometheus instead of computing percentiles manually in Node.js. This would allow for more robust visualization in Grafana and historical data retention.
- **Distributed Tracing (OpenTelemetry)**: Add tracing to follow a single transaction from the initial SQL Server write, through Debezium, into Kafka, and finally out through the consumer worker.
- **Dead Letter Queue (DLQ) Implementation**: Simulate and visualize message failures and DLQ routing in the consumer workers.

### 5.2 Simulation Capabilities
- **Chaos Engineering**: Expand the "Induce Failure" capability to simulate network partitions, Kafka broker failures, or Flink TaskManager crashes to measure system recovery time (MTTR) and data loss.
- **Dynamic Load Profiles**: Allow users to upload a CSV of load profiles (e.g., simulating daily traffic spikes, Black Friday traffic) rather than static or linear RPS ramping.

### 5.3 Infrastructure Additions
- **Kubernetes Support**: Provide Helm charts or Kubernetes manifests to benchmark the approaches in a container-orchestrated environment, allowing for pod autoscaling scenarios.
- **Alternative Message Brokers & DBs**: Add support for PostgreSQL and Redis to see how the outbox pattern performs across different database engines, or replace Kafka with Pulsar/RabbitMQ.
- **Schema Registry Integration**: Enforce and benchmark Avro/Protobuf serialization overhead via Confluent Schema Registry.

### 5.4 Frontend Enhancements
- **Topology Visualizer**: Create a visual node graph (similar to Flink's UI or Confluent Control Center) showing the live flow of data between SQL Server -> Debezium -> Kafka -> Consumers.
- **Automated Benchmarking Reports**: Generate automated PDF/Markdown reports comparing two approaches side-by-side after their runs complete.
