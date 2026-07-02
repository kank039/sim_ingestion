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
- **Apache Flink (JobManager & TaskManager)**: Used for stateful stream processing and stream-to-stream joins.

### 2.2 Backend Simulator (Node.js)
A Node.js backend acting as both the load generator and API server:
- **Worker Threads**: Uses `worker_threads` to generate concurrent database operations (Inserts, Updates, Deletes) based on user-defined RPS (Requests Per Second) and payload sizes.
- **Approach Executors**: Implements the logic for different ingestion patterns.
- **Telemetry Server**: Exposes REST and Server-Sent Events (SSE) endpoints to stream real-time metrics (latency percentiles, queue depths, CPU/Memory usage via Docker socket) to the frontend.
- **Consumer Workers**: Dedicated worker threads for simulating consumer-side enrichment (used in Approach 5).

### 2.3 Frontend Dashboard (React + Vite)
A React-based web dashboard providing:
- **Control Panel**: Start, stop, pause, and configure the simulation (Rps, cardinality, approach selection).
- **Real-time Telemetry**: Live charts visualizing App Latency, Queue Latency, P95/P99 latencies, CPU/Memory utilization, and Kafka lags.
- **System Logs**: Real-time logging of system events and alerts (e.g., race conditions, state TTL warnings).
- **Run History & CSV Export**: Allows users to save simulation runs and export telemetry data for offline analysis.

## 3. Approaches Evaluated

The simulator implements and benchmarks five distinct data ingestion and enrichment strategies:

1. **Database Triggers (DBA Outbox)**: Relies on native database triggers to populate an outbox table.
2. **Transactional Outbox (Recommended)**: The application writes to the domain table and an outbox table within a single ACID transaction. This is the most reliable pattern for guaranteed delivery without dual-write issues.
3. **Stream-to-Stream Join (Flink)**: Uses Apache Flink to join disparate streams in real-time. (Note: May face issues with state TTL and late-arriving data).
4. **JDBC SMT (Single Message Transform)**: Uses Kafka Connect SMTs to enrich CDC events synchronously via JDBC lookups. (Can cause race conditions if the lookup data changes rapidly).
5. **CDC Push + Consumer Enrichment (Multi-JOIN)**: Debezium pushes raw CDC events to Kafka, and downstream consumer workers enrich the events by performing non-blocking multi-table JOINs against read-replicas or the primary DB.

## 4. Getting Started

### Prerequisites
- Docker and Docker Compose
- Node.js (v18+)
- npm

### Installation & Execution
1. Install dependencies:
   ```bash
   npm install
   ```
2. Start the infrastructure (if not already running via script):
   ```bash
   docker-compose up -d
   ```
3. Run the application (Starts both simulator and frontend concurrently):
   ```bash
   npm run dev
   ```
4. Access the frontend dashboard at `http://localhost:5173` (or the port defined by Vite).

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
