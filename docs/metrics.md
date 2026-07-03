# Telemetry & Dashboard Metrics

This document outlines the various metrics tracked during the simulation, displayed on the live React dashboard, and exported in the CSV/Excel files. Understanding these metrics is crucial for evaluating the performance and bottlenecks of each ingestion/enrichment approach.

## 1. Latency & Performance Metrics

- **Application Latency (`appLatency`)**
  - **What it is**: The average round-trip time (in milliseconds) for the simulator application to successfully execute a database operation (Insert, Update, or Delete).
  - **Importance**: Demonstrates the direct impact of an architectural approach on the primary database write path. For example, Database Triggers (Approach 1) typically increase this latency significantly compared to the Transactional Outbox (Approach 2).

- **Queue Latency (`queueLatency`)**
  - **What it is**: The time operations spend waiting in the Node.js internal execution queue before a database connection becomes available.
  - **Importance**: High queue latency indicates that the database connection pool is exhausted or the system is saturated and cannot process requests as fast as the target RPS (Requests Per Second).

- **P95 / P99 Latency (`p95`, `p99`)**
  - **What it is**: The 95th and 99th percentile latencies for database operations.
  - **Importance**: Crucial for understanding "tail latency." While average latency might look fine, high P99 latency indicates that a small percentage of users are experiencing severe delays, often pointing to garbage collection pauses, lock contention in SQL Server, or network hiccups.

## 2. Throughput & Reliability Metrics

- **Records Modified (`recordsModified`)**
  - **What it is**: The cumulative number of successful database mutations performed during the current run.
  - **Importance**: Represents the actual throughput achieved by the system.

- **Records Failed (`recordsFailed`)**
  - **What it is**: The number of operations that threw a database error (e.g., deadlocks or connection failures).
  - **Importance**: A high failure count indicates the system has hit a hard bottleneck or database issues.

- **Records Late (`recordsLate`)**
  - **What it is**: The number of operations that timed out (exceeded `timeoutMs`).
  - **Importance**: A high late count indicates processing delays and excessive wait times in the queue.

- **Success Rate (`successRate`)**
  - **What it is**: The percentage of successful requests out of the total attempted requests.
  - **Importance**: A direct indicator of system stability under the configured RPS load.

## 3. Data Pipeline & CDC Metrics

- **Records In Kafka (`recordsInKafka`)**
  - **What it is**: The total number of events successfully captured by Debezium and committed to the Kafka topic.
  - **Importance**: Used to verify that events are actually flowing through the streaming infrastructure.

- **Capture Lag (`lag`)**
  - **What it is**: The numerical difference between `Records Modified` (in the database) and `Records in Kafka`. 
  - **Importance**: Arguably the most critical metric for evaluating CDC performance. If lag continually increases, it means the CDC connector (Debezium) or the Kafka cluster cannot keep up with the database's write throughput, leading to stale downstream data.

## 4. Consumer / Enrichment Metrics

- **Consumer Enrichment Latency (`consumerEnrichmentLatency` / `avgEnrichmentLatency`)**
  - **What it is**: The time taken by the Node.js consumer worker to execute the multi-table JOINs (enrichment queries) against the database.
  - **Importance**: Measures the cost of doing read-side enrichment. If this is high, it suggests the database is struggling with the read volume or the queries lack proper indexing.
  - **Applicability**: Applicable ONLY to **Approach 5** (CDC Push + Consumer Enrichment).

- **Consumer End-to-End Latency (`consumerE2eLatency` / `avgE2eLatency`)**
  - **What it is**: The total time elapsed from when an event was originally captured/published to when it finished being enriched by the consumer worker.
  - **Importance**: The ultimate measure of freshness for downstream systems. Tells you exactly how long it takes for a mutation to become fully enriched and actionable.
  - **Applicability**: Applicable ONLY to **Approach 5** (CDC Push + Consumer Enrichment).

## 5. Infrastructure Metrics

- **CPU Utilization (`cpu`)**
  - **What it is**: The percentage of allocated CPU limit currently being used by the Docker containers (SQL Server, Kafka, Debezium, Flink, Simulator).
  - **Importance**: Identifies exactly which component is the bottleneck under high load.

- **Memory/IO Utilization (`io` / `mem`)**
  - **What it is**: The memory consumption relative to the container's limit.
  - **Importance**: Helps identify memory leaks (e.g., unbounded state growth in Flink or Debezium heap exhaustion) and excessive disk buffering.
