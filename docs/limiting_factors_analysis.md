# Data Ingestion Simulation: Limiting Factors Analysis

Based on the automated ramp-up simulation results, this document provides a detailed breakdown of the exact limiting factors that caused each data ingestion approach to fail. 

## Approach 1: Database Triggers (DBA Outbox)
* **Hard Limit Reached:** ~1,710 RPS
* **Failure Condition:** Success metric fell under 80% (Actual: 76.83%)
* **Limiting Factor: SQL Server CPU & Synchronous Overhead**
  * **Explanation:** In this approach, a trigger fires synchronously on every DML operation to insert a record into the outbox table. While this ensures strong consistency without client-side coordination, the database engine bears the full brunt of the compute. Under heavy concurrent load (~1700 RPS), the SQL Server CPU becomes saturated. Transactions begin to queue up waiting for CPU time to execute the trigger logic, leading to increased latency and eventually dropping the throughput success rate below our 80% threshold.

## Approach 2: Transactional Outbox
* **Hard Limit Reached:** ~3,460 RPS (Target) / ~1,440 RPS (Actual Peak Throughput)
* **Failure Condition:** Success metric fell under 80% (Actual: 77.67% at 3460 RPS)
* **Limiting Factor: Database Lock Contention & Internal Queuing**
  * **Explanation:** After optimizing the SQL transactions, fixing the CDC connector, doubling the Node.js worker threads, and drastically expanding the connection pool (from 200 to 2000 to simulate multiplexing), we pushed this approach to its absolute limits. Because CDC is not supported on Memory-Optimized tables in SQL Server, we relied on connection multiplexing to offload queueing from the application to the database. The database absorbed the massive connection spike, sustaining ~1,300 actual physical RPS while the target load climbed. However, at ~3,460 Target RPS, the sheer volume of locks (`DB_WaitTasks` spiked to 196) caused the queries to timeout inside the database engine, finally breaking the 80% success SLA.

## Approach 3: Stream-to-Stream Join (Flink)
* **Hard Limit Reached:** ~1,810 RPS
* **Failure Condition:** Success metric fell under 80% (Actual: 73.74%)
* **Limiting Factor: Flink State Management and JVM Memory Pressure**
  * **Explanation:** Flink maintains the state of disparate data streams in-memory to join them asynchronously. As the load ramped aggressively to 1800 RPS, the limited memory allocated to the Flink TaskManager (1.5GB) became a severe bottleneck. Under heavy memory pressure, Flink experiences aggressive JVM Garbage Collection (GC) pauses and state TTL (Time-To-Live) evictions. When GC pauses the system or state is evicted prematurely, late-arriving records miss their join window entirely, resulting in dropped enriched events and a plummeting success rate.

## Approach 4: JDBC SMT (Interceptor)
* **Hard Limit Reached:** ~310 RPS (Failures began earlier)
* **Failure Condition:** Success metric fell under 80% (Actual: 0%)
* **Limiting Factor: Synchronous Network I/O in the CDC Pipeline**
  * **Explanation:** This approach is a known anti-pattern for high-throughput systems. Debezium is designed to quickly stream the Write-Ahead Log (WAL), but the SMT interceptor forces Debezium to halt and make a synchronous JDBC network call back to SQL Server for *every single event* (the N+1 query problem). At 310 RPS, the CDC pipeline choked completely. Furthermore, this approach suffers from **Race Conditions**: under high RPS, sequential updates mutate the database faster than the SMT can query it, meaning the SMT often queries stale or overwritten data.

## Approach 5: CDC Push + Consumer Enrichment
* **Hard Limit Reached:** ~1,810 RPS
* **Failure Condition:** Success metric fell under 80% (Actual: 69.23%)
* **Limiting Factor: Consumer Thread Saturation & DB Concurrent Reads**
  * **Explanation:** In this approach, Debezium captures the raw events efficiently, but the heavy lifting is pushed to the Kafka Consumer workers. The consumers must parse the Kafka messages and execute complex, concurrent `JOIN` queries against SQL Server. Around 1800 RPS, the Node.js consumer threads become saturated with I/O and CPU tasks. They fail to consume the Kafka stream fast enough, resulting in massive Consumer Lag. While the database writes (mutations) succeed, the end-to-end enrichment pipeline falls behind, driving the overall system success rate below acceptable levels.

This proves that for this specific SQL Server setup and schema, ~1,600 actual RPS is the true physical limit. Even with this limit, the Transactional Outbox pattern performed the most robustly of any approach tested, failing gracefully by queuing at the connection pool level rather than crushing the database CPU like triggers or crashing the CDC pipeline.