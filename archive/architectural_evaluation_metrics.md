# Architectural Evaluation Metrics for Data Ingestion

When evaluating different data ingestion strategies (e.g., Transactional Outbox, CDC Triggers, CDC log-based capture), the architecture team must focus on how each approach extracts data from the source system and delivers it to the messaging backbone (Kafka).

The following metrics specifically measure the efficiency, impact, and limits of the ingestion phase, along with methods to track them during simulation and testing.

## 1. Source Database Impact & Operational Overhead
The most critical factor in any ingestion strategy is how it affects the primary source database.

* **Transaction Log (LDF) Growth Rate**
  * *Why it matters:* Log-based CDC approaches rely on the transaction log. If the CDC reader falls behind, the log cannot truncate, which can consume available disk space and impact database IOPS.
  * *How to track:* Monitor `sys.dm_db_log_space_usage` or SQL Server Performance Monitor (PerfMon) counters for `Log File(s) Size (KB)` and `Log File(s) Used Size (KB)` over time during load tests.

* **CPU and IOPS Overhead**
  * *Why it matters:* How much extra CPU does the ingestion method consume on the database server? (e.g., SQL Server triggers add synchronous CPU overhead; polling queries consume CPU and IOPS).
  * *How to track:* Use SQL Server DMV `sys.dm_exec_query_stats` to track CPU time for specific ingestion queries/triggers. For containerized environments, monitor `docker stats` for the SQL Server container.

* **Buffer Cache Hit Ratio / Pollution**
  * *Why it matters:* Does the ingestion method require reading historical or relational data that pollutes the SQL Server buffer pool?
  * *How to track:* Query `sys.dm_os_performance_counters` for `Buffer cache hit ratio` and monitor `Page life expectancy` (PLE). A sharp drop in PLE indicates buffer pool pollution.

* **Lock Contention and Blocking**
  * *Why it matters:* Does the ingestion strategy introduce new locks? (e.g., synchronous triggers block the original transaction; Outbox polling can cause lock escalation).
  * *How to track:* Monitor `sys.dm_os_wait_stats` (specifically `LCK_M_*` waits) and `sys.dm_tran_locks`. Use SQL Server Extended Events to capture Blocked Process Reports during high concurrency.

* **TempDB Pressure**
  * *Why it matters:* Do the ingestion or polling queries require significant TempDB space for hashing, sorting, or spooling?
  * *How to track:* Monitor `sys.dm_db_file_space_usage` and `sys.dm_db_task_space_usage` specifically within the `tempdb` database during peak load.

## 2. Ingestion Latency & Throughput Limits
Measuring the speed and capacity of the ingestion pipeline before it reaches downstream consumers.

* **Commit-to-Kafka Latency (Capture Lag)**
  * *Why it matters:* The exact time delta between the source transaction committing in SQL Server and the resulting event landing in the Kafka topic.
  * *How to track:* Embed a precise timestamp (e.g., `SYSUTCDATETIME()`) as `created_at` in the source SQL record. On the consumer side, calculate the difference between the Kafka message consumption time (or Kafka append time) and the `created_at` timestamp.

* **Maximum Sustainable Ingestion Throughput (RPS)**
  * *Why it matters:* The peak records-per-second the ingestion mechanism can handle before the Capture Lag begins to grow infinitely.
  * *How to track:* Gradually increase the load generator RPS. Plot RPS against the "Commit-to-Kafka Latency." The maximum sustainable RPS is the highest load level before the latency graph trends upwards unboundedly over a 5-minute window.

* **Polling Query Degradation**
  * *Why it matters:* For polling-based approaches (like Outbox without CDC), how does the performance of the polling query degrade as the Outbox table grows larger between cleanup cycles?
  * *How to track:* Log the execution time of the Outbox polling query using `sys.dm_exec_query_stats` or application-side query timing, and correlate it with the total row count of the Outbox table over time.

## 3. Capture Mechanism Stability
Evaluating the infrastructure that actually moves the data.

* **Replication / CDC Reader Lag**
  * *Why it matters:* How far behind the database transaction log is the Debezium connector or CDC capture job?
  * *How to track:* For Debezium/Kafka Connect, monitor JMX metrics or the REST API for `MilliSecondsBehindSource`. For SQL Server native CDC, query the `sys.dm_cdc_log_scan_sessions` DMV.

* **Connection Pool Utilization**
  * *Why it matters:* How many database connections are permanently held open by the ingestion workers or connectors, and does this risk pool exhaustion?
  * *How to track:* Query `sys.dm_exec_sessions` grouped by `program_name` to count active connections used by the ingestion components.

* **Schema Evolution Resilience**
  * *Why it matters:* How does the ingestion pipeline behave when a source table schema changes (e.g., a column is added)?
  * *How to track:* Perform a simulated schema migration (`ALTER TABLE ADD COLUMN`) during a load test. Observe the ingestion worker logs to see if it crashes, pauses, drops the column, or seamlessly propagates the change to the Kafka schema registry.

## 4. Ingestion Consistency and Delivery Guarantees
Ensuring data correctness as it crosses the boundary from the database to Kafka.

* **Message Ordering Guarantees**
  * *Why it matters:* Does the ingestion strategy guarantee that events are published to Kafka in the exact order they were committed?
  * *How to track:* Insert sequentially numbered records (e.g., using an `IDENTITY` column) into the source database. Write a test consumer that reads from the Kafka topic partition and asserts that the sequence numbers are strictly monotonically increasing with no inversions.

* **Delivery Semantics (At-Least-Once vs. Exactly-Once)**
  * *Why it matters:* During a crash of the ingestion worker or connector, does the system safely resume without dropping records? How many duplicates are emitted?
  * *How to track:* Induce a hard failure (e.g., `docker kill` the Debezium container or Outbox worker) during a load test. Once recovered, run a script to aggregate the occurrences of each unique record ID in the Kafka topic. A count greater than 1 indicates At-Least-Once delivery (duplicates present).
