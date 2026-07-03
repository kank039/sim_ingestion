# Metrics Calculations & Formulation

This document details how each metric displayed on the dashboard is formulated, calculated, and fetched from the underlying systems (SQL Server, Kafka, and the Simulator App).

## 1. Application & Simulator Metrics

### Actual RPS (Requests Per Second)
- **Calculation:** `Math.max(0, current_recordsModified - previous_recordsModified)`
- **Location:** Frontend (`useSimulationStats.ts`)
- **Description:** The delta of `Records Modified` between the current 1-second polling interval and the previous one.

### DB Latency (Avg, p95, p99)
- **Calculation:** Measures the `performance.now()` delta specifically for the execution of the SQL operations (e.g., `INSERT/UPDATE/DELETE`).
- **Location:** Backend (`simulation.ts`)
- **Description:** Represents the raw execution time of queries against SQL Server. The backend collects all latencies within a 1-second batch, sorts them, and extracts the 50th, 95th, and 99th percentiles, as well as the average.

### Queue Latency
- **Calculation:** `waitTime = performance.now() - startWait` (measured before entering the DB execution phase).
- **Location:** Backend (`simulation.ts`)
- **Description:** The time a request spends waiting in the Node.js concurrency queue (the limiter) before a slot is available to execute the DB query.

### Records Modified
- **Calculation:** Accumulated count of `limit()` operations that successfully completed `executeOperation()` *before* the `timeoutMs` threshold was reached.
- **Location:** Backend (`simulation.ts` -> `index.ts`)

### Late Records
- **Calculation:** Accumulated count of operations that timed out (either by waiting in the queue longer than `timeoutMs` or by the DB taking longer than `timeoutMs` to respond).
- **Location:** Backend (`simulation.ts` -> `index.ts`)

### Failed Records
- **Calculation:** Accumulated count of operations that threw a hard error (e.g., deadlock, connection drop), explicitly excluding `TIMEOUT` errors.
- **Location:** Backend (`simulation.ts` -> `index.ts`)

### SLA %
- **Calculation:** `(Records Modified / Total Records) * 100` 
  *(where `Total Records = Records Modified + Failed Records + Late Records`)*
- **Location:** Frontend (`useSimulationStats.ts` / `MetricsGrid.tsx`)
- **Description:** The percentage of records that successfully completed within the strict timeout SLA.

### Success %
- **Calculation:** `((Records Modified + Late Records) / Total Records) * 100`
- **Location:** Frontend (`useSimulationStats.ts` / `MetricsGrid.tsx`)
- **Description:** The percentage of records that successfully reached the DB (even if they were late), meaning they didn't throw a hard failure.

---

## 2. SQL Server Metrics

### SQL Server CPU (1m avg)
- **Calculation:** 
  ```sql
  SELECT TOP 1 record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
  FROM sys.dm_os_ring_buffers 
  WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
  ```
- **Location:** Backend (`db.ts`)
- **Description:** Not an instantaneous snapshot, but a natively smoothed 1-minute average produced by SQL Server's `SystemHealth` ring buffer.

### SQL Server I/O (MB/s)
- **Calculation:** 
  ```sql
  SELECT SUM(num_of_bytes_read + num_of_bytes_written) / 1024 / 1024 as TotalIOMB
  FROM sys.dm_io_virtual_file_stats(DB_ID('sim_db'), NULL);
  ```
- **Location:** Backend (`db.ts`)
- **Description:** Takes the cumulative I/O bytes read/written, converts to MB, and compares the delta between the current and previous polling interval divided by the elapsed seconds to yield `MB/s`.

### DB Wait Tasks
- **Calculation:** `SELECT count(*) FROM sys.dm_os_waiting_tasks WHERE session_id > 50`
- **Location:** Backend (`db.ts`)
- **Description:** The instantaneous number of active user sessions (ID > 50) currently suspended and waiting on a resource (like a lock or I/O).

### Active DB Locks
- **Calculation:** `SELECT count(*) FROM sys.dm_tran_locks WHERE request_session_id > 50 AND request_mode IN ('X', 'U', 'IX')`
- **Location:** Backend (`db.ts`)
- **Description:** The instantaneous count of active Exclusive, Update, and Intent-Exclusive locks held by user sessions.

---

## 3. Pipeline & Kafka Metrics

### Records in Kafka
- **Calculation:** `Sum(Partition High Offsets) - baselineKafkaOffset`
- **Location:** Backend (`index.ts`)
- **Description:** The simulator queries the Kafka Admin Client (`admin.fetchTopicOffsets`) for the target topic. It calculates the sum of all partition `high` offsets and subtracts the baseline offset recorded when the simulation started. 

### Pipeline Lag
- **Calculation:** `Math.max(0, (Records Modified + Late Records) - Records in Kafka)`
- **Location:** Backend (`index.ts`)
- **Description:** The difference between the total expected records (anything successfully sent to the DB, on time or late) and the actual records that have been emitted by Debezium into Kafka.

---

## 4. Consumer Enrichment Metrics (Approach 5)

### Consumer E2E (End-to-End) Latency
- **Calculation:** `Current Timestamp - Kafka Message Timestamp`
- **Location:** Backend (`consumer-worker.ts`)
- **Description:** The total time elapsed from when Debezium published the event to Kafka to when the Node.js consumer finished executing the read-side enrichment.

### Consumer Enrichment Latency
- **Calculation:** The raw execution time of the multi-table JOIN executed via `mssql` in the consumer worker.
- **Location:** Backend (`consumer-worker.ts`)
- **Description:** Measures the cost of running read-side JOINs to enrich the raw CDC payload.
