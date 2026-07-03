# Data Ingestion Approaches: Resource Limits and Key Metrics

This document analyzes the five data ingestion approaches in the simulator, outlining the theoretical hard limits based on the machine's physical capabilities and the `docker-compose.yml` resource allocations.

## System Capabilities vs. Container Allocations

**Physical Host Machine:**
- **CPU:** 8 Cores / 16 Logical Processors (Intel Core i7-11800H)
- **Memory:** ~34 GB Total Physical Memory

**Docker Compose Resource Limits:**
- **`sqlserver`:** 4.0 CPUs, 4096M Memory
- **`debezium`:** 1.5 CPUs, 4096M Memory
- **`kafka`:** 2.0 CPUs, 2048M Memory
- **`flink-jobmanager`:** 1.0 CPUs, 1024M Memory
- **`flink-taskmanager`:** 1.5 CPUs, 1536M Memory
- **`simulator`:** 2.0 CPUs, 2048M Memory

**Total Requested:** 12.0 CPUs, ~14.5 GB Memory. 
The host machine comfortably supports the full stack. However, the hard limits for each approach are determined by the individual container caps.

---

## 1. Database Triggers (DBA Outbox)

**Mechanism:** SQL Server executes a synchronous trigger for each DML operation, computing state and inserting a row into an outbox table.
- **Primary Bottleneck:** SQL Server CPU (4.0 Cores) and synchronous transaction overhead.
- **Theoretical Hard Limit:** Because triggers execute synchronously as part of the transaction, they add significant overhead to every insert/update/delete. The 4.0 CPUs on SQL Server will bottleneck quickly under high concurrency. Realistically, performance will degrade severely around **1,000 - 2,500 RPS**, causing application-side queuing and latency spikes.
- **Metrics to Watch:**
  - **SQL Server CPU Utilization:** Will spike significantly and hit the 400% (4 core) limit.
  - **App Latency / DB Latency:** Will grow exponentially as transactions queue up waiting for trigger execution.
  - **Capture Lag (Kafka Lag):** If Debezium can't poll fast enough or if the DB locks slow down the outbox table.

## 2. Transactional Outbox (Recommended)

**Mechanism:** The application explicitly writes the mutation and the outbox event in the same database transaction.
- **Primary Bottleneck:** Debezium CPU (1.5 Cores) and SQL Server Write IOPS.
- **Theoretical Hard Limit:** Much more efficient than triggers. SQL Server handles explicit transactions much better. The bottleneck moves to Debezium parsing the Write-Ahead Log (WAL) and pushing to Kafka. With 1.5 CPUs, Debezium typically maxes out at **5,000 - 10,000 events/sec** depending on the payload size and serialization overhead.
- **Metrics to Watch:**
  - **Debezium CPU Utilization:** Will approach the 1.5 core limit under high load.
  - **Kafka Lag (`recordsInKafka` vs `recordsModified`):** Shows if Debezium CDC is falling behind the raw database insertions.
  - **Capture Lag Ms:** Measures how long it takes an event to reach Kafka after being committed to the database.

## 3. Stream-to-Stream Join (Flink)

**Mechanism:** Raw CDC streams for multiple tables are pushed to Kafka, and Apache Flink joins these streams in-memory to build an enriched aggregate.
- **Primary Bottleneck:** Flink TaskManager Memory (1536M) and CPU (1.5 Cores).
- **Theoretical Hard Limit:** Flink must maintain state (in-memory or RocksDB) to join asynchronous streams. With only 1.5GB of RAM allocated to the TaskManager, state blowup is a massive risk. If streams arrive out of order or if the join window is large, the JVM will hit OutOfMemory (OOM) errors or aggressively garbage collect, dropping throughput to near zero. State TTL limits also mean late-arriving data will fail to join.
- **Metrics to Watch:**
  - **Flink TaskManager Memory:** Approaching 100% means imminent crash or extreme GC pauses.
  - **Late Records (`recordsLate`):** Events that missed the join window due to state expiration or skew.
  - **Flink TaskManager CPU:** Will spike if GC is thrashing due to memory pressure.

## 4. JDBC SMT (Interceptor)

**Mechanism:** As Debezium captures raw events, a Single Message Transform (SMT) intercepts each event and makes a synchronous JDBC query back to SQL Server to enrich the payload before sending it to Kafka.
- **Primary Bottleneck:** Network I/O, SQL Server Concurrent Connections, and Debezium CPU (1.5 Cores).
- **Theoretical Hard Limit:** This is an anti-pattern for high-throughput CDC. The synchronous network hop (N+1 query problem) per event limits throughput severely. Furthermore, SMTs are vulnerable to **Race Conditions**: by the time the SMT queries SQL Server, the data might have already mutated again (especially with rapid sequential updates). Throughput will likely cap at **a few hundred RPS**.
- **Metrics to Watch:**
  - **Flaw Alerts:** Look out for "RACE CONDITION DETECTED".
  - **Capture Lag Ms:** Will skyrocket as Debezium pauses CDC processing to execute synchronous database queries.
  - **SQL Server Memory & CPU:** Serving thousands of individual singleton queries from Debezium will exhaust resources quickly.

## 5. CDC Push + Consumer Enrichment (Multi-JOIN)

**Mechanism:** CDC pushes raw lightweight events to Kafka. Consumer workers (running inside the Simulator) consume these events and execute complex multi-table `JOIN` queries (with `NOLOCK`) against SQL Server to enrich the data.
- **Primary Bottleneck:** Simulator CPU (2.0 Cores) and SQL Server CPU (4.0 Cores) handling heavy concurrent reads.
- **Theoretical Hard Limit:** The Simulator is capped at 2.0 CPUs. If we spawn a large number of consumer workers (subscribers), the Node.js event loop will become saturated parsing Kafka messages and managing database connections. Simultaneously, SQL Server (4.0 CPUs) might be crushed by thousands of concurrent multi-table join queries per second. Hard limit will likely be reached around **2,000 - 4,000 RPS** depending on the complexity of the joins.
- **Metrics to Watch:**
  - **Simulator CPU & SQL Server CPU:** Both will race to their respective limits (2.0 and 4.0).
  - **Consumer E2E Latency (`consumerE2eLatency`):** Total time from DB mutation to full enrichment.
  - **Enrichment Latency (`consumerEnrichmentLatency`):** Specifically measures the time taken by the SQL Server `JOIN` queries.
  - **Enrichments Failed:** Indicates SQL Server timeouts or Simulator connection pool exhaustion.
