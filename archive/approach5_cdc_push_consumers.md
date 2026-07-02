# Approach 5: CDC Push → Kafka Consumer Workers with Multi-Table JOIN Enrichment

> **Status:** Planned  
> **Author:** Auto-generated  
> **Date:** 2026-07-01  

---

## 1. Overview

Approach 5 introduces a consumer-side enrichment pattern that is fundamentally different from Approaches 1–4. While the existing approaches focus on **how to get data into Kafka** (triggers, outbox, Flink joins, SMT interceptors), Approach 5 focuses on **what happens after data lands in Kafka** — specifically, how multiple subscriber/consumer workers handle enrichment under concurrent load.

### The Pattern

```
SQL Server (billing_record) → Debezium CDC → Kafka → N Consumer Workers → Multi-Table SELECTs
```

1. **Debezium** captures `billing_record` changes in real-time via CDC and pushes them to Kafka instantaneously
2. The user configures **N subscriber workers** (each representing a subscriber/consumer)
3. Each worker runs in its own **Kafka consumer group** (fan-out: every worker processes every message)
4. On each message, the worker executes a **non-blocking, non-locking SELECT** that JOINs across 4 tables to enrich the billing event
5. All JOINs use `WITH (NOLOCK)` to ensure zero interference with writers

### Why This Matters

In real-world billing/telecom systems, CDC events from the billing table are consumed by many downstream systems simultaneously — rating engines, fraud detection, usage analytics, customer portals, etc. Each system needs different enrichment data, and they all read from the same source tables concurrently. The worst case is when every consumer JOINs every available table.

---

## 2. New Database Tables

Three new tables simulate a realistic billing domain:

### subscriber_plan
Represents different billing plans/tiers (e.g., Prepaid Gold, Postpaid Enterprise).

| Column | Type | Description |
|--------|------|-------------|
| id | INT PK | Plan identifier |
| plan_name | VARCHAR(100) | Human-readable plan name |
| plan_type | VARCHAR(50) | PREPAID / POSTPAID / HYBRID |
| base_rate | DECIMAL(10,2) | Monthly base charge |
| discount_pct | DECIMAL(5,2) | Volume discount percentage |
| effective_from | DATETIME2 | Plan validity start |
| effective_to | DATETIME2 | Plan validity end (NULL = active) |

### subscriber_usage
Tracks consumption/usage metrics per subscriber (batch).

| Column | Type | Description |
|--------|------|-------------|
| id | INT IDENTITY PK | Auto-increment ID |
| batch_id | INT | FK to invoice_batch |
| usage_type | VARCHAR(50) | DATA / VOICE / SMS / ROAMING |
| quantity | DECIMAL(15,4) | Amount consumed |
| unit | VARCHAR(20) | MB / MIN / COUNT |
| recorded_at | DATETIME2 | When usage was recorded |

### rate_schedule
Rate cards for computing final billing amounts.

| Column | Type | Description |
|--------|------|-------------|
| id | INT PK | Rate schedule identifier |
| plan_id | INT | FK to subscriber_plan |
| usage_type | VARCHAR(50) | Matches subscriber_usage.usage_type |
| rate_per_unit | DECIMAL(10,4) | Cost per unit |
| min_charge | DECIMAL(10,2) | Minimum charge per line item |
| max_charge | DECIMAL(10,2) | Maximum charge cap (NULL = uncapped) |
| valid_from | DATETIME2 | Rate validity start |
| valid_to | DATETIME2 | Rate validity end |

### Modified: invoice_batch
Add a `plan_id` column (FK to `subscriber_plan`) to enable the JOIN chain:

```
billing_record → invoice_batch → subscriber_plan → rate_schedule
                                                  ↗
                 subscriber_usage ───────────────┘
```

---

## 3. The Worst-Case Enrichment Query

Every consumer worker executes this query for each Kafka message:

```sql
SELECT
    br.id            AS billing_id,
    br.batch_id,
    br.amount,
    br.created_at,
    -- From invoice_batch (1st JOIN)
    ib.invoice_number,
    ib.status        AS invoice_status,
    -- From subscriber_plan (2nd JOIN)
    sp.plan_name,
    sp.plan_type,
    sp.base_rate,
    sp.discount_pct,
    -- From subscriber_usage (3rd JOIN)
    su.usage_type,
    su.quantity,
    su.unit,
    -- From rate_schedule (4th JOIN)
    rs.rate_per_unit,
    rs.min_charge,
    rs.max_charge
FROM billing_record br WITH (NOLOCK)
INNER JOIN invoice_batch ib WITH (NOLOCK)
    ON br.batch_id = ib.id
INNER JOIN subscriber_plan sp WITH (NOLOCK)
    ON ib.plan_id = sp.id
LEFT JOIN subscriber_usage su WITH (NOLOCK)
    ON br.batch_id = su.batch_id
LEFT JOIN rate_schedule rs WITH (NOLOCK)
    ON sp.id = rs.plan_id
    AND su.usage_type = rs.usage_type
WHERE br.batch_id = @batchId;
```

### Why This Is Worst-Case

- **4 JOINs** across 5 tables (including the source `billing_record`)
- **LEFT JOINs** on `subscriber_usage` and `rate_schedule` mean the query plan must scan even when no matching rows exist
- `WITH (NOLOCK)` on every table ensures zero lock acquisition but means the query optimizer cannot use certain optimizations
- Every single consumer worker runs this same heavy query for every single message — no filtering, no caching, no shortcuts

### Why Non-Blocking / Non-Locking

The key insight is that **read operations should never block write operations**. In a well-designed system:
- Writers (simulation workers) INSERT/UPDATE/DELETE into `billing_record` at high RPS
- Readers (consumer workers) SELECT with `NOLOCK` hints, accepting potential dirty reads
- The two workloads are completely independent — measuring how they coexist is the point of this approach

---

## 4. Consumer Worker Architecture

### Threading Model

```
Main Thread (index.ts)
  │
  ├── 4 × Simulation Worker (simulation.ts)
  │     └── Write to billing_record (same as Approach 1)
  │
  └── N × Consumer Worker (consumer-worker.ts)     ← NEW
        ├── Kafka Consumer (own consumer group)
        ├── SQL Server Connection Pool (2-5 conns)
        └── Enrichment Query Executor
```

### Worker Lifecycle

1. **Spawn**: Main thread creates `N` worker threads when simulation starts
2. **Initialize**: Each worker connects to Kafka + SQL Server
3. **Consume**: Workers poll Kafka for new messages in a loop
4. **Enrich**: For each message, execute the multi-table JOIN query
5. **Report**: Every 1 second, send latency stats to main thread
6. **Shutdown**: On stop signal, disconnect consumers, close pools, exit

### Fan-Out vs Competing Consumers

Each worker uses its own consumer group ID (`subscriber-worker-{id}`), meaning **every worker receives every message**. This simulates the worst case where all subscribers process all events.

For a more realistic simulation, workers could share a single consumer group, but that would reduce per-worker load and not stress the read path as much.

---

## 5. Metrics Collected

### Per-Worker Metrics (reported every 1s)
| Metric | Description |
|--------|-------------|
| `enrichmentLatency` | Time to execute the 4-JOIN SELECT query (ms) |
| `e2eLatency` | Time from CDC capture timestamp to enrichment completion (ms) |
| `messagesConsumed` | Number of Kafka messages processed |
| `enrichmentsFailed` | Number of failed SELECT queries |

### Aggregated Metrics (displayed on dashboard)
| Metric | Description |
|--------|-------------|
| `numSubscribers` | Number of active consumer workers |
| `totalMessagesConsumed` | Sum across all workers |
| `avgEnrichmentLatency` | Average across all workers |
| `p95EnrichmentLatency` | 95th percentile enrichment time |
| `p99EnrichmentLatency` | 99th percentile enrichment time |
| `avgE2eLatency` | Average end-to-end latency |
| `consumerLag` | Kafka consumer group lag (how far behind) |
| `enrichmentsFailed` | Total failures across all workers |

---

## 6. Expected Behavior Under Load

### Low Load (100 RPS, 5 subscribers)
- Enrichment latency: ~1-5ms (queries are simple with NOLOCK)
- E2E latency: ~50-200ms (dominated by Debezium polling interval)
- Consumer lag: ~0 (consumers keep up easily)
- DB CPU: Low impact from reads

### High Load (2000+ RPS, 20+ subscribers)
- Enrichment latency: Increases as connection pool pressure grows
- E2E latency: Increases as Kafka throughput saturates
- Consumer lag: May start accumulating (consumers fall behind)
- DB CPU: Noticeable increase from concurrent reads
- **Key observation**: `active_locks` should stay LOW (proving non-blocking reads work), while `wait_tasks` may increase due to I/O contention

### Extreme Load (5000 RPS, 50 subscribers)
- 50 workers × 5000 messages/sec = 250,000 SELECT queries/sec
- SQL Server connection pool exhaustion likely
- Consumer lag will grow rapidly
- This is the stress test that reveals the true limits of the read-side architecture

---

## 7. Comparison with Other Approaches

| Aspect | Approach 1-4 | Approach 5 |
|--------|--------------|------------|
| Focus | Write-side: how to get data into Kafka | Read-side: how consumers handle events |
| Enrichment | Done during/before Kafka delivery | Done after Kafka delivery, by consumers |
| Lock behavior | Varies (triggers lock, outbox uses TX) | Zero locks on reads (NOLOCK) |
| Scalability knob | RPS | RPS × Number of Subscribers |
| New metric | Pipeline lag | Enrichment latency + E2E latency |
| Tables involved | 2-3 | 5 (billing_record + 4 JOINed tables) |
| DB read pressure | Minimal | High (proportional to subscribers × RPS) |

---

## 8. Configuration

### Frontend Controls (Approach 5 only)
- **Number of Subscribers**: 1–100 (default: 5)
  - Each subscriber = 1 Kafka consumer worker thread
  - More subscribers = more concurrent enrichment queries

### Backend Configuration
- Consumer worker connection pool: 2-5 connections per worker
- Kafka consumer poll interval: 100ms
- Consumer group prefix: `subscriber-worker-`

---

## 9. References

- [Debezium SQL Server Connector](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
- [SQL Server NOLOCK Hint](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table)
- [KafkaJS Consumer API](https://kafka.js.org/docs/consuming)
- Existing approaches: [init.sql](file:///d:/tmp/sim_dataIngestion/scripts/init.sql), [simulation.ts](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts)
