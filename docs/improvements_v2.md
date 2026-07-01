# Improvements v2 — Data Ingestion Simulation Platform

> This document catalogs **remaining and new improvements** identified through a full codebase audit.
> Items already implemented in the CHANGELOG are excluded. Focus is on the simulator's primary goal:
> **testing different data-ingestion approaches and saving comparable results.**
>
> Priority: 🔴 Critical · 🟠 High · 🟡 Medium · 🟢 Low
> Effort: S (< 2 hrs) · M (2–8 hrs) · L (1–3 days) · XL (> 3 days)

---

## 1. 🧪 Simulation Accuracy & Realism

### 1.1 SQL Injection via String Interpolation 🔴 S
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L127-L184)

All SQL queries use raw string interpolation:
```ts
await pool.request().query(`INSERT INTO billing_record (batch_id, amount) VALUES (${batchId}, ${amount})`);
```

While `batchId` and `amount` are locally generated numbers so there's no *security* risk, this bypasses SQL Server's query plan caching. Every query is compiled as a new ad-hoc plan, inflating `sys.dm_exec_cached_plans` and skewing CPU/memory metrics vs. what a real application using parameterized queries would show.

**Recommendation:** Switch to parameterized queries:
```ts
await pool.request()
  .input('batchId', sql.Int, batchId)
  .input('amount', sql.Decimal(10, 2), amount)
  .query(`INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount)`);
```

This gives more realistic DB performance metrics and is a trivial change.

---

### 1.2 Tiny `batch_id` Range (1–100) Creates Unrealistic Hotspots 🟠 S
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L120)

```ts
const batchId = Math.floor(Math.random() * 100) + 1;
```

With only 100 distinct `batch_id` values and 5000 RPS, updates/deletes constantly collide on the same rows, causing excessive lock contention. This doesn't reflect real workloads with millions of distinct records.

**Recommendation:** Make the `batch_id` range configurable via the control panel (e.g., 100 / 10,000 / 1,000,000). This turns row contention into a tunable simulation parameter:
```ts
const batchId = Math.floor(Math.random() * cardinality) + 1;
```

---

### 1.3 No Approach-Specific Behavior for Approaches 3 & 4 🔴 M
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L170-L184)

Approaches 1 (Triggers), 3 (Flink), and 4 (SMT) all execute **identical code** — raw inserts/updates/deletes against `billing_record`. There is no behavioral difference between them. The only differentiated approach is #2 (Transactional Outbox).

This means the simulation cannot actually produce different performance characteristics for approaches 3 vs 4 vs 1 — the results will be statistically identical.

**Recommendation:**
- **Approach 3 (Flink):** Insert into both `billing_record` and a second Kafka topic (`enrichment-requests`) to simulate the dual-write pattern that Flink must join.
- **Approach 4 (SMT):** Add a simulated `SELECT` query after every write (to simulate the JDBC lookup) and introduce artificial latency to model the SMT interceptor overhead:
  ```ts
  if (currentApproach === 4) {
      // Simulate SMT's JDBC lookup
      await pool.request().query(`SELECT invoice_number FROM invoice_batch WHERE id = ${batchId}`);
  }
  ```

---

### 1.4 Flaw Alert is Hard-Coded, Not Measured 🟠 S
**File:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L152-L155)

```ts
if (currentApproach === 4 && currentRps > 20) {
    flawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
}
```

**Recommendation:** 
This will indicate that the process is falling behind and will join the latest data from helper table if there were multiple updates as the records were still in queue waiting to be processed when updates happened therfore losing changes from last update.
```ts
if (currentApproach === 4 && pipeLineLag > 0) {
    flawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
}
```

---

### 1.5 Latency Measurement Includes Queue Wait Time 🟡 S
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L88-L101)

The `startWait` timer starts *before* entering the concurrency limiter queue. If the limiter blocks for 500ms and the actual DB operation takes 10ms, the reported latency is 510ms. This conflates queueing delay with actual DB execution time.

**Recommendation:** Track two separate metrics:
- `dbLatency`: Measured inside `executeOperation()` only (already exists at L122/L191)
- `queueLatency`: `performance.now() - startWait` minus `dbLatency`

Report both to the frontend so users can distinguish "the DB is slow" from "the app can't keep up."

---

### 1.6 Operation Mix Is Fixed (20% Delete / 30% Update / 50% Insert) 🟡 S
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L124-L184)

The operation distribution is hardcoded. Real workloads vary wildly — some are 95% inserts, others are update-heavy.

**Recommendation:** Expose the operation mix as sliders in the Control Panel:
```
Insert: ___70___% | Update: ___20___% | Delete: ___10___%
```
Pass these as part of the `start` message to workers.

---

## 2. 📁 Simulation Results Persistence & Comparison

### 2.1 No Server-Side Results Persistence 🔴 M
**Files:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts), [`run_simulation.ps1`](file:///d:/tmp/sim_dataIngestion/scripts/run_simulation.ps1)

The PowerShell script saves CSV to `./output/`, but the web dashboard has **no server-side persistence**. The frontend's CSV download is client-side only and loses data on page refresh.

**Recommendation:** Add a `/api/simulate/save-run` endpoint that:
1. Snapshots the current run's config + telemetry history into a JSON file:
   ```
   docs/results/run_<timestamp>_approach<N>_<rps>rps.json
   ```
2. Stores: approach, rps, duration, operation mix, timeout, telemetry time-series, final totals (modified, failed, lag, success rate)
3. Auto-saves when simulation stops

Schema:
```json
{
  "id": "20260701_134200",
  "approach": 2,
  "approachName": "Transactional Outbox",
  "config": {
    "rps": 1000,
    "timeoutMs": 3000,
    "insertsOnly": false,
    "gradual": false,
    "duration": 60
  },
  "summary": {
    "totalModified": 58420,
    "totalFailed": 180,
    "avgLatency": 42,
    "maxLatency": 312,
    "avgLag": 120,
    "successRate": 99.69
  },
  "telemetry": [
    { "time": "...", "appLatency": 35, "cpu": 22, "io": 4.5, ... }
  ]
}
```

---

### 2.2 No Run Comparison / History View 🟠 L
**Files:** New frontend component needed

Users cannot compare Approach 1 vs Approach 2 results side-by-side, which is the entire purpose of the simulation.

**Recommendation:**
1. Add a `/api/results` endpoint that lists all saved run files
2. Add a `/api/results/:id` endpoint to load a specific run
3. Create a `RunHistory.tsx` component with:
   - A table listing past runs (date, approach, RPS, duration, key metrics)
   - A **Compare** mode: select 2+ runs and overlay their telemetry on the same chart
   - Delete run capability

---

### 2.3 PowerShell Script Doesn't Record Approach Name or Config 🟡 S
**File:** [`run_simulation.ps1`](file:///d:/tmp/sim_dataIngestion/scripts/run_simulation.ps1#L115-L118)

The CSV filename is `simulation_stats_<timestamp>.csv` with no indication of which approach or RPS was used. The CSV also doesn't include a header row with the run config.

**Recommendation:**
- Filename: `simulation_approach${approach}_${rps}rps_${timestamp}.csv`
- Prepend a metadata comment row: `# Approach: 2, RPS: 1000, Timeout: 3000ms, Duration: 60s, InsertsOnly: false`

---

## 3. ⚡ Backend Improvements

### 3.1 `globalRecordsPushed` Used But Never Declared 🔴 S
**File:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L324)

```ts
globalRecordsPushed = 0; // ← undefined variable!
```

This is a bug — should be `globalRecordsModified`. The clean endpoint silently fails to reset the correct counter.

---

### 3.2 Records Counters Only Increment, Never Reset Between Runs 🟠 S
**File:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L96-L97)

`globalRecordsModified` and `globalRecordsFailed` are cumulative across the entire server lifetime. If you run Approach 1, stop, then run Approach 2, the counters include Approach 1's numbers.

The clean endpoint tries to reset them (L324–326) but uses the wrong variable name (`globalRecordsPushed`). Even if fixed, the counters should also reset on `/api/simulate/start`.

**Recommendation:** Reset counters at the start of each new simulation run:
```ts
app.post('/api/simulate/start', (req, res) => {
    globalRecordsModified = 0;
    globalRecordsFailed = 0;
    // ... rest of start logic
});
```

---

### 3.3 Duplicate Stats Assembly Logic 🟡 M
**File:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L136-L178) and [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L240-L282)

The exact same stats assembly code (DB stats + container stats + Kafka offsets + latency + flaw alert) is duplicated between the `/api/stats` GET handler and the SSE broadcast interval.

**Recommendation:** Extract into a shared `async function buildStatsPayload()`:
```ts
async function buildStatsPayload() {
    const [dbStats, containerStats] = await Promise.all([getDBStats(), getContainerStats()]);
    // ... rest of shared logic
    return payload;
}
```

---

### 3.4 SSE Sends Full Log Array Every Second 🟡 S
**File:** [`index.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/index.ts#L275)

Every SSE broadcast includes the entire `systemLogs` array (up to 500 entries). At 1 event/sec, this sends ~100KB+ of redundant data every second.

**Recommendation:** Only send new logs since the last broadcast:
```ts
let lastLogTimestamp = 0;
// In broadcast:
const newLogs = systemLogs.filter(l => l.timestamp > lastLogTimestamp);
lastLogTimestamp = Date.now();
// Include newLogs instead of systemLogs
```

Or use a separate SSE channel for logs.

---

## 4. 🖥️ Frontend Improvements

### 4.1 No Per-Run Telemetry Isolation 🟠 M
**File:** [`useSimulationStats.ts`](file:///d:/tmp/sim_dataIngestion/frontend/src/hooks/useSimulationStats.ts#L28-L41)

History accumulates across runs. If you stop and start again, old telemetry data bleeds into the new run's chart.

**Recommendation:** Clear `history` when a new simulation starts:
```ts
// In useSimulationStats, detect transition from not-running to running
if (data.isRunning && !prevRunning) {
    setHistory([]); // Fresh chart for new run
}
```

---

### 4.2 No Percentile Latency Metrics (p50, p95, p99) 🟠 M
**File:** [`simulation.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/simulation.ts#L42-L58)

Only average latency is tracked and reported. Averages hide tail latency — a system with avg 50ms but p99 of 2000ms is performing terribly.

**Recommendation:**
- Sort the circular buffer on each stats interval and compute p50/p95/p99
- Send as part of the stats message: `{ avgLatency, p50, p95, p99 }`
- Display in the MetricsGrid and make chartable

---

### 4.3 History Capped at 300 Points (5 minutes) 🟡 S
**File:** [`useSimulationStats.ts`](file:///d:/tmp/sim_dataIngestion/frontend/src/hooks/useSimulationStats.ts#L40)

```ts
return newHistory.slice(-300);
```

For long-duration stress tests (30+ minutes), only the last 5 minutes of data are retained in the chart.

**Recommendation:** Either:
- Increase to 3600 (1 hour) — memory impact is minimal (~1MB for 3600 points)

---

### 4.4 `dbStats.cpu` and `dbStats.io` Types Are `any` 🟡 S
**File:** [`types.ts`](file:///d:/tmp/sim_dataIngestion/frontend/src/types.ts#L32)

```ts
dbStats: any; // Or specific shape like { cpu: number, io: number }
```

This is the last remaining `any` in the type system.

**Recommendation:**
```ts
dbStats: { cpu: number; io: number };
```

---

### 4.5 No "Duration" or "Elapsed Time" Display 🟡 S
**File:** Frontend dashboard

Users can't see how long the current simulation has been running. This is essential for reproducible comparison — "Approach 1 at 1000 RPS for 60s vs. Approach 2 at 1000 RPS for 60s."

**Recommendation:** Track `startedAt` timestamp on simulation start and display an elapsed timer in the header: `⏱ Running: 01:23`.

---

## 5. 🏗️ Infrastructure & Config

### 5.1 Debezium Connector Config Not Approach-Aware 🟠 M
**File:** [`register_connectors.ps1`](file:///d:/tmp/sim_dataIngestion/register_connectors.ps1)

The same Debezium connector monitors all tables regardless of approach. For Approach 2 (Outbox), Debezium should use the Event Router SMT to read from `outbox_events`. For Approach 4, it should use the JDBC SMT for enrichment.

**Recommendation:** Create per-approach connector configs:
```
scripts/connectors/
├── approach1_triggers.json
├── approach2_outbox.json
├── approach3_flink.json
└── approach4_smt.json
```

Switch connector registration when the approach changes, or register all four with different topic prefixes.

---

### 5.2 Kafka Uses Only 1 Partition 🟡 S
**File:** [`docker-compose.yml`](file:///d:/tmp/sim_dataIngestion/docker-compose.yml#L43)

```yaml
KAFKA_NUM_PARTITIONS: 1
```

Single-partition Kafka can't demonstrate parallelism bottlenecks. At high RPS, this becomes a single-threaded bottleneck.

**Recommendation:** Increase to 3–6 partitions for a more realistic simulation.

---

### 5.3 No Flink Job Is Actually Deployed 🟢 L
**File:** [`docker-compose.yml`](file:///d:/tmp/sim_dataIngestion/docker-compose.yml#L88-L123)

The Flink JobManager and TaskManager are running but idle — there's no Flink SQL job or JAR to perform the stream-to-stream join that Approach 3 describes.

**Recommendation:** Create a Flink SQL job that:
1. Reads from `sim.sim_db.dbo.billing_record` Kafka topic
2. Joins with `sim.sim_db.dbo.invoice_batch` (as a lookup table or CDC stream)
3. Writes enriched events to an `enriched-billing` topic

Without this, Approach 3 is purely theoretical and produces no distinct metrics.

---

## 6. 📊 Observability & Analysis

### 6.1 No Throughput Metric (Actual RPS Achieved) 🟠 S
The dashboard shows *configured* RPS but not *actual achieved* RPS. If you set 5000 RPS but the system can only sustain 2000, you can't see this.

**Recommendation:** Calculate `actualRps = recordsModified_delta / timeDelta` per second and display it next to the configured RPS.

---

### 6.2 No "Summary Report" at End of Run 🟡 M
When a simulation stops, there's no summary. Users must mentally scan the chart to assess results.

**Recommendation:** Show a modal/panel on simulation stop with:
| Metric | Value |
|---|---|
| Duration | 60s |
| Total Records | 58,600 |
| Failed Records | 180 |
| Success Rate | 99.69% |
| Avg Latency | 42ms |
| p95 Latency | 180ms |
| p99 Latency | 312ms |
| Avg Pipeline Lag | 120 |
| Peak DB CPU | 88% |

With a **Save Run** button that persists to `docs/results/`.

---

### 6.3 No DB Wait Stats or Lock Contention Metrics 🟢 M
**File:** [`db.ts`](file:///d:/tmp/sim_dataIngestion/simulator/src/db.ts#L47-L96)

Only CPU and I/O are tracked from SQL Server. For comparing approaches, lock contention and wait statistics are critical — Approach 2 (Outbox) should show higher lock waits due to transactions.

**Recommendation:** Query additional DMVs:
```sql
-- Active wait stats
SELECT wait_type, waiting_tasks_count, wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type IN ('LCK_M_X', 'LCK_M_U', 'WRITELOG', 'PAGEIOLATCH_SH');

-- Active lock count
SELECT COUNT(*) as active_locks FROM sys.dm_tran_locks WHERE resource_database_id = DB_ID('sim_db');
```

---

## 7. 🧹 Code Quality & Bugs

### 7.1 `isCleaning` Prop Not Typed in ControlPanel Interface 🟡 S
**File:** [`ControlPanel.tsx`](file:///d:/tmp/sim_dataIngestion/frontend/src/components/ControlPanel.tsx#L11-L28)

`isCleaning` is used at L32 but not declared in `ControlPanelProps`:
```ts
// Missing from interface:
isCleaning: boolean;
```

TypeScript should be catching this — check `tsconfig` for strict mode.

---

### 7.2 `APPROACHES` Array Duplicated 🟢 S
**Files:** [`App.tsx`](file:///d:/tmp/sim_dataIngestion/frontend/src/App.tsx#L15-L20) and [`ControlPanel.tsx`](file:///d:/tmp/sim_dataIngestion/frontend/src/components/ControlPanel.tsx#L4-L9)

The same array exists in two files. If a new approach is added, both must be updated.

**Recommendation:** Move to a shared `constants.ts` or into `types.ts`.

---

### 7.3 `init.sql` Has a Broken CDC Check (L111) 🟡 S
**File:** [`init.sql`](file:///d:/tmp/sim_dataIngestion/scripts/init.sql#L111-L113)

```sql
IF sys.fn_cdc_has_column_changed() IS NULL -- this is just a dummy way to check, better to check sys.tables
```

This function requires parameters and will error. The comment even acknowledges it's wrong. The subsequent `GO` at L113 means this broken statement runs as its own batch.

**Recommendation:** Remove these three lines entirely — the actual CDC checks at L115+ are already correct.

---

## Priority Summary

| Priority | Items | Quick Wins |
|----------|-------|------------|
| 🔴 Critical | 1.1, 1.3, 2.1, 3.1 | 1.1, 3.1 |
| 🟠 High | 1.2, 1.4, 2.2, 3.2, 4.1, 4.2, 5.1, 6.1 | 1.2, 1.4, 3.2, 6.1 |
| 🟡 Medium | 1.5, 1.6, 2.3, 3.3, 3.4, 4.3, 4.4, 4.5, 5.2, 6.2, 6.3, 7.1, 7.3 | Many |
| 🟢 Low | 5.3, 7.2 | 3.5, 7.2 |

### Recommended Implementation Order (for simulation goals)

1. **Fix bugs first**: 3.1 (`globalRecordsPushed`), 7.3 (broken SQL)
2. **Make approaches actually different**: 1.3 (approach-specific behavior)
3. **Persist and compare results**: 2.1 (server-side save), 2.2 (comparison UI)
4. **Improve measurement accuracy**: 1.1 (parameterized queries), 4.2 (percentile latencies), 3.2 (counter reset)
5. **Tune realism**: 1.2 (batch_id cardinality), 1.6 (operation mix), 5.1 (per-approach connectors)
6. **Polish**: Everything else
