# Metrics Calculations & Formulation

How every number on the dashboard (and in the CSV exports) is produced. All of it is computed in the
simulator backend; the frontend only formats it (shared math lives in `frontend/src/metrics.ts`).

## 0. Run lifecycle (why the numbers are comparable)

Every run goes `idle → provisioning → running ⇄ paused → draining → idle`.

**Provisioning** makes the environment match exactly one approach before any measurement starts:

| | A1 Triggers | A2 Outbox | A3 Flink | A4 SMT | A5 Consumers |
|---|---|---|---|---|---|
| `trg_billing_record_cdc` | enabled | disabled | disabled | disabled | disabled |
| CDC enabled on | `cdc_events_shadow` | `outbox_events` | `billing_record`, `enrichment_requests` | `billing_record` | `billing_record` |
| Debezium connector | fresh, those tables only | same | same | same + `InvoiceLookup` SMT | same |
| Flink job | none | none | `sim-approach3-join` | none | none |
| Consumer threads | none | none | none | none | N subscribers |
| Tracked topic | `…cdc_events_shadow` | `…outbox_events` | `sim.approach3.enriched` | `…billing_record` | `…billing_record` |

Anything else (connectors from other runs or older versions, Flink jobs, consumer threads, CDC on other
tables, the trigger) is removed first. A new connector is created per run with `snapshot.mode=no_data`, so
it never replays old rows. The write tables are truncated (unless *Reset write tables* is off) and
reference data is seeded for the chosen cardinality. Finally a **probe** write is sent through the real
path and the run only starts once it shows up on the tracked topic: the pipeline is proven live and any
backlog from earlier activity has been flushed.

**Draining** (after Stop): load stops, in-flight writes finish, and the simulator waits until every committed
change has been delivered (and, for A5, consumed) or until no progress is made for 15s. The run summary is
taken at that point, then everything is torn down, leaving no trigger, CDC, connector, job or thread running.

## 1. Load generator

### Target / Issued / Actual RPS
- **Target** (`rps`): the configured rate (ramped by `rampStepRps` per second when gradual; paused while paused).
- **Pacing:** each of the 8 writer threads gets `target / 8` (fractional rates are fine) and an open-loop pacer
  issues `floor(elapsed × rate)` operations in total, so 10 RPS really is 10 RPS.
- **Issued RPS** (`issuedRps`) and **Actual RPS** (`actualRps`, operations committed per second) are
  computed from cumulative counters over a 5-second rolling window.

### Operation outcomes
Each issued operation ends in exactly one bucket:
- **Committed on time** (`recordsModified`): committed, queue wait + DB time ≤ SLA timeout.
- **Late** (`recordsLate`): committed, but slower than the SLA timeout. Late operations are *not* abandoned;
  they are real commits and produce change events.
- **Failed** (`recordsFailed`): the DB call errored. For A3 this includes a dual write whose second insert
  failed after the billing change committed (also counted in `partialWrites`).
- **Dropped** (`recordsDropped`): shed by the generator without touching the DB (writer queue full, or
  waited in the queue longer than the SLA timeout).

`recordsCommitted = recordsModified + recordsLate`.

- **SLA %** = `recordsModified / (modified + late + failed + dropped)`
- **Success %** = `(modified + late) / (modified + late + failed + dropped)`

### Write path
One round trip per operation (two for A3's dual write). Update/delete pick the first row of the batch; if
none exists the operation inserts instead, so **every committed operation changes exactly one row** and
produces exactly one change event on the tracked topic. `opsApplied` reports what actually happened
(`c`/`u`/`d`). The write path avoids table variables and temp tables, which would contend on tempdb.

### DB latency (avg, p50, p95, p99) and queue latency
- Each writer thread records DB execution time and queue wait into log-bucketed histograms (~1% precision).
- Histograms from all threads are **merged** before percentiles are taken, so p95/p99 are true percentiles
  of all operations (the old version averaged per-thread percentiles, which is not a percentile).
- Dashboard values are per 1-second window; the run summary uses the whole run.

## 2. Pipeline metrics

### Events delivered (`recordsInKafka`)
A tracker thread consumes the run's tracked topic starting from the exact partition offsets captured at run
start (its consumer group is pre-committed at those offsets) and counts change events, excluding tombstones.

### Pipeline lag (`lag`)
`recordsCommitted − recordsInKafka`: committed changes not yet on the approach's output topic. Because the
count is per run, per topic and exact, the final lag after draining should be 0; anything else is lost data.

### Commit → topic latency (`pipelineLatency`)
For every delivered event: `now − source.ts_ms` (the source transaction's commit time from Debezium; for A3
the commit time carried through the Flink join). Shown as avg/p95 over a 5-second rolling window because
SQL Server CDC delivers in capture-job bursts. `captureLagMs` is kept as an alias of the average.

> SQL Server's CDC capture job polls the log every 5s by default, which dominates this latency for every
> approach equally. Lower it with `sys.sp_cdc_change_job @job_type='capture', @pollinginterval=1` if needed.

### Alerts (`flawAlert`)
Only raised for conditions that were actually observed:
- **PIPELINE BACKLOG**: lag exceeds `max(10 × target RPS, 5000)`.
- **ENRICHMENT MISS** (A4): the SMT's JDBC lookup returned no invoice for an event.
- **DUAL WRITE INCONSISTENCY** (A3): a billing change committed without its enrichment request.

## 3. Consumer enrichment metrics (Approach 5)
- **Enrichment latency:** time of the multi-table JOIN, keyed by the billing id and batch id from the event
  (`before` for deletes; tombstones are skipped, never replaced with a random id).
- **E2E latency:** `now − source.ts_ms` after enrichment completes (commit → enriched).
- **Consumer lag:** for every subscriber and partition, `topic end offset − next offset to process`, summed.
- Each subscriber is its own consumer group, so every subscriber processes every event.

## 4. SQL Server metrics
All DMV reads run as one batch per second in a background loop that never overlaps itself:
CPU (`dm_os_ring_buffers`, ~1-minute smoothed), I/O MB/s (delta of `dm_io_virtual_file_stats`), waiting tasks,
X/U/IX locks, log space used, page life expectancy, tempdb space **in use** (unallocated space is excluded),
and active connections.

## 5. Container metrics
Docker Engine API (`/containers/{id}/stats?one-shot=true`), CPU computed from consecutive samples and split
into per-core bars using each container's real CPU limit (`HostConfig.NanoCpus`). Memory excludes page cache.

## 6. Verification (`POST /api/simulate/verify`)
Re-reads the run's events on the tracked topic (start offsets → current end) and reports:
- **missing**: committed changes with no event;
- **duplicates**: the same source change (commit LSN, change LSN, serial no; or change token + op for A3)
  delivered more than once;
- **key-order violations**: within one message key, a source log position going backwards;
- **entities spread across partitions**: entities (billing record / aggregate) whose events landed on more
  than one partition, meaning a consumer cannot rely on per-entity ordering.
