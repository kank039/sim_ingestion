# Potential Optimizations by Approach

Based on the performance limits identified during our ramp-up simulations, this document outlines the theoretical and practical optimizations that could be applied to further scale each data ingestion approach.

---

## Approach 1: Database Triggers (DBA Outbox)
**Current Bottleneck:** Synchronous execution of triggers blocking the primary transaction and crushing SQL Server CPU.

### Optimizations
1. **SQL Server Service Broker:** Instead of inserting directly into a physical outbox table using a trigger, the trigger can asynchronously drop a message into a SQL Server Service Broker queue. This removes the heavy synchronous I/O from the transaction path.
2. **Memory-Optimized Tables (In-Memory OLTP):** If the outbox table must be used, configuring the `outbox_events` table as a memory-optimized, non-durable (or durable) table can significantly reduce the latching and locking overhead associated with traditional disk-based tables.
3. **Vertical Scaling (Compute):** Because this approach is fundamentally bound by the database engine's CPU, vertically scaling the database instance (adding more cores) provides linear, albeit expensive, scaling.

---

## Approach 2: Transactional Outbox
**Current Bottleneck:** Database connection pool saturation and lock contention at ~2,400 Target RPS.

### Optimizations
1. **Database Sharding / Partitioning:** Distribute the `billing_record` and `outbox_events` tables across multiple physical database instances (Sharding) or filegroups based on `batch_id` or `tenant_id`. This distributes lock contention across multiple nodes.
2. **In-Memory OLTP:** Similar to Approach 1, making the `outbox_events` table a Memory-Optimized table in SQL Server eliminates traditional page-latch contention, allowing much higher concurrent insert rates.
3. **Connection Multiplexing (PgBouncer/Proxy):** Use a robust connection pooler between the application and the database to better handle connection spikes and queueing gracefully without instantly exhausting the application's local connection pool.
4. **Table Cleanup Optimization:** Outbox tables grow rapidly. Implementing efficient, lock-free retention policies (like table partitioning by day and dropping partitions, rather than `DELETE` queries) is required to maintain long-term throughput.

---

## Approach 3: Stream-to-Stream Join (Flink)
**Current Bottleneck:** JVM memory pressure, GC pauses, and state eviction due to the massive in-memory state required for the join window.

### Optimizations
1. **RocksDB State Backend:** Switch Flink from a purely Heap-based state backend to the RocksDB state backend. This allows Flink's state to spill to disk, preventing Out-Of-Memory (OOM) errors and GC pauses under heavy load, trading a slight latency increase for massive stability.
2. **Horizontal TaskManager Scaling:** Unlike a monolithic database, Flink scales horizontally very easily. Adding more TaskManager nodes and increasing Kafka partitions will seamlessly distribute the memory pressure.
3. **Tighter Watermarks & Join Windows:** If the business domain allows, aggressively reducing the allowable "join window" (e.g., from 5 minutes to 30 seconds) forces Flink to flush state earlier, drastically reducing the memory footprint.

---

## Approach 4: JDBC SMT (Interceptor)
**Current Bottleneck:** Synchronous network N+1 queries overwhelming Debezium and stalling the CDC pipeline.

### Optimizations
1. **Distributed Caching (Redis):** Modify or wrap the SMT to query a fast, distributed in-memory cache (like Redis) rather than hitting the primary SQL Server over JDBC for every event.
2. **Batching / Asynchronous Lookups:** Standard Kafka Connect SMTs execute synchronously. Writing a custom, asynchronous Kafka Connect Transform that batches lookups could relieve the N+1 pressure.
3. **Architectural Pivot:** Fundamentally, this architecture is an anti-pattern for high throughput. The true optimization is to abandon it in favor of Approach 5 (Consumer Enrichment) or Approach 3 (Stream Join).

---

## Approach 5: CDC Push + Consumer Enrichment
**Current Bottleneck:** Consumer worker CPU saturation and heavy concurrent read pressure on the database.

### Optimizations
1. **Consumer Horizontal Scaling:** This approach's greatest strength is its scalability. We can endlessly scale the Node.js consumers horizontally (adding more pods/servers) as long as we increase the Kafka topic partition count to match.
2. **Micro-Batching Queries:** Instead of the consumers executing a `SELECT` query for *every* Kafka message, the consumers can pull a batch of 500 messages, extract the unique IDs, and execute a single `SELECT ... WHERE id IN (...)` query. This slashes the database connection and execution overhead.
3. **Read Replicas:** Point the consumer's enrichment `SELECT` queries to a SQL Server Read Replica (Always On Availability Group). This completely isolates the heavy read load from the primary write database.
4. **Enrichment Data Caching:** Data like `subscriber_plan` rarely changes. Consumers should load this data into a local memory cache or Redis, eliminating 90% of the database round-trips entirely.
