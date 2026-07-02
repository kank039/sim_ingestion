# Approach 3: Stream-to-Stream Join (Flink)

```mermaid
flowchart TD
    App["Simulator App"] -->|"1a. Write Domain"| DB[("billing_record")]
    App -->|"1b. Write Enrichment Req"| DB2[("enrichment_requests")]
    
    CDC1["Debezium CDC"] -->|"2a. Capture billing"| DB
    CDC2["Debezium CDC"] -->|"2b. Capture enrichment"| DB2
    
    CDC1 -->|"3a. Publish"| Topic1[["Kafka: billing_events"]]
    CDC2 -->|"3b. Publish"| Topic2[["Kafka: enrichment_events"]]
    
    Topic1 --> Flink["Flink JobManager/TaskManager"]
    Topic2 --> Flink
    
    Flink -->|"4. Stateful Stream Join"| FlinkState[("RocksDB State")]
    Flink -->|"5. Output Enriched Event"| OutTopic[["Kafka: enriched_output"]]
```

**Description:**
Dual writes populate domain and enrichment tables separately. Changes are streamed via CDC into Kafka topics. Apache Flink performs a stateful stream-to-stream join to enrich the events in real-time. This can face issues with state TTL and late-arriving data.

### Pros:
- **Highly Scalable**: Flink provides a highly robust, distributed state backend capable of processing massive volumes of events.
- **Complete Decoupling**: The database is completely offloaded from any enrichment responsibility; the streaming layer handles all complex event processing.

### Cons:
- **Operational Complexity**: Managing a Flink cluster (JobManagers, TaskManagers, RocksDB state, checkpointing) adds substantial infrastructure complexity.
- **Late-Arriving Data**: Stateful joins are susceptible to dropping events if data arrives out-of-order or after the State Time-To-Live (TTL) has expired.
