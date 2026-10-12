# Approach 4: JDBC SMT (Interceptor)

```mermaid
flowchart TD
    App["Simulator App"] -->|"1a. Write Domain"| DB[("billing_record")]
    CDC["Debezium CDC"] -->|"2. Capture Change"| DB
    
    subgraph KafkaConnect ["Kafka Connect"]
        CDC
        SMT["JDBC SMT Interceptor"]
    end
    
    CDC -->|"3. Route Event to SMT"| SMT
    SMT -->|"4. Synchronous JDBC Lookup"| DB2[("invoice_batch etc")]
    DB2 -->|"5. Return Lookup Data"| SMT
    SMT -->|"6. Publish Enriched Event"| Kafka[["Kafka Topic"]]
```

**Description:**
Kafka Connect intercepts the CDC event using a Single Message Transform (SMT) and performs a synchronous JDBC query back to the database to enrich the event payload before it is published to Kafka. This can introduce latency and race conditions if lookup data mutates quickly.

**In the simulator:** the custom SMT `sim.smt.InvoiceLookup` (`debezium/smt`, built into the `sim-debezium`
image) runs inside the Debezium connector task. For every `billing_record` event it queries
`invoice_batch` over JDBC and adds an `enrichment` struct (`invoice_number`, `invoice_status`,
`lookup_ts_ms`, `lookup_us`) to the event. The application write path is untouched. The dashboard shows the
SMT lookup time and the number of events whose lookup found nothing (*ENRICHMENT MISS* alert).

### Pros:
- **Simplified Stack**: Requires no external streaming processing applications like Flink. Everything is configured and centralized inside Kafka Connect.
- **Always Up-to-Date**: Ensures the event gets the most current state of the joined data exactly at the moment it passes through the connector.

### Cons:
- **Race Conditions**: Highly susceptible to race conditions. If the database updates rapidly before the SMT queries it, the event might be enriched with future/incorrect state.
- **Bottleneck Risk**: Introduces synchronous database lookups into the critical CDC streaming path, potentially causing massive bottlenecks during high throughput.
