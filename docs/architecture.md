# Overall Architecture

```mermaid
flowchart TD
    subgraph Frontend ["React + Vite"]
        Dashboard["Dashboard / UI"]
    end

    subgraph Backend ["Node.js Simulator"]
        API["API Server"]
        Workers["Worker Threads (Load Generators)"]
        Consumers["Consumer Workers"]
        Telemetry["Telemetry & Stats Engine"]
    end

    subgraph Infrastructure ["Docker Compose"]
        DB[("SQL Server 2022")]
        CDC["Debezium CDC"]
        Broker[["Kafka Broker"]]
        Flink["Apache Flink"]
    end

    Dashboard <-->|"REST / SSE"| API
    API --> Workers
    Workers -->|"Writes"| DB
    DB -->|"Transaction Logs"| CDC
    CDC -->|"CDC Events"| Broker
    Broker <--> Flink
    Broker --> Consumers
    Consumers -->|"Multi-Join Queries"| DB
    Telemetry -->|"Metrics"| Dashboard
    API --- Telemetry
```

## Run lifecycle

```mermaid
stateDiagram-v2
    [*] --> idle: startup resets pipeline (no connectors, jobs, CDC, trigger)
    idle --> provisioning: Start (approach N)
    provisioning --> running: probe write reached the output topic
    provisioning --> idle: failure (environment torn down, error shown)
    running --> paused: Pause
    paused --> running: Resume
    running --> draining: Stop / Start of another approach
    paused --> draining: Stop
    draining --> idle: all committed changes delivered (or no progress for 15s), summary recorded, teardown
```

Provisioning enables only what approach N needs (see the table in
[metrics_calculation.md](metrics_calculation.md)); teardown removes all of it again.
