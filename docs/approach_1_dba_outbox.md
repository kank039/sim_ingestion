# Approach 1: Database Triggers (DBA Outbox)

```mermaid
flowchart TD
    App["Simulator App"] -->|"1. Insert/Update/Delete"| DB_Main[("billing_record")]
    DB_Main -->|"2. Trigger Execution"| DB_Outbox[("cdc_events_shadow")]
    CDC["Debezium CDC"] -->|"3. Tail Log"| DB_Outbox
    CDC -->|"4. Push Event"| Kafka[["Kafka Topic"]]
```

**Description:**
This approach relies on database triggers to capture mutations on the domain table (`billing_record`) and automatically insert a corresponding event (joined with `invoice_batch`) into the `cdc_events_shadow` table. Debezium then tails the transaction log for that table only and pushes events to `sim.sim_db.dbo.cdc_events_shadow`.

**In the simulator:** the trigger `trg_billing_record_cdc` is enabled only while an Approach 1 run is active; CDC is enabled only on `cdc_events_shadow`.

### Pros:
- **Zero Application Changes**: Application code doesn't need to be modified significantly, as the event generation logic is pushed entirely to the database.
- **Guaranteed Consistency**: Ensures an event is created in the exact same transaction as the domain entity mutation.

### Cons:
- **High Performance Overhead**: Database triggers can significantly slow down write operations (Inserts/Updates/Deletes).
- **Maintenance Burden**: Triggers are notoriously difficult to debug, trace, and version-control compared to application code.
- **Tight Coupling**: Business logic for event publishing gets tightly coupled with the database schema.
