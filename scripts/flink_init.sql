-- Flink SQL Job Placeholder for Approach 3 (Stream-to-Stream Join)
-- This script creates the Kafka tables and performs the join.

-- 1. Create the source table for Billing Records
CREATE TABLE billing_records (
    id INT,
    account_id INT,
    amount DECIMAL(10,2),
    created_at TIMESTAMP(3),
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sim.sim_db.dbo.billing_record',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-consumer-group',
    'format' = 'debezium-json'
);

-- 2. Create the source table for Enrichment Requests (from dual-writes)
CREATE TABLE enrichment_requests (
    billing_record_id INT,
    enrichment_data STRING,
    requested_at TIMESTAMP(3),
    WATERMARK FOR requested_at AS requested_at - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sim.sim_db.dbo.enrichment_requests',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-consumer-group',
    'format' = 'debezium-json'
);

-- 3. Create the sink table (e.g., Elasticsearch, another Kafka topic, or Postgres)
CREATE TABLE enriched_billing_output (
    id INT,
    account_id INT,
    amount DECIMAL(10,2),
    enrichment_data STRING,
    processed_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_billing_records',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- 4. Perform the Stream-to-Stream Interval Join and insert into the Sink
INSERT INTO enriched_billing_output
SELECT 
    b.id,
    b.account_id,
    b.amount,
    e.enrichment_data,
    CURRENT_TIMESTAMP
FROM billing_records b
JOIN enrichment_requests e
ON b.id = e.billing_record_id
AND e.requested_at BETWEEN b.created_at - INTERVAL '10' SECOND AND b.created_at + INTERVAL '10' SECOND;
