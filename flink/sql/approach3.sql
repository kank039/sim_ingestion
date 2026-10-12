-- Approach 3: stream-to-stream interval join, submitted by the simulator when an Approach 3 run starts
-- (sql-client.sh -f inside the JobManager container) and cancelled when the run ends.
--
-- The writer dual-writes billing_record and enrichment_requests (two separate transactions).
-- Debezium streams both tables; this job joins each billing change with the enrichment request
-- carrying the same change_token and operation, within +/-30s of commit time.

SET 'pipeline.name' = 'sim-approach3-join';
SET 'table.exec.source.idle-timeout' = '5 s';

CREATE TABLE billing_cdc (
    `payload` ROW<
        `before` ROW<`id` INT, `batch_id` INT, `amount` STRING, `change_token` STRING>,
        `after`  ROW<`id` INT, `batch_id` INT, `amount` STRING, `change_token` STRING>,
        `op` STRING,
        `source` ROW<`ts_ms` BIGINT>
    >,
    billing_id AS COALESCE(`payload`.`after`.`id`, `payload`.`before`.`id`),
    batch_id AS COALESCE(`payload`.`after`.`batch_id`, `payload`.`before`.`batch_id`),
    change_token AS COALESCE(`payload`.`after`.`change_token`, `payload`.`before`.`change_token`),
    op AS `payload`.`op`,
    commit_ts_ms AS `payload`.`source`.`ts_ms`,
    event_time AS TO_TIMESTAMP_LTZ(`payload`.`source`.`ts_ms`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sim.sim_db.dbo.billing_record',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sim-approach3-flink',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE request_cdc (
    `payload` ROW<
        `after` ROW<`id` INT, `billing_record_id` INT, `batch_id` INT, `op` STRING, `change_token` STRING, `payload` STRING>,
        `source` ROW<`ts_ms` BIGINT>
    >,
    change_token AS `payload`.`after`.`change_token`,
    req_op AS `payload`.`after`.`op`,
    request_payload AS `payload`.`after`.`payload`,
    request_ts_ms AS `payload`.`source`.`ts_ms`,
    event_time AS TO_TIMESTAMP_LTZ(`payload`.`source`.`ts_ms`, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sim.sim_db.dbo.enrichment_requests',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'sim-approach3-flink',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE enriched_output (
    billing_id INT,
    batch_id INT,
    op STRING,
    change_token STRING,
    request_payload STRING,
    billing_ts_ms BIGINT,
    request_ts_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'sim.approach3.enriched',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json',
    'sink.delivery-guarantee' = 'none'
);

INSERT INTO enriched_output
SELECT b.billing_id, b.batch_id, b.op, b.change_token, r.request_payload, b.commit_ts_ms, r.request_ts_ms
FROM billing_cdc b
JOIN request_cdc r
    ON b.change_token = r.change_token
   AND b.op = r.req_op
   AND r.event_time BETWEEN b.event_time - INTERVAL '30' SECOND AND b.event_time + INTERVAL '30' SECOND;
