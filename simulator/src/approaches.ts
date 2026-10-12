import { config } from './config';

export type ApproachId = 1 | 2 | 3 | 4 | 5;

export const TOPIC_PREFIX = 'sim';
export const cdcTopic = (table: string) => `${TOPIC_PREFIX}.${config.db.database}.dbo.${table}`;
export const FLINK_OUTPUT_TOPIC = 'sim.approach3.enriched';
export const FLINK_JOB_NAME = 'sim-approach3-join';
export const CONNECTOR_NAME_PREFIX = 'sim-';
export const HISTORY_TOPIC_PREFIX = 'sim-schema-history.';
/** Connector names used by older versions of this project; removed on startup so they stop capturing. */
export const LEGACY_CONNECTOR_NAMES = [
    'billing_record_connector',
    'approach1-triggers-connector',
    'approach2-outbox-connector',
    'approach3-cdc-connector',
    'approach5-push-connector',
];

/** Every table that may ever have CDC enabled by this simulator. */
export const ALL_CDC_TABLES = [
    'billing_record', 'invoice_batch', 'cdc_events_shadow', 'outbox_events',
    'enrichment_requests', 'subscriber_plan', 'subscriber_usage', 'rate_schedule',
];

/** Tables written during a run; truncated when a run starts with resetData. */
export const WRITE_TABLES = ['billing_record', 'outbox_events', 'cdc_events_shadow', 'enrichment_requests'];

export type TrackedKind = 'cdc' | 'flink-output';

export interface ApproachProfile {
    id: ApproachId;
    name: string;
    /** trg_billing_record_cdc enabled (Approach 1 only). */
    trigger: boolean;
    /** Tables with SQL Server CDC enabled and captured by the run's Debezium connector. */
    cdcTables: string[];
    /** Topic on which each committed operation produces exactly one (non-tombstone) event. */
    trackedTopic: string;
    trackedKind: TrackedKind;
    /** Debezium connector runs the InvoiceLookup JDBC SMT. */
    smt: boolean;
    /** Flink interval-join job is submitted. */
    flinkJob: boolean;
    /** Node.js consumer workers enrich events from the tracked topic. */
    consumers: boolean;
    /** Writer performs the second (non-atomic) insert into enrichment_requests. */
    dualWrite: boolean;
    /** Writer inserts the outbox row in the same transaction. */
    outbox: boolean;
}

export const APPROACHES: Record<ApproachId, ApproachProfile> = {
    1: {
        id: 1, name: 'Database Triggers (DBA Outbox)',
        trigger: true, cdcTables: ['cdc_events_shadow'],
        trackedTopic: cdcTopic('cdc_events_shadow'), trackedKind: 'cdc',
        smt: false, flinkJob: false, consumers: false, dualWrite: false, outbox: false,
    },
    2: {
        id: 2, name: 'Transactional Outbox',
        trigger: false, cdcTables: ['outbox_events'],
        trackedTopic: cdcTopic('outbox_events'), trackedKind: 'cdc',
        smt: false, flinkJob: false, consumers: false, dualWrite: false, outbox: true,
    },
    3: {
        id: 3, name: 'Stream-to-Stream Join (Flink)',
        trigger: false, cdcTables: ['billing_record', 'enrichment_requests'],
        trackedTopic: FLINK_OUTPUT_TOPIC, trackedKind: 'flink-output',
        smt: false, flinkJob: true, consumers: false, dualWrite: true, outbox: false,
    },
    4: {
        id: 4, name: 'JDBC SMT (Interceptor)',
        trigger: false, cdcTables: ['billing_record'],
        trackedTopic: cdcTopic('billing_record'), trackedKind: 'cdc',
        smt: true, flinkJob: false, consumers: false, dualWrite: false, outbox: false,
    },
    5: {
        id: 5, name: 'CDC Push + Consumer Enrichment (Multi-JOIN)',
        trigger: false, cdcTables: ['billing_record'],
        trackedTopic: cdcTopic('billing_record'), trackedKind: 'cdc',
        smt: false, flinkJob: false, consumers: true, dualWrite: false, outbox: false,
    },
};

export function isApproachId(v: unknown): v is ApproachId {
    return v === 1 || v === 2 || v === 3 || v === 4 || v === 5;
}

/** Topics that must exist before the pipeline starts (Flink fails on missing source topics). */
export function requiredTopics(profile: ApproachProfile): string[] {
    const topics = profile.cdcTables.map(cdcTopic);
    if (!topics.includes(profile.trackedTopic)) topics.push(profile.trackedTopic);
    return topics;
}

export function connectorName(profile: ApproachProfile, runId: string): string {
    return `${CONNECTOR_NAME_PREFIX}a${profile.id}-${runId}`;
}

export function buildConnectorConfig(profile: ApproachProfile, name: string): Record<string, string> {
    const { internal, db } = config;
    const cfg: Record<string, string> = {
        'connector.class': 'io.debezium.connector.sqlserver.SqlServerConnector',
        'tasks.max': '1',
        'database.hostname': internal.dbHost,
        'database.port': String(internal.dbPort),
        'database.user': db.user,
        'database.password': db.password,
        'database.names': db.database,
        'database.encrypt': 'false',
        'topic.prefix': TOPIC_PREFIX,
        'table.include.list': profile.cdcTables.map(t => `dbo.${t}`).join(','),
        // A fresh connector per run must not replay existing rows: stream only new changes.
        'snapshot.mode': 'no_data',
        'decimal.handling.mode': 'string',
        'tombstones.on.delete': 'true',
        'include.schema.changes': 'false',
        'schema.history.internal.kafka.bootstrap.servers': internal.kafkaBootstrap,
        'schema.history.internal.kafka.topic': `${HISTORY_TOPIC_PREFIX}${name}`,
        'schema.history.internal.store.only.captured.tables.ddl': 'true',
    };
    if (profile.smt) {
        Object.assign(cfg, {
            'transforms': 'enrich',
            'transforms.enrich.type': 'sim.smt.InvoiceLookup',
            'transforms.enrich.jdbc.url': `jdbc:sqlserver://${internal.dbHost}:${internal.dbPort};databaseName=${db.database};encrypt=false`,
            'transforms.enrich.jdbc.user': db.user,
            'transforms.enrich.jdbc.password': db.password,
        });
    }
    return cfg;
}
