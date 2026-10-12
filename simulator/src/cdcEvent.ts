import type { TrackedKind } from './approaches';

export interface ParsedEvent {
    tombstone: boolean;
    op?: string;
    /** Commit time of the source change (Debezium source.ts_ms), epoch ms. */
    commitTsMs?: number;
    row?: Record<string, any>;
    /** Uniquely identifies one source change; used for duplicate detection. */
    identity?: string;
    /** Sortable position in the source log; used for per-key ordering checks. */
    position?: string;
    /** The business entity the event is about (billing record / aggregate). */
    entityId?: string;
    enrichment?: { invoice_number?: string | null; invoice_status?: string | null; lookup_us?: number } | null;
}

const pad = (n: unknown, width = 10) => String(n ?? '').padStart(width, '0');

/** Field holding the business entity id, per source table. */
const ENTITY_FIELD: Record<string, string> = {
    billing_record: 'id',
    cdc_events_shadow: 'billing_id',
    outbox_events: 'aggregate_id',
    enrichment_requests: 'billing_record_id',
};

export function tableFromTopic(topic: string): string {
    const parts = topic.split('.');
    return parts[parts.length - 1];
}

/**
 * Parses a message from an approach's tracked topic. Returns null for messages that can't be
 * parsed (counted separately), and `{ tombstone: true }` for Kafka tombstones.
 */
export function parseTrackedMessage(value: Buffer | string | null | undefined, kind: TrackedKind, topic: string): ParsedEvent | null {
    if (value === null || value === undefined || value.length === 0) return { tombstone: true };
    let json: any;
    try {
        json = JSON.parse(value.toString());
    } catch {
        return null;
    }
    if (!json || typeof json !== 'object') return null;

    if (kind === 'flink-output') {
        if (json.change_token === undefined && json.billing_id === undefined) return null;
        return {
            tombstone: false,
            op: json.op,
            commitTsMs: typeof json.billing_ts_ms === 'number' ? json.billing_ts_ms : undefined,
            row: json,
            identity: `${json.change_token}|${json.op}`,
            entityId: json.billing_id !== undefined && json.billing_id !== null ? String(json.billing_id) : undefined,
        };
    }

    // Debezium JSON, with or without the schema wrapper.
    const env = json.payload !== undefined && json.schema !== undefined ? json.payload : json;
    if (!env || typeof env !== 'object' || typeof env.op !== 'string') return null;
    const row = env.after ?? env.before ?? undefined;
    const src = env.source || {};
    const table = tableFromTopic(topic);
    const entityField = ENTITY_FIELD[table];
    const entity = row && entityField ? row[entityField] : undefined;
    const position = src.commit_lsn !== undefined ? `${src.commit_lsn}|${src.change_lsn}|${pad(src.event_serial_no)}` : undefined;
    return {
        tombstone: false,
        op: env.op,
        commitTsMs: typeof src.ts_ms === 'number' ? src.ts_ms : undefined,
        row,
        identity: position,
        position,
        entityId: entity !== undefined && entity !== null ? String(entity) : undefined,
        enrichment: env.enrichment,
    };
}

/** Extracts the billing record id and batch id a raw billing_record CDC event refers to. */
export function billingRefFromEvent(ev: ParsedEvent | null): { billingId: number; batchId: number } | null {
    if (!ev || ev.tombstone || !ev.row) return null;
    const billingId = Number(ev.row.id);
    const batchId = Number(ev.row.batch_id);
    if (!Number.isInteger(billingId) || !Number.isInteger(batchId)) return null;
    return { billingId, batchId };
}
