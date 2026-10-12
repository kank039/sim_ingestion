import { describe, it, expect } from 'vitest';
import { parseTrackedMessage, billingRefFromEvent } from '../src/cdcEvent';

const envelope = (payload: any) => Buffer.from(JSON.stringify({ schema: {}, payload }));

describe('parseTrackedMessage', () => {
    it('recognises tombstones', () => {
        expect(parseTrackedMessage(null, 'cdc', 'sim.sim_db.dbo.billing_record')).toEqual({ tombstone: true });
    });

    it('parses a Debezium update with commit time, identity and entity', () => {
        const ev = parseTrackedMessage(envelope({
            op: 'u', before: { id: 7, batch_id: 3 }, after: { id: 7, batch_id: 3 },
            source: { ts_ms: 1000, commit_lsn: '0000002c:00000ab8:0003', change_lsn: '0000002c:00000ab8:0002', event_serial_no: 2 },
        }), 'cdc', 'sim.sim_db.dbo.billing_record');
        expect(ev?.op).toBe('u');
        expect(ev?.commitTsMs).toBe(1000);
        expect(ev?.entityId).toBe('7');
        expect(ev?.identity).toBe('0000002c:00000ab8:0003|0000002c:00000ab8:0002|0000000002');
    });

    it('uses `before` for deletes and the right entity field per table', () => {
        const del = parseTrackedMessage(envelope({ op: 'd', before: { id: 9, batch_id: 4 }, after: null, source: { ts_ms: 5 } }), 'cdc', 'sim.sim_db.dbo.billing_record');
        expect(billingRefFromEvent(del)).toEqual({ billingId: 9, batchId: 4 });
        const shadow = parseTrackedMessage(envelope({ op: 'c', after: { id: 1, billing_id: 42 }, source: {} }), 'cdc', 'sim.sim_db.dbo.cdc_events_shadow');
        expect(shadow?.entityId).toBe('42');
        const outbox = parseTrackedMessage(envelope({ op: 'c', after: { id: 1, aggregate_id: 77 }, source: {} }), 'cdc', 'sim.sim_db.dbo.outbox_events');
        expect(outbox?.entityId).toBe('77');
    });

    it('reads the SMT enrichment block', () => {
        const ev = parseTrackedMessage(envelope({ op: 'c', after: { id: 1, batch_id: 2 }, source: { ts_ms: 1 }, enrichment: { invoice_number: 'INV-1002', lookup_us: 900 } }), 'cdc', 'sim.sim_db.dbo.billing_record');
        expect(ev?.enrichment?.invoice_number).toBe('INV-1002');
    });

    it('parses Flink output rows', () => {
        const ev = parseTrackedMessage(Buffer.from(JSON.stringify({ billing_id: 5, op: 'c', change_token: 'T', billing_ts_ms: 123 })), 'flink-output', 'sim.approach3.enriched');
        expect(ev).toMatchObject({ op: 'c', commitTsMs: 123, identity: 'T|c', entityId: '5' });
    });

    it('returns null for garbage and gives no billing ref for tombstones', () => {
        expect(parseTrackedMessage(Buffer.from('not json'), 'cdc', 't')).toBeNull();
        expect(billingRefFromEvent({ tombstone: true })).toBeNull();
    });
});
