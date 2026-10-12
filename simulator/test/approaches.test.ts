import { describe, it, expect } from 'vitest';
import { APPROACHES, buildConnectorConfig, connectorName, requiredTopics, ALL_CDC_TABLES } from '../src/approaches';
import { pickOp, buildWriteBatch } from '../src/writer';

describe('approach profiles', () => {
    it('only Approach 1 uses the trigger and each approach captures only its own tables', () => {
        expect(Object.values(APPROACHES).filter(p => p.trigger).map(p => p.id)).toEqual([1]);
        expect(APPROACHES[1].cdcTables).toEqual(['cdc_events_shadow']);
        expect(APPROACHES[2].cdcTables).toEqual(['outbox_events']);
        expect(APPROACHES[3].cdcTables).toEqual(['billing_record', 'enrichment_requests']);
        expect(APPROACHES[4].cdcTables).toEqual(['billing_record']);
        expect(APPROACHES[5].cdcTables).toEqual(['billing_record']);
        for (const p of Object.values(APPROACHES)) for (const t of p.cdcTables) expect(ALL_CDC_TABLES).toContain(t);
    });

    it('builds a fresh, non-snapshotting connector per run with the SMT only for Approach 4', () => {
        const name = connectorName(APPROACHES[4], '123');
        const cfg = buildConnectorConfig(APPROACHES[4], name);
        expect(name).toBe('sim-a4-123');
        expect(cfg['snapshot.mode']).toBe('no_data');
        expect(cfg['table.include.list']).toBe('dbo.billing_record');
        expect(cfg['transforms.enrich.type']).toBe('sim.smt.InvoiceLookup');
        expect(cfg['schema.history.internal.kafka.topic']).toContain(name);
        expect(buildConnectorConfig(APPROACHES[5], 'x').transforms).toBeUndefined();
    });

    it('requires the Flink source and output topics for Approach 3', () => {
        expect(requiredTopics(APPROACHES[3])).toEqual([
            'sim.sim_db.dbo.billing_record', 'sim.sim_db.dbo.enrichment_requests', 'sim.approach3.enriched',
        ]);
    });

    it('wraps only the outbox write in a transaction', () => {
        expect(buildWriteBatch(APPROACHES[2])).toMatch(/BEGIN TRAN[\s\S]*outbox_events[\s\S]*COMMIT TRAN/);
        expect(buildWriteBatch(APPROACHES[1])).not.toMatch(/BEGIN TRAN|outbox_events/);
    });
});

describe('pickOp', () => {
    it('honours inserts-only and the weights', () => {
        const w = { insert: 0.5, update: 0.3, delete: 0.2 };
        expect(pickOp(true, w, 0.01)).toBe('c');
        expect(pickOp(false, w, 0.1)).toBe('d');
        expect(pickOp(false, w, 0.3)).toBe('u');
        expect(pickOp(false, w, 0.9)).toBe('c');
        expect(pickOp(false, { insert: 0, update: 0, delete: 0 }, 0.5)).toBe('c');
    });
});
