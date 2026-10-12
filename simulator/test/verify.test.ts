import { describe, it, expect } from 'vitest';
import { VerifyAnalyzer } from '../src/verify';
import type { ParsedEvent } from '../src/cdcEvent';

const ev = (identity: string, entityId: string, position = identity): ParsedEvent => ({ tombstone: false, identity, position, entityId });

describe('VerifyAnalyzer', () => {
    it('reports a clean run', () => {
        const a = new VerifyAnalyzer();
        a.add(0, 'k1', ev('001', '1'));
        a.add(0, 'k1', ev('002', '1'));
        a.add(1, 'k2', ev('003', '2'));
        a.add(1, 'k2', { tombstone: true });
        const r = a.result('t', 3, true);
        expect(r).toMatchObject({ messagesChecked: 3, missing: 0, duplicates: 0, keyOrderViolations: 0, ordered: true, tombstones: 1 });
    });

    it('does not treat repeated updates of one row as duplicates', () => {
        const a = new VerifyAnalyzer();
        a.add(0, 'k1', ev('001', '1'));
        a.add(0, 'k1', ev('002', '1'));
        expect(a.result('t', 2, true).duplicates).toBe(0);
    });

    it('detects redelivered changes, missing events and key-order regressions', () => {
        const a = new VerifyAnalyzer();
        a.add(0, 'k1', ev('002', '1'));
        a.add(0, 'k1', ev('001', '1')); // went backwards
        a.add(0, 'k1', ev('002', '1')); // redelivered
        const r = a.result('t', 5, true);
        expect(r.duplicates).toBe(1);
        expect(r.keyOrderViolations).toBe(1);
        expect(r.missing).toBe(3);
        expect(r.ordered).toBe(false);
    });

    it('flags entities spread over several partitions (no per-entity ordering guarantee)', () => {
        const a = new VerifyAnalyzer();
        a.add(0, 'a', ev('001', '1'));
        a.add(3, 'b', ev('002', '1'));
        const r = a.result('t', 2, true);
        expect(r.entitiesSpreadAcrossPartitions).toBe(1);
        expect(r.ordered).toBe(false);
    });
});
