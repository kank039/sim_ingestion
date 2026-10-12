import { describe, it, expect } from 'vitest';
import { LatencyHistogram } from '../src/histogram';

describe('LatencyHistogram', () => {
    it('returns percentiles within ~2% of the exact value', () => {
        const h = new LatencyHistogram();
        const values = Array.from({ length: 10_000 }, (_, i) => i + 1); // 1..10000 ms
        values.forEach(v => h.record(v));
        expect(h.count).toBe(10_000);
        expect(h.mean()).toBeCloseTo(5000.5, 5);
        for (const [p, exact] of [[0.5, 5000], [0.95, 9500], [0.99, 9900]] as const) {
            expect(Math.abs(h.percentile(p) - exact) / exact).toBeLessThan(0.02);
        }
        expect(h.percentile(1)).toBe(10_000);
        expect(h.percentile(0)).toBe(1);
    });

    it('merges exactly: merged percentiles equal those of the combined data', () => {
        const a = new LatencyHistogram();
        const b = new LatencyHistogram();
        const all = new LatencyHistogram();
        for (let i = 0; i < 1000; i++) { a.record(5); all.record(5); }   // fast worker
        for (let i = 0; i < 10; i++) { b.record(500); all.record(500); } // slow worker
        const merged = LatencyHistogram.from(a.toJSON());
        merged.merge(b.toJSON());
        expect(merged.count).toBe(1010);
        expect(merged.percentile(0.99)).toBe(all.percentile(0.99));
        // Averaging the two workers' p99s (the old approach) would report ~252ms; the real p99 is ~5ms.
        expect(merged.percentile(0.99)).toBeLessThan(6);
    });

    it('handles empty, zero and invalid values', () => {
        const h = new LatencyHistogram();
        expect(h.percentile(0.95)).toBe(0);
        expect(h.mean()).toBe(0);
        h.record(0);
        h.record(NaN);
        h.record(-3);
        expect(h.count).toBe(2);
        expect(h.percentile(0.5)).toBe(0);
    });
});
