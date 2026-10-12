import { describe, it, expect } from 'vitest';
import { RatePacer, splitRate } from '../src/pacer';

function simulate(pacer: RatePacer, durationMs: number, tickMs = 5, start = 0) {
    let issued = 0;
    for (let t = start; t <= start + durationMs; t += tickMs) issued += pacer.due(t);
    return issued;
}

describe('RatePacer', () => {
    it('issues exactly the requested rate, including low and fractional rates', () => {
        for (const rate of [0.5, 1.25, 10, 15, 99, 1000]) {
            const p = new RatePacer();
            p.setRate(rate, 0);
            expect(simulate(p, 10_000)).toBe(Math.floor(rate * 10));
        }
    });

    it('issues nothing at rate 0', () => {
        const p = new RatePacer();
        p.setRate(0, 0);
        expect(simulate(p, 5000)).toBe(0);
    });

    it('follows rate changes from the moment they are applied', () => {
        const p = new RatePacer();
        p.setRate(100, 0);
        const first = simulate(p, 1000);
        p.setRate(500, 1000);
        const second = simulate(p, 1000, 5, 1000);
        expect(first).toBe(100);
        expect(second).toBe(500);
    });

    it('caps catch-up bursts after a stall', () => {
        const p = new RatePacer(1);
        p.setRate(100, 0);
        expect(p.due(10_000)).toBe(100); // a 10s stall issues at most 1s worth
        expect(p.due(10_005)).toBe(0);
    });
});

describe('splitRate', () => {
    it('splits totals so the shares add up exactly', () => {
        for (const [total, n] of [[10, 8], [100, 8], [7, 3], [0, 4]] as const) {
            const shares = splitRate(total, n);
            expect(shares).toHaveLength(n);
            expect(shares.reduce((a, b) => a + b, 0)).toBeCloseTo(total, 9);
        }
    });
});
