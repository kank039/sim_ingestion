/**
 * Open-loop rate pacer. Call `due(now)` frequently; it returns how many operations should be
 * issued so that the cumulative count tracks `rate * elapsed` exactly, including fractional
 * rates (e.g. 1.25 ops/s) and rate changes mid-run.
 */
export class RatePacer {
    private rate = 0;
    private segmentStart = 0;
    private segmentIssued = 0;

    constructor(private readonly maxCatchUpSec = 1) {}

    getRate(): number {
        return this.rate;
    }

    /** Starts a new pacing segment at `now` (also used after pause/resume so no burst is owed). */
    setRate(rate: number, now: number): void {
        this.rate = Number.isFinite(rate) && rate > 0 ? rate : 0;
        this.segmentStart = now;
        this.segmentIssued = 0;
    }

    due(now: number): number {
        if (this.rate <= 0) return 0;
        const target = Math.floor(((now - this.segmentStart) / 1000) * this.rate);
        let n = target - this.segmentIssued;
        if (n <= 0) return 0;
        // If the event loop stalled, don't fire an unbounded burst: cap the catch-up and
        // move the segment forward so the shortfall is visible as a lower issued rate.
        const cap = Math.max(1, Math.ceil(this.rate * this.maxCatchUpSec));
        if (n > cap) {
            this.segmentIssued = target - cap;
            n = cap;
        }
        this.segmentIssued += n;
        return n;
    }
}

/** Splits a total rate evenly across `workers` (fractional shares are fine for RatePacer). */
export function splitRate(total: number, workers: number): number[] {
    const safeTotal = Number.isFinite(total) && total > 0 ? total : 0;
    return Array.from({ length: workers }, () => safeTotal / workers);
}
