/**
 * Log-bucketed latency histogram (~1% relative error).
 *
 * Unlike averaging per-thread percentiles, histograms can be merged exactly across
 * worker threads and across time windows, so the p95/p99 we report are real percentiles
 * of every recorded sample.
 */
const MIN_VALUE = 0.01; // ms; everything at or below lands in bucket 0
const GROWTH = 1.02;
const LOG_GROWTH = Math.log(GROWTH);

export interface SerializedHistogram {
    buckets: [number, number][];
    count: number;
    sum: number;
    min: number;
    max: number;
}

export class LatencyHistogram {
    private buckets = new Map<number, number>();
    count = 0;
    sum = 0;
    min = Infinity;
    max = -Infinity;

    static bucketOf(value: number): number {
        if (!(value > MIN_VALUE)) return 0;
        return 1 + Math.floor(Math.log(value / MIN_VALUE) / LOG_GROWTH);
    }

    /** Geometric midpoint of a bucket. */
    static valueOf(bucket: number): number {
        if (bucket <= 0) return MIN_VALUE;
        return MIN_VALUE * Math.pow(GROWTH, bucket - 0.5);
    }

    record(value: number): void {
        if (!Number.isFinite(value)) return;
        const v = Math.max(0, value);
        const b = LatencyHistogram.bucketOf(v);
        this.buckets.set(b, (this.buckets.get(b) || 0) + 1);
        this.count++;
        this.sum += v;
        if (v < this.min) this.min = v;
        if (v > this.max) this.max = v;
    }

    merge(other: LatencyHistogram | SerializedHistogram | null | undefined): void {
        if (!other || other.count === 0) return;
        const entries: Iterable<[number, number]> = other instanceof LatencyHistogram ? other.buckets.entries() : other.buckets;
        for (const [b, c] of entries) this.buckets.set(b, (this.buckets.get(b) || 0) + c);
        this.count += other.count;
        this.sum += other.sum;
        if (other.min < this.min) this.min = other.min;
        if (other.max > this.max) this.max = other.max;
    }

    mean(): number {
        return this.count > 0 ? this.sum / this.count : 0;
    }

    /** p in [0, 1]. Returns 0 for an empty histogram. */
    percentile(p: number): number {
        if (this.count === 0) return 0;
        if (p <= 0) return this.min;
        if (p >= 1) return this.max;
        const rank = Math.ceil(p * this.count);
        const sorted = [...this.buckets.keys()].sort((a, b) => a - b);
        let seen = 0;
        for (const b of sorted) {
            seen += this.buckets.get(b)!;
            if (seen >= rank) {
                return Math.min(this.max, Math.max(this.min, LatencyHistogram.valueOf(b)));
            }
        }
        return this.max;
    }

    summary() {
        return {
            count: this.count,
            avg: this.mean(),
            p50: this.percentile(0.5),
            p95: this.percentile(0.95),
            p99: this.percentile(0.99),
            max: this.count > 0 ? this.max : 0,
        };
    }

    reset(): void {
        this.buckets.clear();
        this.count = 0;
        this.sum = 0;
        this.min = Infinity;
        this.max = -Infinity;
    }

    toJSON(): SerializedHistogram {
        return { buckets: [...this.buckets.entries()], count: this.count, sum: this.sum, min: this.min, max: this.max };
    }

    static from(serialized: SerializedHistogram | null | undefined): LatencyHistogram {
        const h = new LatencyHistogram();
        h.merge(serialized);
        return h;
    }
}

export type HistogramSummary = ReturnType<LatencyHistogram['summary']>;
