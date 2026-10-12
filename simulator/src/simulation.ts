/**
 * Load-generator worker thread. Issues write operations at an exact rate (RatePacer),
 * limits in-flight operations, and reports per-second counters plus latency histograms.
 */
import { parentPort, workerData } from 'worker_threads';
import sql from 'mssql';
import { openPool } from './db';
import { APPROACHES, isApproachId, type ApproachProfile } from './approaches';
import { LatencyHistogram } from './histogram';
import { RatePacer } from './pacer';
import { buildWriteBatch, executeWrite, pickOp, type RequestedOp } from './writer';

const concurrency: number = workerData?.concurrency ?? 50;
const queueLimit: number = workerData?.queueLimit ?? 5000;

let pool: sql.ConnectionPool | null = null;
let profile: ApproachProfile = APPROACHES[1];
let batchSql = buildWriteBatch(profile);
let timeoutMs = 3000;
let insertsOnly = true;
let cardinality = 100;
let weights = { insert: 0.5, update: 0.3, delete: 0.2 };

let running = false;
let paused = false;
const pacer = new RatePacer();
let ticker: NodeJS.Timeout | null = null;

// Per-window counters (reset after each stats message).
let issued = 0, onTime = 0, late = 0, failed = 0, dropped = 0, partialWrites = 0;
let ops = { c: 0, u: 0, d: 0 };
const dbHist = new LatencyHistogram();
const queueHist = new LatencyHistogram();
const errors = new Map<string, number>();

// Concurrency limiter.
let active = 0;
const waiting: (() => void)[] = [];
let outstanding = 0; // issued but not yet settled (queued + active)

async function connect() {
    while (true) {
        try {
            pool = await openPool(concurrency + 2, { label: 'Writer', retries: 5 });
            parentPort?.postMessage({ type: 'ready' });
            return;
        } catch (e) {
            parentPort?.postMessage({ type: 'error', message: String((e as Error).message || e) });
        }
    }
}

function recordError(e: unknown) {
    const msg = String((e as Error)?.message || e).slice(0, 200);
    errors.set(msg, (errors.get(msg) || 0) + 1);
}

async function runOne(scheduledAt: number) {
    outstanding++;
    try {
        if (active < concurrency) {
            active++;
        } else {
            if (waiting.length >= queueLimit) {
                dropped++;
                return;
            }
            // The releasing operation hands its slot straight to us (active is not decremented).
            await new Promise<void>(resolve => waiting.push(resolve));
        }
        try {
            const waitMs = performance.now() - scheduledAt;
            if (waitMs > timeoutMs) {
                // Expired while queued: shed it without touching the database.
                dropped++;
                return;
            }
            await execute(waitMs);
        } finally {
            releaseSlot();
        }
    } finally {
        outstanding--;
    }
}

function releaseSlot() {
    const next = waiting.shift();
    if (next) next();
    else active--;
}

async function execute(waitMs: number) {
    if (!pool) {
        failed++;
        recordError('No database connection');
        return;
    }
    const requestedOp: RequestedOp = pickOp(insertsOnly, weights);
    const batchId = Math.floor(Math.random() * cardinality) + 1;
    const amount = (Math.random() * 1000).toFixed(2);
    const start = performance.now();
    try {
        const result = await executeWrite(pool, profile, batchSql, { batchId, amount, requestedOp });
        const execMs = performance.now() - start;
        dbHist.record(execMs);
        queueHist.record(waitMs);
        ops[result.op]++;
        // The operation committed either way; "late" means it missed the SLA.
        if (waitMs + execMs > timeoutMs) late++; else onTime++;
    } catch (e) {
        failed++;
        if ((e as any)?.partialWrite) partialWrites++;
        recordError(e);
    }
}

function tick() {
    if (!running || paused) return;
    const n = pacer.due(performance.now());
    for (let i = 0; i < n; i++) {
        issued++;
        void runOne(performance.now());
    }
}

function flushStats(final = false) {
    parentPort?.postMessage({
        type: 'stats',
        final,
        issued, onTime, late, failed, dropped, partialWrites,
        ops,
        dbHist: dbHist.toJSON(),
        queueHist: queueHist.toJSON(),
        errors: [...errors.entries()].map(([message, count]) => ({ message, count })),
        inFlight: outstanding,
    });
    issued = onTime = late = failed = dropped = partialWrites = 0;
    ops = { c: 0, u: 0, d: 0 };
    dbHist.reset();
    queueHist.reset();
    errors.clear();
}

setInterval(() => flushStats(), 1000);

async function stop() {
    running = false;
    paused = false;
    if (ticker) clearInterval(ticker);
    ticker = null;
    // Let everything already issued settle (bounded by the SLA timeout plus the DB request timeout).
    const deadline = Date.now() + timeoutMs + 35_000;
    while (outstanding > 0 && Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 25));
    }
    flushStats(true);
    parentPort?.postMessage({ type: 'stopped', abandoned: outstanding });
}

parentPort?.on('message', async (msg) => {
    switch (msg.type) {
        case 'configure': {
            if (isApproachId(msg.approach)) profile = APPROACHES[msg.approach as 1];
            batchSql = buildWriteBatch(profile);
            applyUpdate(msg);
            if (typeof msg.cardinality === 'number' && msg.cardinality >= 1) cardinality = Math.floor(msg.cardinality);
            break;
        }
        case 'update':
            applyUpdate(msg);
            break;
        case 'start':
            running = true;
            paused = false;
            pacer.setRate(msg.rate, performance.now());
            if (!ticker) ticker = setInterval(tick, 5);
            break;
        case 'rate':
            pacer.setRate(msg.rate, performance.now());
            break;
        case 'pause':
            paused = true;
            break;
        case 'resume':
            paused = false;
            pacer.setRate(pacer.getRate(), performance.now()); // don't owe a burst for the paused time
            break;
        case 'stop':
            await stop();
            break;
        case 'reconnect': {
            const old = pool;
            pool = null;
            try { await old?.close(); } catch { /* ignore */ }
            await connect();
            break;
        }
    }
});

function applyUpdate(msg: any) {
    if (typeof msg.timeoutMs === 'number' && msg.timeoutMs > 0) timeoutMs = msg.timeoutMs;
    if (typeof msg.insertsOnly === 'boolean') insertsOnly = msg.insertsOnly;
    if (msg.weights) {
        const w = msg.weights;
        weights = { insert: Number(w.insert) || 0, update: Number(w.update) || 0, delete: Number(w.delete) || 0 };
    }
}

void connect();
