/**
 * Approach 5 consumer thread. Hosts one Kafka consumer group per assigned subscriber; every
 * raw billing_record CDC event is enriched with a multi-table JOIN against SQL Server.
 */
import { parentPort, workerData } from 'worker_threads';
import sql from 'mssql';
import type { Consumer, EachMessagePayload } from 'kafkajs';
import { openPool } from './db';
import { createKafka, setGroupOffsets, type PartitionOffsets } from './kafka';
import { parseTrackedMessage, billingRefFromEvent } from './cdcEvent';
import { LatencyHistogram } from './histogram';

const workerId: number = workerData.workerId;
const subscribers: number[] = workerData.subscribers;
const groupPrefix: string = workerData.groupPrefix;
const topic: string = workerData.topic;
const startOffsets: PartitionOffsets = workerData.startOffsets;

const ENRICHMENT_QUERY = `
SELECT
    br.id AS billing_id, br.amount, br.created_at,
    ib.invoice_number, ib.status AS invoice_status,
    sp.plan_name, sp.plan_type, sp.base_rate, sp.discount_pct,
    su.usage_type, su.quantity, su.unit,
    rs.rate_per_unit, rs.min_charge, rs.max_charge
FROM dbo.invoice_batch ib WITH (NOLOCK)
INNER JOIN dbo.subscriber_plan sp WITH (NOLOCK) ON ib.plan_id = sp.id
LEFT JOIN dbo.billing_record br WITH (NOLOCK) ON br.id = @billingId
LEFT JOIN dbo.subscriber_usage su WITH (NOLOCK) ON su.batch_id = ib.id
LEFT JOIN dbo.rate_schedule rs WITH (NOLOCK) ON rs.plan_id = sp.id AND rs.usage_type = su.usage_type
WHERE ib.id = @batchId;`;

let pool: sql.ConnectionPool | null = null;
let consumers: Consumer[] = [];
let running = false;

let consumed = 0, failed = 0, skipped = 0, misses = 0;
const enrichHist = new LatencyHistogram();
const e2eHist = new LatencyHistogram();
/** subscriber -> partition -> next offset to process */
const positions: Record<number, Record<number, string>> = {};

function flush() {
    parentPort?.postMessage({
        type: 'consumer-stats',
        workerId,
        consumed, failed, skipped, misses,
        enrich: enrichHist.toJSON(),
        e2e: e2eHist.toJSON(),
        positions,
    });
    consumed = failed = skipped = misses = 0;
    enrichHist.reset();
    e2eHist.reset();
}
setInterval(flush, 1000);

function handlerFor(subscriber: number) {
    return async ({ partition, message }: EachMessagePayload) => {
        positions[subscriber][partition] = String(Number(message.offset) + 1);
        if (!running || !pool) return;
        const ev = parseTrackedMessage(message.value, 'cdc', topic);
        const ref = billingRefFromEvent(ev);
        if (!ref) {
            skipped++; // tombstones and unparsable messages carry nothing to enrich
            return;
        }
        const start = performance.now();
        try {
            const req = pool.request();
            req.input('billingId', sql.Int, ref.billingId);
            req.input('batchId', sql.Int, ref.batchId);
            const res = await req.query(ENRICHMENT_QUERY);
            enrichHist.record(performance.now() - start);
            if (ev?.commitTsMs !== undefined) e2eHist.record(Math.max(0, Date.now() - ev.commitTsMs));
            if (res.recordset.length === 0) misses++;
            consumed++;
        } catch (e) {
            failed++;
            if (failed <= 3) console.error(`[Consumer ${workerId}] Enrichment failed:`, (e as Error).message);
        }
    };
}

async function start() {
    pool = await openPool(subscribers.length + 2, { label: `Consumer ${workerId}`, retries: 10 });
    const kafka = createKafka(`sim-consumer-${workerId}`);
    const admin = kafka.admin();
    await admin.connect();
    for (const sub of subscribers) {
        await setGroupOffsets(admin, `${groupPrefix}-sub-${sub}`, topic, startOffsets);
    }
    await admin.disconnect();

    running = true;
    const joins: Promise<void>[] = [];
    for (const sub of subscribers) {
        positions[sub] = Object.fromEntries(startOffsets.map(p => [p.partition, p.offset]));
        const consumer = kafka.consumer({ groupId: `${groupPrefix}-sub-${sub}`, maxWaitTimeInMs: 100 });
        await consumer.connect();
        joins.push(new Promise<void>(resolve => consumer.on(consumer.events.GROUP_JOIN, () => resolve())));
        await consumer.subscribe({ topic, fromBeginning: false });
        await consumer.run({ eachMessage: handlerFor(sub) });
        consumers.push(consumer);
    }
    await Promise.race([Promise.all(joins), new Promise(r => setTimeout(r, 30_000))]);
    parentPort?.postMessage({ type: 'consumer-ready', workerId, subscribers: subscribers.length });
}

async function stop() {
    running = false;
    await Promise.allSettled(consumers.map(c => c.disconnect()));
    consumers = [];
    try { await pool?.close(); } catch { /* ignore */ }
    pool = null;
    flush();
    parentPort?.postMessage({ type: 'consumer-stopped', workerId });
}

parentPort?.on('message', async (msg) => {
    if (msg.type === 'stop') await stop();
});

start().catch(e => parentPort?.postMessage({ type: 'consumer-error', workerId, error: String((e as Error).message || e) }));
