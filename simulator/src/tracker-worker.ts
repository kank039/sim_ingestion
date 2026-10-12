/**
 * Pipeline tracker thread: consumes the run's tracked topic from the exact offsets captured
 * at run start, counts delivered change events and measures commit -> topic latency.
 */
import { parentPort, workerData } from 'worker_threads';
import type { Consumer } from 'kafkajs';
import { createKafka, setGroupOffsets, type PartitionOffsets } from './kafka';
import { parseTrackedMessage } from './cdcEvent';
import { LatencyHistogram } from './histogram';
import type { TrackedKind } from './approaches';

const topic: string = workerData.topic;
const kind: TrackedKind = workerData.kind;
const groupId: string = workerData.groupId;
const startOffsets: PartitionOffsets = workerData.startOffsets;
const checkEnrichment: boolean = !!workerData.checkEnrichment;

let delivered = 0, tombstones = 0, unparsable = 0, enrichmentMisses = 0;
const latency = new LatencyHistogram();
const lookupHist = new LatencyHistogram();
let consumer: Consumer | null = null;

function flush() {
    parentPort?.postMessage({
        type: 'tracker-stats',
        delivered, tombstones, unparsable, enrichmentMisses,
        latency: latency.toJSON(),
        lookup: lookupHist.toJSON(),
    });
    delivered = tombstones = unparsable = enrichmentMisses = 0;
    latency.reset();
    lookupHist.reset();
}
setInterval(flush, 1000);

async function start() {
    const kafka = createKafka(`sim-tracker-${groupId}`);
    const admin = kafka.admin();
    await admin.connect();
    await setGroupOffsets(admin, groupId, topic, startOffsets);
    await admin.disconnect();

    consumer = kafka.consumer({ groupId, maxWaitTimeInMs: 100 });
    await consumer.connect();
    const joined = new Promise<void>(resolve => consumer!.on(consumer!.events.GROUP_JOIN, () => resolve()));
    await consumer.subscribe({ topic, fromBeginning: false });
    await consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: async ({ batch }) => {
            const now = Date.now();
            for (const m of batch.messages) {
                const ev = parseTrackedMessage(m.value, kind, topic);
                if (!ev) { unparsable++; continue; }
                if (ev.tombstone) { tombstones++; continue; }
                delivered++;
                if (ev.commitTsMs !== undefined) latency.record(Math.max(0, now - ev.commitTsMs));
                if (checkEnrichment) {
                    if (!ev.enrichment || !ev.enrichment.invoice_number) enrichmentMisses++;
                    if (ev.enrichment && typeof ev.enrichment.lookup_us === 'number') lookupHist.record(ev.enrichment.lookup_us / 1000);
                }
            }
        },
    });
    await Promise.race([joined, new Promise(r => setTimeout(r, 30_000))]);
    parentPort?.postMessage({ type: 'tracker-ready' });
}

parentPort?.on('message', async (msg) => {
    if (msg.type === 'stop') {
        try { await consumer?.disconnect(); } catch { /* ignore */ }
        flush();
        parentPort?.postMessage({ type: 'tracker-stopped' });
    }
});

start().catch(e => parentPort?.postMessage({ type: 'tracker-error', message: String((e as Error).message || e) }));
