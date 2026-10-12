import type { Admin } from 'kafkajs';
import type { TrackedKind } from './approaches';
import { parseTrackedMessage, type ParsedEvent } from './cdcEvent';
import { createKafka, endOffsets, setGroupOffsets, type PartitionOffsets } from './kafka';

export interface VerifyResult {
    topic: string;
    messagesChecked: number;
    uniqueEvents: number;
    expected: number;
    missing: number;
    duplicates: number;
    keyOrderViolations: number;
    entitiesChecked: number;
    entitiesSpreadAcrossPartitions: number;
    ordered: boolean;
    tombstones: number;
    unparsable: number;
    complete: boolean;
}

/**
 * Pure analysis of a run's events:
 *  - duplicates: the same source change (log position / change token) delivered more than once
 *  - key ordering: within a message key, source log positions must never go backwards
 *  - entity spread: events for one entity on several partitions cannot be consumed in order
 */
export class VerifyAnalyzer {
    private seen = new Map<string, number>();
    private lastPositionByKey = new Map<string, string>();
    private entityPartitions = new Map<string, Set<number>>();
    messages = 0;
    tombstones = 0;
    unparsable = 0;
    keyOrderViolations = 0;

    add(partition: number, key: string | null, ev: ParsedEvent | null) {
        if (!ev) { this.unparsable++; return; }
        if (ev.tombstone) { this.tombstones++; return; }
        this.messages++;
        if (ev.identity) this.seen.set(ev.identity, (this.seen.get(ev.identity) || 0) + 1);
        if (ev.position && key !== null) {
            const k = `${partition}|${key}`;
            const last = this.lastPositionByKey.get(k);
            if (last !== undefined && ev.position < last) this.keyOrderViolations++;
            if (last === undefined || ev.position > last) this.lastPositionByKey.set(k, ev.position);
        }
        if (ev.entityId !== undefined) {
            let set = this.entityPartitions.get(ev.entityId);
            if (!set) this.entityPartitions.set(ev.entityId, set = new Set());
            set.add(partition);
        }
    }

    result(topic: string, expected: number, complete: boolean): VerifyResult {
        let duplicates = 0;
        for (const c of this.seen.values()) if (c > 1) duplicates += c - 1;
        const unique = this.seen.size || this.messages;
        let spread = 0;
        for (const s of this.entityPartitions.values()) if (s.size > 1) spread++;
        return {
            topic,
            messagesChecked: this.messages,
            uniqueEvents: unique,
            expected,
            missing: Math.max(0, expected - unique),
            duplicates,
            keyOrderViolations: this.keyOrderViolations,
            entitiesChecked: this.entityPartitions.size,
            entitiesSpreadAcrossPartitions: spread,
            ordered: this.keyOrderViolations === 0 && spread === 0,
            tombstones: this.tombstones,
            unparsable: this.unparsable,
            complete,
        };
    }
}

/** Reads [startOffsets, current end) of the topic and analyzes it. */
export async function verifyTopic(admin: Admin, topic: string, kind: TrackedKind, startOffsets: PartitionOffsets, expected: number, timeoutMs = 60_000): Promise<VerifyResult> {
    const end = await endOffsets(admin, topic);
    const startMap = new Map(startOffsets.map(p => [p.partition, Number(p.offset)]));
    const remaining = new Set(end.filter(p => Number(p.offset) > (startMap.get(p.partition) ?? 0)).map(p => p.partition));
    const endMap = new Map(end.map(p => [p.partition, Number(p.offset)]));
    const analyzer = new VerifyAnalyzer();
    if (remaining.size === 0) return analyzer.result(topic, expected, true);

    const groupId = `sim-verify-${Date.now()}`;
    await setGroupOffsets(admin, groupId, topic, end.map(p => ({ partition: p.partition, offset: String(startMap.get(p.partition) ?? 0) })));
    const consumer = createKafka('sim-verify').consumer({ groupId, maxWaitTimeInMs: 200 });
    let done!: () => void;
    const finished = new Promise<void>(r => (done = r));
    try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });
        await consumer.run({
            eachBatchAutoResolve: true,
            eachBatch: async ({ batch }) => {
                const last = endMap.get(batch.partition) ?? 0;
                for (const m of batch.messages) {
                    const offset = Number(m.offset);
                    if (offset >= last) continue;
                    analyzer.add(batch.partition, m.key ? m.key.toString() : null, parseTrackedMessage(m.value, kind, topic));
                    if (offset >= last - 1) remaining.delete(batch.partition);
                }
                if (remaining.size === 0) done();
            },
        });
        const timer = new Promise<void>(r => setTimeout(r, timeoutMs));
        await Promise.race([finished, timer]);
    } finally {
        await consumer.disconnect().catch(() => undefined);
        await admin.deleteGroups([groupId]).catch(() => undefined);
    }
    return analyzer.result(topic, expected, remaining.size === 0);
}
