import { Kafka, logLevel, type Admin } from 'kafkajs';
import { config } from './config';

export type PartitionOffsets = { partition: number; offset: string }[];

export function createKafka(clientId: string) {
    return new Kafka({ clientId, brokers: config.kafkaBrokers, logLevel: logLevel.WARN });
}

/** Current end offsets ("high watermarks") of every partition. Empty if the topic doesn't exist. */
export async function endOffsets(admin: Admin, topic: string): Promise<PartitionOffsets> {
    try {
        const res = await admin.fetchTopicOffsets(topic);
        return res.map(p => ({ partition: p.partition, offset: p.high }));
    } catch {
        return [];
    }
}

export const sumOffsets = (offsets: PartitionOffsets) => offsets.reduce((s, p) => s + Number(p.offset), 0);

/** Messages in [start, end) across partitions. */
export function countBetween(start: PartitionOffsets, end: PartitionOffsets): number {
    const s = new Map(start.map(p => [p.partition, Number(p.offset)]));
    return end.reduce((sum, p) => sum + Math.max(0, Number(p.offset) - (s.get(p.partition) ?? 0)), 0);
}

export async function ensureTopics(admin: Admin, topics: string[], numPartitions = 6) {
    const existing = new Set(await admin.listTopics());
    const missing = topics.filter(t => !existing.has(t));
    if (missing.length === 0) return;
    await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map(topic => ({ topic, numPartitions, replicationFactor: 1 })),
    });
}

/** Pre-commits a fresh consumer group at exact offsets so it starts precisely there. */
export async function setGroupOffsets(admin: Admin, groupId: string, topic: string, offsets: PartitionOffsets) {
    if (offsets.length === 0) return;
    await admin.setOffsets({ groupId, topic, partitions: offsets.map(p => ({ partition: p.partition, offset: p.offset })) });
}

export async function deleteGroups(admin: Admin, predicate: (groupId: string) => boolean) {
    const { groups } = await admin.listGroups();
    const ids = groups.map(g => g.groupId).filter(predicate);
    if (ids.length === 0) return [];
    try {
        await admin.deleteGroups(ids);
    } catch {
        // Groups with active members can't be deleted; that's fine.
    }
    return ids;
}
