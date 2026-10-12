/**
 * Background collector for SQL Server DMVs and container CPU/memory. Runs sequentially
 * (the next poll starts only after the previous one finished), so slow polls can never pile up,
 * and API/SSE handlers just read the cached values.
 */
import type sql from 'mssql';
import { getDBStats, EMPTY_DB_STATS, type DbStats } from './db';
import { listContainers, containerStatsOnce, inspectContainer, containerName } from './docker';
import { addLog } from './logs';

export interface ContainerStat {
    name: string;
    cpu: string;
    cpuCores: string[];
    cpuLimit: number;
    mem: string;
    cacheMem: string;
}

let dbStats: DbStats = { ...EMPTY_DB_STATS };
let dbStatsOk = false;
let containerStats: ContainerStat[] = [];
const prevCpu = new Map<string, { total: number; system: number }>();
const cpuLimits = new Map<string, number>();
let dockerWarned = false;
let dbWarned = false;

export const getCachedDbStats = () => dbStats;
export const getCachedContainerStats = () => containerStats;
export const isDbStatsHealthy = () => dbStatsOk;

async function cpuLimitFor(id: string, hostCpus: number): Promise<number> {
    if (cpuLimits.has(id)) return cpuLimits.get(id)!;
    let limit = hostCpus;
    try {
        const info = await inspectContainer(id);
        const nano = Number(info?.HostConfig?.NanoCpus || 0);
        const quota = Number(info?.HostConfig?.CpuQuota || 0);
        const period = Number(info?.HostConfig?.CpuPeriod || 0);
        if (nano > 0) limit = nano / 1e9;
        else if (quota > 0 && period > 0) limit = quota / period;
    } catch { /* keep host cpus */ }
    cpuLimits.set(id, limit);
    return limit;
}

export function splitAcrossCores(cpuPercent: number, limitCpus: number): string[] {
    const cores: string[] = [];
    let remaining = cpuPercent;
    const n = Math.max(1, Math.ceil(limitCpus));
    for (let i = 0; i < n; i++) {
        const capacity = i === Math.floor(limitCpus) && limitCpus % 1 !== 0 ? (limitCpus % 1) * 100 : 100;
        const used = Math.min(remaining, capacity);
        cores.push(used.toFixed(2) + '%');
        remaining = Math.max(0, remaining - used);
    }
    return cores;
}

async function collectContainers(): Promise<ContainerStat[]> {
    const containers = (await listContainers()).filter(c => containerName(c) !== 'kafka-ui');
    const results = await Promise.all(containers.map(async (c) => {
        try {
            const stat = await containerStatsOnce(c.Id);
            const total = Number(stat?.cpu_stats?.cpu_usage?.total_usage || 0);
            const system = Number(stat?.cpu_stats?.system_cpu_usage || 0);
            const onlineCpus = Number(stat?.cpu_stats?.online_cpus || stat?.cpu_stats?.cpu_usage?.percpu_usage?.length || 1);
            const prev = prevCpu.get(c.Id);
            prevCpu.set(c.Id, { total, system });
            let cpu = 0;
            if (prev && system > prev.system && total >= prev.total) {
                cpu = ((total - prev.total) / (system - prev.system)) * onlineCpus * 100;
            }
            const limit = await cpuLimitFor(c.Id, onlineCpus);

            let mem = 0, cacheMem = 0;
            const ms = stat?.memory_stats;
            if (ms?.limit) {
                const cache = Number(ms.stats?.inactive_file ?? ms.stats?.cache ?? 0);
                mem = (Math.max(0, Number(ms.usage || 0) - cache) / ms.limit) * 100;
                cacheMem = (cache / ms.limit) * 100;
            }
            return {
                name: containerName(c),
                cpu: cpu.toFixed(2) + '%',
                cpuCores: splitAcrossCores(cpu, limit),
                cpuLimit: limit,
                mem: mem.toFixed(2) + '%',
                cacheMem: cacheMem.toFixed(2) + '%',
            } as ContainerStat;
        } catch {
            return null;
        }
    }));
    return results.filter((r): r is ContainerStat => r !== null).sort((a, b) => a.name.localeCompare(b.name));
}

export function startTelemetry(getPool: () => sql.ConnectionPool | null, intervalMs = 1000) {
    const loop = async () => {
        const started = Date.now();
        const pool = getPool();
        const [db, containers] = await Promise.allSettled([
            pool ? getDBStats(pool) : Promise.reject(new Error('no pool')),
            collectContainers(),
        ]);
        if (db.status === 'fulfilled') {
            dbStats = db.value;
            dbStatsOk = true;
            dbWarned = false;
        } else {
            dbStatsOk = false;
            if (!dbWarned && pool) {
                addLog(`DB stats unavailable: ${String((db.reason as Error)?.message || db.reason)}`, 'warn');
                dbWarned = true;
            }
        }
        if (containers.status === 'fulfilled') {
            containerStats = containers.value;
            dockerWarned = false;
        } else {
            containerStats = [];
            if (!dockerWarned) {
                addLog(`Container stats unavailable (Docker API): ${String((containers.reason as Error)?.message || containers.reason)}`, 'warn');
                dockerWarned = true;
            }
        }
        setTimeout(loop, Math.max(0, intervalMs - (Date.now() - started)));
    };
    void loop();
}
