/** Kafka Connect (Debezium) REST client and Flink REST/SQL helpers. */
import { config } from './config';
import { CONNECTOR_NAME_PREFIX, LEGACY_CONNECTOR_NAMES, FLINK_JOB_NAME } from './approaches';
import { execInContainer } from './docker';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function http(method: string, url: string, body?: unknown, timeoutMs = 15_000) {
    const res = await fetch(url, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let json: any = undefined;
    try { json = text ? JSON.parse(text) : undefined; } catch { /* not JSON */ }
    return { status: res.status, ok: res.ok, json, text };
}

// ---------------------------------------------------------------- Debezium / Kafka Connect

export async function listConnectors(): Promise<string[]> {
    const res = await http('GET', `${config.debeziumUrl}/connectors`);
    if (!res.ok) throw new Error(`Kafka Connect list failed (${res.status}): ${res.text}`);
    return res.json as string[];
}

export const isSimConnector = (name: string) => name.startsWith(CONNECTOR_NAME_PREFIX) || LEGACY_CONNECTOR_NAMES.includes(name);

export async function deleteConnector(name: string) {
    const res = await http('DELETE', `${config.debeziumUrl}/connectors/${encodeURIComponent(name)}`);
    if (!res.ok && res.status !== 404) throw new Error(`Delete connector ${name} failed (${res.status}): ${res.text}`);
}

/** Removes every connector this simulator owns (current naming and legacy names). */
export async function deleteSimConnectors(): Promise<string[]> {
    const names = (await listConnectors()).filter(isSimConnector);
    for (const n of names) await deleteConnector(n);
    // Deletion is asynchronous in Connect; wait until they are gone so they stop capturing.
    for (let i = 0; i < 30; i++) {
        const remaining = (await listConnectors()).filter(isSimConnector);
        if (remaining.length === 0) break;
        await sleep(500);
    }
    return names;
}

export async function createConnector(name: string, cfg: Record<string, string>) {
    const res = await http('POST', `${config.debeziumUrl}/connectors`, { name, config: cfg }, 30_000);
    if (!res.ok) throw new Error(`Register connector ${name} failed (${res.status}): ${res.text}`);
}

export async function connectorStatus(name: string): Promise<any | null> {
    const res = await http('GET', `${config.debeziumUrl}/connectors/${encodeURIComponent(name)}/status`);
    return res.ok ? res.json : null;
}

export async function waitForConnectorRunning(name: string, timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    let last: any = null;
    while (Date.now() < deadline) {
        last = await connectorStatus(name);
        const task = last?.tasks?.[0];
        if (task?.state === 'FAILED') throw new Error(`Connector ${name} task failed: ${String(task.trace || '').split('\n')[0]}`);
        if (last?.connector?.state === 'RUNNING' && task?.state === 'RUNNING') return;
        await sleep(500);
    }
    throw new Error(`Connector ${name} not RUNNING after ${timeoutMs}ms (last status: ${JSON.stringify(last)})`);
}

export async function waitForKafkaConnect(timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        try {
            await listConnectors();
            return;
        } catch {
            await sleep(2000);
        }
    }
    throw new Error(`Kafka Connect not reachable at ${config.debeziumUrl}`);
}

// ---------------------------------------------------------------- Flink

export interface FlinkJob { jid: string; name: string; state: string }

const ACTIVE_JOB_STATES = new Set(['CREATED', 'RUNNING', 'RESTARTING', 'INITIALIZING', 'RECONCILING', 'FAILING', 'CANCELLING']);

export async function listFlinkJobs(): Promise<FlinkJob[]> {
    const res = await http('GET', `${config.flinkUrl}/jobs/overview`);
    if (!res.ok) throw new Error(`Flink jobs overview failed (${res.status})`);
    return (res.json?.jobs || []) as FlinkJob[];
}

/** Cancels every active job on the simulator's Flink cluster (it is dedicated to this simulator). */
export async function cancelFlinkJobs(): Promise<FlinkJob[]> {
    const active = (await listFlinkJobs()).filter(j => ACTIVE_JOB_STATES.has(j.state) && j.state !== 'CANCELLING');
    for (const j of active) {
        await http('PATCH', `${config.flinkUrl}/jobs/${j.jid}?mode=cancel`);
    }
    for (let i = 0; i < 60; i++) {
        const still = (await listFlinkJobs()).filter(j => ACTIVE_JOB_STATES.has(j.state));
        if (still.length === 0) break;
        await sleep(500);
    }
    return active;
}

/** Submits the Approach 3 interval-join job via the SQL client inside the JobManager container. */
export async function submitApproach3Job(timeoutMs: number) {
    const { exitCode, output } = await execInContainer(
        config.containers.flinkJobManager,
        ['/opt/flink/bin/sql-client.sh', '-f', '/opt/flink/sql/approach3.sql'],
        timeoutMs,
    );
    if (exitCode !== 0 || /\[ERROR\]/.test(output)) {
        throw new Error(`Flink SQL submission failed (exit ${exitCode}): ${output.split('\n').filter(l => /ERROR|Exception|Caused by/.test(l)).slice(0, 5).join(' | ') || output.slice(-500)}`);
    }
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        const job = (await listFlinkJobs()).find(j => j.name === FLINK_JOB_NAME && ACTIVE_JOB_STATES.has(j.state));
        if (job?.state === 'RUNNING') return job;
        await sleep(1000);
    }
    throw new Error(`Flink job ${FLINK_JOB_NAME} did not reach RUNNING`);
}
