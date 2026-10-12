import express from 'express';
import cors from 'cors';
import path from 'path';
import fs from 'fs/promises';
import type sql from 'mssql';
import { config } from './config';
import { initializeDatabase, backfillChangeTokens, openPool } from './db';
import { createKafka } from './kafka';
import { addLog, getLogs, getLogsAfter, latestLogSeq, clearLogs } from './logs';
import { startTelemetry } from './telemetry';
import { RunManager, HttpError } from './runManager';
import { restartContainer, listContainers, containerName } from './docker';
import { waitForKafkaConnect } from './connect';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const kafka = createKafka('simulator-admin');
const admin = kafka.admin();
let pool: sql.ConnectionPool | null = null;
const manager = new RunManager(() => pool, admin);

const app = express();
app.use(cors());
// Saved runs include the full per-second history, which easily exceeds the 100kb default.
app.use(express.json({ limit: '50mb' }));

/** Wraps a handler so thrown HttpErrors become proper status codes. */
const handle = (fn: (req: express.Request, res: express.Response) => Promise<unknown> | unknown) =>
    async (req: express.Request, res: express.Response) => {
        try {
            const out = await fn(req, res);
            if (!res.headersSent) res.json(out ?? { ok: true });
        } catch (e) {
            const status = e instanceof HttpError ? e.status : 500;
            if (status >= 500) addLog(`${req.method} ${req.path} failed: ${String((e as Error).message || e)}`, 'error');
            if (!res.headersSent) res.status(status).json({ error: String((e as Error).message || e) });
        }
    };

// ---------------------------------------------------------------- stats

app.get('/api/stats', (req, res) => {
    res.json({ ...manager.getSnapshot(), logs: getLogs() });
});

let sseClients: { res: express.Response; lastSeq: number }[] = [];
app.get('/api/stats/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    // A new client gets the recent history once, then only new log lines.
    const client = { res, lastSeq: 0 };
    sseClients.push(client);
    req.on('close', () => { sseClients = sseClients.filter(c => c !== client); });
});

async function statsTick() {
    try {
        const snapshot = await manager.tick();
        for (const client of sseClients) {
            const newLogs = getLogsAfter(client.lastSeq);
            client.lastSeq = latestLogSeq();
            client.res.write(`data: ${JSON.stringify({ ...snapshot, newLogs })}\n\n`);
        }
    } catch (e) {
        console.error('Stats tick failed:', e);
    } finally {
        setTimeout(statsTick, 1000);
    }
}

// ---------------------------------------------------------------- run control

app.post('/api/simulate/start', handle(async (req, res) => {
    const runId = await manager.start(req.body || {});
    if (req.body?.waitUntilRunning) {
        while (manager.phase === 'provisioning') await sleep(500);
        if (manager.phase !== 'running') throw new HttpError(500, manager.lastError || 'Provisioning failed');
    }
    res.status(202).json({ message: 'Run accepted; provisioning the approach environment', runId });
}));
app.post('/api/simulate/update', handle(req => { manager.update(req.body || {}); return { message: 'Run updated' }; }));
app.post('/api/simulate/stop', handle(async (req, res) => {
    await manager.stop(!!req.body?.waitUntilIdle);
    res.status(202).json({ message: 'Stopping: draining pipeline, then tearing down' });
}));
app.post('/api/simulate/pause', handle(() => { manager.pause(); return { message: 'Simulation paused' }; }));
app.post('/api/simulate/resume', handle(() => { manager.resume(); return { message: 'Simulation resumed' }; }));
app.post('/api/simulate/verify', handle(() => manager.verify()));

app.post('/api/simulate/clean', handle(async () => { await manager.clean(); return { message: 'Cleaned' }; }));
app.post('/api/simulate/clean-cdc', handle(async () => { await manager.cleanCdc(); return { message: 'CDC cleanup completed' }; }));
app.post('/api/simulate/clean-kafka', handle(async () => { await manager.cleanKafka(); return { message: 'Kafka purge completed' }; }));

// ---------------------------------------------------------------- system

app.get('/health', (req, res) => {
    const ready = manager.phase !== 'starting';
    res.status(ready ? 200 : 503).json({
        status: ready ? 'ok' : 'starting',
        phase: manager.phase,
        workers: manager.readyWriters,
        dbConnected: !!pool?.connected,
    });
});

app.get('/api/system/health-check', handle(() => manager.healthCheck()));

let isReconnecting = false;
app.post('/api/system/reconnect', handle(async () => {
    if (isReconnecting) throw new HttpError(409, 'Reconnect already in progress');
    isReconnecting = true;
    try {
        addLog('Reconnecting to SQL Server and Kafka...');
        const old = pool;
        pool = await openPool(20, { label: 'SQL Server', retries: 2 });
        try { await old?.close(); } catch { /* ignore */ }
        try { await admin.disconnect(); } catch { /* ignore */ }
        await admin.connect();
        await manager.reconnectWriters();
        addLog('Reconnected.');
        return { message: 'Reconnected' };
    } finally {
        isReconnecting = false;
    }
}));

app.post('/api/system/induce-failure', handle(async () => {
    if (manager.phase !== 'running' && manager.phase !== 'paused') throw new HttpError(409, 'Start a run first');
    const name = config.containers.debezium;
    const found = (await listContainers()).find(c => containerName(c) === name);
    if (!found) throw new HttpError(404, `Container '${name}' not found`);
    addLog(`Inducing hard failure: restarting the ${name} container...`, 'warn');
    await restartContainer(found.Id);
    addLog(`${name} restarted; the connector resumes from its last committed offset (an unclean stop can cause duplicates - run Verify).`, 'warn');
    return { message: 'Hard failure induced' };
}));

app.post('/api/logs/clear', (req, res) => {
    clearLogs();
    res.json({ message: 'Logs cleared' });
});

// ---------------------------------------------------------------- saved results

app.post('/api/simulate/save-run', handle(async (req) => {
    await fs.mkdir(config.resultsDir, { recursive: true });
    const data = req.body;
    const runId = data.runId || Date.now().toString();
    await fs.writeFile(path.join(config.resultsDir, `${runId}.json`), JSON.stringify(data, null, 2));
    addLog(`Run ${runId} saved.`);
    return { message: 'Run saved', runId };
}));

app.get('/api/results', handle(async () => {
    await fs.mkdir(config.resultsDir, { recursive: true });
    const results = [];
    for (const file of await fs.readdir(config.resultsDir)) {
        if (!file.endsWith('.json')) continue;
        try {
            const data = JSON.parse(await fs.readFile(path.join(config.resultsDir, file), 'utf-8'));
            const s = data.summary || data;
            results.push({
                runId: data.runId, timestamp: data.timestamp, approach: data.approach,
                rps: s.finalTargetRps ?? data.rps, elapsedSec: s.elapsedSec ?? data.elapsedSec,
                recordsModified: s.recordsCommitted ?? data.recordsModified ?? 0, recordsFailed: s.recordsFailed ?? data.recordsFailed ?? 0,
                successRate: s.successRate ?? data.successRate, p95: s.p95 ?? data.p95, p99: s.p99 ?? data.p99,
            });
        } catch {
            // skip unreadable files
        }
    }
    results.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
    return results;
}));

app.get('/api/results/:id', async (req, res) => {
    try {
        const content = await fs.readFile(path.join(config.resultsDir, `${req.params.id}.json`), 'utf-8');
        res.json(JSON.parse(content));
    } catch {
        res.status(404).json({ error: 'Result not found' });
    }
});

// ---------------------------------------------------------------- startup / shutdown

async function connectKafkaAdmin() {
    for (;;) {
        try {
            await admin.connect();
            return;
        } catch (e) {
            addLog(`Kafka not reachable yet (${String((e as Error).message || e)}); retrying...`, 'warn');
            await sleep(5000);
        }
    }
}

async function init() {
    app.listen(config.port, () => console.log(`Simulator API listening on port ${config.port}`));
    statsTick();

    addLog('Initializing database...');
    pool = await initializeDatabase(msg => addLog(msg));
    startTelemetry(() => pool);

    await connectKafkaAdmin();
    addLog('Kafka admin connected.');

    // Undo whatever a previous session or an older version left running.
    addLog('Resetting pipeline to a neutral state...');
    try {
        await waitForKafkaConnect(120_000);
    } catch (e) {
        addLog(`${String((e as Error).message)}; connector cleanup will happen when a run starts.`, 'warn');
    }
    await manager.teardown(null);
    await backfillChangeTokens(pool);

    manager.spawnWriters();
    const deadline = Date.now() + 120_000;
    while (manager.readyWriters < config.load.writerThreads && Date.now() < deadline) await sleep(250);
    manager.markReady();
    addLog(`Simulator ready (${manager.readyWriters}/${config.load.writerThreads} writer threads).`);
}

let shuttingDown = false;
async function shutdown(signal: string) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n${signal} received, shutting down...`);
    await manager.shutdown().catch(() => undefined);
    try { await admin.disconnect(); } catch { /* ignore */ }
    try { await pool?.close(); } catch { /* ignore */ }
    process.exit(0);
}
process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

init().catch((e) => {
    console.error('Fatal startup error:', e);
    process.exit(1);
});
