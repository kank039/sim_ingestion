/**
 * Owns the lifecycle of a simulation run:
 *
 *   idle -> provisioning -> running <-> paused -> draining -> idle
 *
 * Provisioning makes the environment match exactly one approach (trigger, CDC tables, a fresh
 * Debezium connector, Flink job, consumer threads) and tears down anything a previous approach
 * left behind, so no other approach's machinery consumes resources during a measurement.
 */
import type sql from 'mssql';
import type { Admin } from 'kafkajs';
import type { Worker } from 'worker_threads';
import { config } from './config';
import {
    APPROACHES, WRITE_TABLES, HISTORY_TOPIC_PREFIX, connectorName, buildConnectorConfig, requiredTopics,
    isApproachId, type ApproachId, type ApproachProfile,
} from './approaches';
import {
    setCdcTables, setTriggerEnabled, truncateTables, ensureReferenceData, startCdcCaptureJob,
    getCdcTrackedTables, isTriggerEnabled,
} from './db';
import {
    deleteSimConnectors, createConnector, waitForConnectorRunning, connectorStatus, listConnectors, isSimConnector,
    cancelFlinkJobs, submitApproach3Job, listFlinkJobs,
} from './connect';
import { endOffsets, sumOffsets, countBetween, ensureTopics, deleteGroups, type PartitionOffsets } from './kafka';
import { LatencyHistogram, type SerializedHistogram } from './histogram';
import { splitRate } from './pacer';
import { buildWriteBatch, executeWrite } from './writer';
import { spawnWorker, stopWorker } from './workers';
import { addLog } from './logs';
import { getCachedDbStats, getCachedContainerStats } from './telemetry';
import { verifyTopic, type VerifyResult } from './verify';

export type Phase = 'starting' | 'idle' | 'provisioning' | 'running' | 'paused' | 'draining' | 'cleaning';

export interface RunParams {
    approach: ApproachId;
    rps: number;
    gradual: boolean;
    endRps: number;
    rampStepRps: number;
    timeoutMs: number;
    insertsOnly: boolean;
    cardinality: number;
    weights: { insert: number; update: number; delete: number };
    numSubscribers: number;
    resetData: boolean;
}

export class HttpError extends Error {
    constructor(public status: number, message: string) { super(message); }
}

const ROLLING_WINDOWS = 5;
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const round = (n: number, d = 0) => { const f = 10 ** d; return Math.round(n * f) / f; };

function newCounters() {
    return { issued: 0, onTime: 0, late: 0, failed: 0, dropped: 0, partialWrites: 0, ops: { c: 0, u: 0, d: 0 } };
}

function newHists() {
    return {
        db: new LatencyHistogram(),
        queue: new LatencyHistogram(),
        pipeline: new LatencyHistogram(),
        lookup: new LatencyHistogram(),
        enrich: new LatencyHistogram(),
        e2e: new LatencyHistogram(),
    };
}

interface RunState {
    runId: string;
    params: RunParams;
    profile: ApproachProfile;
    connector: string | null;
    startOffsets: PartitionOffsets;
    groupPrefix: string;
    startedAt: number;
    stopRequestedAt: number | null;
    pausedAt: number | null;
    pausedMs: number;
    targetRps: number;
    totals: ReturnType<typeof newCounters>;
    delivered: number;
    tombstones: number;
    unparsable: number;
    enrichmentMisses: number;
    runHists: ReturnType<typeof newHists>;
    windowHists: ReturnType<typeof newHists>;
    consumer: { consumed: number; failed: number; skipped: number; misses: number; readySubscribers: number; positions: Map<number, Record<number, string>> };
    flawAlert: string | null;
    measuring: boolean;
}

export class RunManager {
    phase: Phase = 'starting';
    phaseDetail = 'Initializing';
    lastError: string | null = null;
    run: RunState | null = null;
    lastRunSummary: any = null;
    lastVerify: VerifyResult | null = null;

    private writers: Worker[] = [];
    private writerReady: boolean[] = [];
    private tracker: Worker | null = null;
    private consumers: Worker[] = [];
    private rampTimer: NodeJS.Timeout | null = null;
    private abortRequested = false;
    private snapshot: any = null;
    private lastEnd: PartitionOffsets = [];
    /** Last few 1s windows: pipeline latencies arrive in CDC-poll bursts, so they are shown over 5s. */
    private recentHists: ReturnType<typeof newHists>[] = [];
    private rateSamples: { t: number; runId: string; issued: number; committed: number; consumed: number }[] = [];

    constructor(private getPool: () => sql.ConnectionPool | null, private admin: Admin) {}

    // ------------------------------------------------------------------ writer threads

    spawnWriters() {
        for (let i = 0; i < config.load.writerThreads; i++) {
            const w = spawnWorker('simulation', { concurrency: config.load.concurrencyPerThread, queueLimit: config.load.queueLimitPerThread });
            this.writerReady[i] = false;
            w.on('message', (m) => this.onWriterMessage(i, m));
            w.on('error', (e: Error) => addLog(`Writer thread ${i + 1} crashed: ${e.message}`, 'error'));
            this.writers.push(w);
        }
    }

    get readyWriters() {
        return this.writerReady.filter(Boolean).length;
    }

    private onWriterMessage(i: number, m: any) {
        if (m.type === 'ready') {
            if (!this.writerReady[i]) addLog(`Writer thread ${i + 1} connected (${this.readyWriters + 1}/${this.writers.length})`);
            this.writerReady[i] = true;
        } else if (m.type === 'error') {
            this.writerReady[i] = false;
            addLog(`Writer thread ${i + 1}: ${m.message}`, 'warn');
        } else if (m.type === 'stats') {
            const r = this.run;
            if (!r || !r.measuring && !m.final) return;
            const t = r.totals;
            t.issued += m.issued; t.onTime += m.onTime; t.late += m.late;
            t.failed += m.failed; t.dropped += m.dropped; t.partialWrites += m.partialWrites;
            t.ops.c += m.ops.c; t.ops.u += m.ops.u; t.ops.d += m.ops.d;
            r.runHists.db.merge(m.dbHist); r.windowHists.db.merge(m.dbHist);
            r.runHists.queue.merge(m.queueHist); r.windowHists.queue.merge(m.queueHist);
            for (const e of m.errors as { message: string; count: number }[]) {
                addLog(`Write error x${e.count}: ${e.message}`, 'error');
            }
        }
    }

    private broadcast(msg: any) {
        for (const w of this.writers) w.postMessage(msg);
    }

    private setTargetRps(rps: number) {
        if (!this.run) return;
        this.run.targetRps = rps;
        const shares = splitRate(rps, this.writers.length);
        this.writers.forEach((w, i) => w.postMessage({ type: 'rate', rate: shares[i] }));
    }

    // ------------------------------------------------------------------ public API

    validate(body: any, partial = false): Partial<RunParams> {
        const out: Partial<RunParams> = {};
        const num = (k: string, min: number, max: number) => {
            if (body[k] === undefined || body[k] === null || body[k] === '') return undefined;
            const v = Number(body[k]);
            if (!Number.isFinite(v) || v < min || v > max) throw new HttpError(400, `${k} must be a number between ${min} and ${max}`);
            return v;
        };
        if (body.approach !== undefined) {
            const a = Number(body.approach);
            if (!isApproachId(a)) throw new HttpError(400, 'approach must be 1-5');
            out.approach = a;
        } else if (!partial) throw new HttpError(400, 'approach is required');
        const rps = num('rps', 0, 100_000); if (rps !== undefined) out.rps = rps;
        const endRps = num('endRps', 0, 100_000); if (endRps !== undefined) out.endRps = endRps;
        const step = num('rampStepRps', 1, 100_000); if (step !== undefined) out.rampStepRps = step;
        const timeoutMs = num('timeoutMs', 1, 600_000); if (timeoutMs !== undefined) out.timeoutMs = timeoutMs;
        const card = num('cardinality', 1, config.load.maxCardinality); if (card !== undefined) out.cardinality = Math.floor(card);
        const subs = num('numSubscribers', 0, 200); if (subs !== undefined) out.numSubscribers = Math.floor(subs);
        if (body.gradual !== undefined) out.gradual = !!body.gradual;
        if (body.insertsOnly !== undefined) out.insertsOnly = !!body.insertsOnly;
        if (body.resetData !== undefined) out.resetData = !!body.resetData;
        const w = ['insertWeight', 'updateWeight', 'deleteWeight'].map(k => num(k, 0, 1));
        if (w.some(x => x !== undefined)) {
            out.weights = { insert: w[0] ?? 0, update: w[1] ?? 0, delete: w[2] ?? 0 };
        }
        return out;
    }

    async start(body: any): Promise<string> {
        if (this.phase === 'starting') throw new HttpError(503, 'Simulator is still starting up');
        if (this.phase === 'provisioning' || this.phase === 'draining' || this.phase === 'cleaning') {
            throw new HttpError(409, `Cannot start while ${this.phase}`);
        }
        const v = this.validate(body);
        const params: RunParams = {
            approach: v.approach!,
            rps: v.rps ?? 10,
            gradual: v.gradual ?? false,
            endRps: v.endRps ?? v.rps ?? 10,
            rampStepRps: v.rampStepRps ?? 50,
            timeoutMs: v.timeoutMs ?? 3000,
            insertsOnly: v.insertsOnly ?? true,
            cardinality: v.cardinality ?? 100,
            weights: v.weights ?? { insert: 0.5, update: 0.3, delete: 0.2 },
            numSubscribers: v.numSubscribers ?? (v.approach === 5 ? 5 : 0),
            resetData: v.resetData ?? true,
        };
        if (params.approach === 5 && params.numSubscribers < 1) throw new HttpError(400, 'Approach 5 needs at least 1 subscriber');

        if (this.phase === 'running' || this.phase === 'paused') {
            addLog('A run is active; stopping it before starting the new one.');
            await this.stop(true);
        }
        const runId = Date.now().toString();
        this.phase = 'provisioning';
        this.lastError = null;
        this.abortRequested = false;
        void this.provision(runId, params);
        return runId;
    }

    update(body: any) {
        const r = this.run;
        if (!r || (this.phase !== 'running' && this.phase !== 'paused')) throw new HttpError(409, 'No active run to update');
        const v = this.validate(body, true);
        if (v.approach !== undefined && v.approach !== r.params.approach) throw new HttpError(409, 'Changing the approach requires a new run');
        if (v.cardinality !== undefined && v.cardinality !== r.params.cardinality) throw new HttpError(409, 'Changing cardinality requires a new run');
        if (v.numSubscribers !== undefined && r.params.approach === 5 && v.numSubscribers !== r.params.numSubscribers) {
            throw new HttpError(409, 'Changing the number of subscribers requires a new run');
        }
        Object.assign(r.params, {
            ...(v.rps !== undefined && { rps: v.rps }),
            ...(v.endRps !== undefined && { endRps: v.endRps }),
            ...(v.rampStepRps !== undefined && { rampStepRps: v.rampStepRps }),
            ...(v.gradual !== undefined && { gradual: v.gradual }),
            ...(v.timeoutMs !== undefined && { timeoutMs: v.timeoutMs }),
            ...(v.insertsOnly !== undefined && { insertsOnly: v.insertsOnly }),
            ...(v.weights !== undefined && { weights: v.weights }),
        });
        this.broadcast({ type: 'update', timeoutMs: r.params.timeoutMs, insertsOnly: r.params.insertsOnly, weights: r.params.weights });
        if (v.rps !== undefined || v.gradual !== undefined || v.endRps !== undefined) {
            this.stopRamp();
            this.setTargetRps(r.params.rps);
            this.startRampIfNeeded();
        }
        addLog(`Run updated: target ${r.targetRps} RPS, timeout ${r.params.timeoutMs}ms${r.params.gradual ? `, ramp to ${r.params.endRps}` : ''}`);
    }

    pause() {
        if (this.phase !== 'running' || !this.run) throw new HttpError(409, 'Not running');
        this.phase = 'paused';
        this.run.pausedAt = Date.now();
        this.broadcast({ type: 'pause' });
        addLog('Simulation paused (load generation and ramp halted; pipeline keeps draining).');
    }

    resume() {
        if (this.phase !== 'paused' || !this.run) throw new HttpError(409, 'Not paused');
        if (this.run.pausedAt) this.run.pausedMs += Date.now() - this.run.pausedAt;
        this.run.pausedAt = null;
        this.phase = 'running';
        this.broadcast({ type: 'resume' });
        addLog('Simulation resumed.');
    }

    /** Stops load, waits for the pipeline to deliver what was committed, records the summary, tears down. */
    async stop(awaitCompletion = false): Promise<void> {
        if (this.phase === 'provisioning') {
            this.abortRequested = true;
            addLog('Stop requested during provisioning; aborting.');
            return;
        }
        if (this.phase !== 'running' && this.phase !== 'paused') throw new HttpError(409, 'Not running');
        const done = this.drainAndTeardown();
        if (awaitCompletion) await done;
    }

    private async drainAndTeardown() {
        const r = this.run!;
        if (r.pausedAt) { r.pausedMs += Date.now() - r.pausedAt; r.pausedAt = null; }
        r.stopRequestedAt = Date.now();
        this.phase = 'draining';
        this.phaseDetail = 'Waiting for in-flight writes';
        this.stopRamp();
        addLog('Stopping load generation...');
        await Promise.all(this.writers.map(w => new Promise<void>(resolve => {
            const timer = setTimeout(resolve, r.params.timeoutMs + 40_000);
            const onMsg = (m: any) => { if (m.type === 'stopped') { clearTimeout(timer); w.off('message', onMsg); resolve(); } };
            w.on('message', onMsg);
            w.postMessage({ type: 'stop' });
        })));

        this.phaseDetail = 'Waiting for the pipeline to deliver committed changes';
        const deadline = Date.now() + config.timeouts.drainMs;
        let lastProgress = Date.now();
        let lastDelivered = -1;
        while (Date.now() < deadline) {
            const committed = r.totals.onTime + r.totals.late;
            const consumerDone = !r.profile.consumers || this.consumerLag() === 0;
            if (r.delivered >= committed && consumerDone) break;
            if (r.delivered !== lastDelivered) { lastDelivered = r.delivered; lastProgress = Date.now(); }
            if (Date.now() - lastProgress > 15_000) break;
            await sleep(500);
        }
        r.measuring = false;
        this.lastRunSummary = this.buildSummary();
        const s = this.lastRunSummary;
        addLog(`Run ${r.runId} finished: ${s.recordsCommitted} committed, ${s.recordsInKafka} delivered on ${r.profile.trackedTopic}, lag ${s.lag}.`);

        this.phaseDetail = 'Tearing down pipeline';
        await this.teardown(r);
        this.phase = 'idle';
        this.phaseDetail = '';
    }

    // ------------------------------------------------------------------ provisioning

    private checkAbort() {
        if (this.abortRequested) throw new Error('Aborted by stop request');
    }

    private step(detail: string) {
        this.checkAbort();
        this.phaseDetail = detail;
        addLog(`[provision] ${detail}`);
    }

    private async provision(runId: string, params: RunParams) {
        const profile = APPROACHES[params.approach];
        const pool = this.getPool();
        const run: RunState = {
            runId, params, profile, connector: null, startOffsets: [], groupPrefix: `sim-run-${runId}`,
            startedAt: 0, stopRequestedAt: null, pausedAt: null, pausedMs: 0, targetRps: params.rps,
            totals: newCounters(), delivered: 0, tombstones: 0, unparsable: 0, enrichmentMisses: 0,
            runHists: newHists(), windowHists: newHists(),
            consumer: { consumed: 0, failed: 0, skipped: 0, misses: 0, readySubscribers: 0, positions: new Map() },
            flawAlert: null, measuring: false,
        };
        this.run = run;
        addLog(`Provisioning Approach ${profile.id} (${profile.name}) for run ${runId}`);
        try {
            if (!pool) throw new Error('No database connection');

            this.step('Waiting for writer threads');
            const writerDeadline = Date.now() + 60_000;
            while (this.readyWriters < this.writers.length && Date.now() < writerDeadline) await sleep(250);
            if (this.readyWriters < this.writers.length) throw new Error(`Only ${this.readyWriters}/${this.writers.length} writer threads connected to SQL Server`);

            this.step('Tearing down anything left from previous approaches');
            await this.teardown(null);

            if (params.resetData) {
                this.step('Truncating write tables');
                await truncateTables(pool, WRITE_TABLES);
            }

            this.step(`Ensuring reference data for cardinality ${params.cardinality.toLocaleString()}`);
            const t0 = Date.now();
            await ensureReferenceData(pool, params.cardinality);
            if (Date.now() - t0 > 2000) addLog(`Reference data ready in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

            this.step(`Configuring database: trigger ${profile.trigger ? 'ON' : 'OFF'}, CDC on [${profile.cdcTables.join(', ')}]`);
            await setTriggerEnabled(pool, profile.trigger);
            await setCdcTables(pool, profile.cdcTables, msg => addLog(msg));
            await startCdcCaptureJob(pool);

            this.step('Creating Kafka topics');
            await ensureTopics(this.admin, requiredTopics(profile));

            const name = connectorName(profile, runId);
            this.step(`Registering Debezium connector ${name}`);
            await createConnector(name, buildConnectorConfig(profile, name));
            run.connector = name;
            await waitForConnectorRunning(name, config.timeouts.connectorReadyMs);

            if (profile.flinkJob) {
                this.step('Submitting Flink interval-join job');
                await submitApproach3Job(180_000);
            }

            this.step('Probing pipeline end-to-end');
            await this.probe(pool, profile);

            run.startOffsets = await endOffsets(this.admin, profile.trackedTopic);
            this.lastEnd = run.startOffsets;

            this.step('Starting delivery tracker');
            await this.startTracker(run);

            if (profile.consumers) {
                this.step(`Starting ${params.numSubscribers} subscriber consumers`);
                await this.startConsumers(run);
            }

            this.step('Starting load generation');
            run.measuring = true;
            run.startedAt = Date.now();
            this.broadcast({
                type: 'configure', approach: params.approach, cardinality: params.cardinality,
                timeoutMs: params.timeoutMs, insertsOnly: params.insertsOnly, weights: params.weights,
            });
            const shares = splitRate(params.rps, this.writers.length);
            this.writers.forEach((w, i) => w.postMessage({ type: 'start', rate: shares[i] }));
            this.phase = 'running';
            this.phaseDetail = '';
            this.startRampIfNeeded();
            addLog(`Run ${runId} started: Approach ${profile.id} at ${params.rps} RPS${profile.consumers ? ` with ${params.numSubscribers} subscribers` : ''}`);
        } catch (e) {
            const msg = String((e as Error).message || e);
            this.lastError = msg;
            addLog(`Provisioning failed: ${msg}`, 'error');
            this.phaseDetail = 'Cleaning up after failed provisioning';
            await this.teardown(run).catch(() => undefined);
            this.run = null;
            this.phase = 'idle';
            this.phaseDetail = '';
        }
    }

    /** One real write through the approach's path; waits until it shows up on the tracked topic. */
    private async probe(pool: sql.ConnectionPool, profile: ApproachProfile) {
        const before = sumOffsets(await endOffsets(this.admin, profile.trackedTopic));
        const batchSql = buildWriteBatch(profile);
        const deadline = Date.now() + config.timeouts.probeMs;
        let lastWrite = 0;
        let nudged = false;
        while (Date.now() < deadline) {
            this.checkAbort();
            // Re-probe periodically: a consumer (e.g. Flink's source) may start from "latest"
            // only after its first fetch, so an early probe can legitimately be skipped.
            if (Date.now() - lastWrite > 10_000) {
                await executeWrite(pool, profile, batchSql, { batchId: 1, amount: '0.00', requestedOp: 'c' });
                lastWrite = Date.now();
            }
            if (sumOffsets(await endOffsets(this.admin, profile.trackedTopic)) > before) return;
            if (!nudged && Date.now() > deadline - config.timeouts.probeMs / 2) {
                await startCdcCaptureJob(pool);
                nudged = true;
            }
            await sleep(500);
        }
        throw new Error(`Probe write did not reach ${profile.trackedTopic} within ${config.timeouts.probeMs / 1000}s`);
    }

    private startTracker(run: RunState): Promise<void> {
        return new Promise((resolve, reject) => {
            const t = spawnWorker('tracker-worker', {
                topic: run.profile.trackedTopic,
                kind: run.profile.trackedKind,
                groupId: `${run.groupPrefix}-tracker`,
                startOffsets: run.startOffsets,
                checkEnrichment: run.profile.smt,
            });
            this.tracker = t;
            const timer = setTimeout(() => reject(new Error('Tracker did not become ready')), 45_000);
            t.on('message', (m) => {
                if (m.type === 'tracker-ready') { clearTimeout(timer); resolve(); }
                else if (m.type === 'tracker-error') { clearTimeout(timer); reject(new Error(`Tracker: ${m.message}`)); }
                else if (m.type === 'tracker-stats' && this.run === run) {
                    run.delivered += m.delivered;
                    run.tombstones += m.tombstones; run.unparsable += m.unparsable;
                    run.enrichmentMisses += m.enrichmentMisses;
                    run.runHists.pipeline.merge(m.latency); run.windowHists.pipeline.merge(m.latency);
                    run.runHists.lookup.merge(m.lookup); run.windowHists.lookup.merge(m.lookup);
                }
            });
            t.on('error', (e: Error) => addLog(`Tracker crashed: ${e.message}`, 'error'));
        });
    }

    private async startConsumers(run: RunState) {
        const n = run.params.numSubscribers;
        const threads = Math.min(config.load.consumerThreads, n);
        const assignments: number[][] = Array.from({ length: threads }, () => []);
        for (let s = 0; s < n; s++) assignments[s % threads].push(s);

        const ready = assignments.map((subs, i) => new Promise<void>((resolve, reject) => {
            const w = spawnWorker('consumer-worker', {
                workerId: i, subscribers: subs, groupPrefix: run.groupPrefix,
                topic: run.profile.trackedTopic, startOffsets: run.startOffsets,
            });
            this.consumers.push(w);
            const timer = setTimeout(() => reject(new Error(`Consumer thread ${i + 1} did not become ready`)), 60_000);
            w.on('message', (m) => {
                if (m.type === 'consumer-ready') {
                    clearTimeout(timer);
                    run.consumer.readySubscribers += m.subscribers;
                    addLog(`Consumer thread ${i + 1} ready (${run.consumer.readySubscribers}/${n} subscribers)`);
                    resolve();
                } else if (m.type === 'consumer-error') {
                    clearTimeout(timer);
                    reject(new Error(`Consumer thread ${i + 1}: ${m.error}`));
                } else if (m.type === 'consumer-stats' && this.run === run) {
                    run.consumer.consumed += m.consumed;
                    run.consumer.failed += m.failed; run.consumer.skipped += m.skipped; run.consumer.misses += m.misses;
                    run.runHists.enrich.merge(m.enrich); run.windowHists.enrich.merge(m.enrich);
                    run.runHists.e2e.merge(m.e2e); run.windowHists.e2e.merge(m.e2e);
                    for (const [sub, parts] of Object.entries(m.positions as Record<string, Record<number, string>>)) {
                        run.consumer.positions.set(Number(sub), parts);
                    }
                }
            });
            w.on('error', (e: Error) => addLog(`Consumer thread ${i + 1} crashed: ${e.message}`, 'error'));
        }));
        await Promise.all(ready);
    }

    private consumerLag(): number {
        const r = this.run;
        if (!r || !r.profile.consumers) return 0;
        let lag = 0;
        for (let s = 0; s < r.params.numSubscribers; s++) {
            const pos = r.consumer.positions.get(s) || {};
            for (const p of this.lastEnd) {
                const consumed = Number(pos[p.partition] ?? r.startOffsets.find(x => x.partition === p.partition)?.offset ?? 0);
                lag += Math.max(0, Number(p.offset) - consumed);
            }
        }
        return lag;
    }

    /**
     * Returns the environment to a neutral state: no simulator connectors, no Flink jobs, no
     * tracker/consumer threads, trigger disabled and CDC off on every simulator table.
     */
    async teardown(run: RunState | null) {
        const pool = this.getPool();
        const warn = (what: string, e: unknown) => addLog(`Teardown: ${what}: ${String((e as Error)?.message || e)}`, 'warn');

        if (this.tracker) {
            await stopWorker(this.tracker, 'tracker-stopped', 10_000).catch(e => warn('tracker', e));
            this.tracker = null;
        }
        if (this.consumers.length) {
            const cs = this.consumers;
            this.consumers = [];
            await Promise.all(cs.map(c => stopWorker(c, 'consumer-stopped', 15_000).catch(e => warn('consumer thread', e))));
            addLog(`Stopped ${cs.length} consumer thread(s).`);
        }
        try {
            const removed = await deleteSimConnectors();
            if (removed.length) addLog(`Removed Debezium connector(s): ${removed.join(', ')}`);
        } catch (e) { warn('Debezium connectors', e); }
        try {
            const cancelled = await cancelFlinkJobs();
            if (cancelled.length) addLog(`Cancelled Flink job(s): ${cancelled.map(j => j.name).join(', ')}`);
        } catch (e) { warn('Flink jobs', e); }
        if (pool) {
            try { await setTriggerEnabled(pool, false); } catch (e) { warn('trigger', e); }
            try { await setCdcTables(pool, [], msg => addLog(msg)); } catch (e) { warn('CDC tables', e); }
        }
        try {
            const topics = (await this.admin.listTopics()).filter(t => t.startsWith(HISTORY_TOPIC_PREFIX));
            if (topics.length) await this.admin.deleteTopics({ topics });
        } catch (e) { warn('schema history topics', e); }
        if (run) {
            try { await deleteGroups(this.admin, g => g.startsWith(run.groupPrefix)); } catch (e) { warn('consumer groups', e); }
        }
    }

    // ------------------------------------------------------------------ ramp

    private startRampIfNeeded() {
        const r = this.run;
        if (!r || !r.params.gradual || r.params.endRps <= r.targetRps) return;
        this.rampTimer = setInterval(() => {
            if (!this.run || this.phase === 'paused') return;
            if (this.phase !== 'running') { this.stopRamp(); return; }
            const next = Math.min(this.run.params.endRps, this.run.targetRps + this.run.params.rampStepRps);
            this.setTargetRps(next);
            if (next >= this.run.params.endRps) this.stopRamp();
        }, 1000);
    }

    private stopRamp() {
        if (this.rampTimer) clearInterval(this.rampTimer);
        this.rampTimer = null;
    }

    // ------------------------------------------------------------------ maintenance actions

    private requireIdle(action: string) {
        if (this.phase !== 'idle') throw new HttpError(409, `Cannot ${action} while ${this.phase}`);
    }

    async clean() {
        this.requireIdle('clean');
        const pool = this.getPool();
        if (!pool) throw new HttpError(503, 'No database connection');
        this.phase = 'cleaning';
        try {
            this.phaseDetail = 'Tearing down pipeline';
            await this.teardown(null);
            this.phaseDetail = 'Truncating write tables';
            await truncateTables(pool, WRITE_TABLES);
            addLog(`Truncated ${WRITE_TABLES.join(', ')}.`);
            this.lastRunSummary = null;
            this.run = null;
        } finally {
            this.phase = 'idle';
            this.phaseDetail = '';
        }
    }

    async cleanCdc() {
        this.requireIdle('clean CDC');
        this.phase = 'cleaning';
        try {
            // Disabling CDC drops the change tables; with no connector attached nothing is lost mid-stream.
            await this.teardown(null);
            addLog('CDC disabled on all simulator tables (change tables dropped). Next run re-enables only what it needs.');
        } finally {
            this.phase = 'idle';
        }
    }

    async cleanKafka() {
        this.requireIdle('purge Kafka');
        this.phase = 'cleaning';
        try {
            const topics = await this.admin.listTopics();
            for (const topic of topics.filter(t => t.startsWith('sim.'))) {
                const partitions = (await endOffsets(this.admin, topic)).filter(p => p.offset !== '0');
                if (partitions.length) {
                    await this.admin.deleteTopicRecords({ topic, partitions });
                    addLog(`Purged records from ${topic}`);
                }
            }
            const groups = await deleteGroups(this.admin, g => g.startsWith('sim-'));
            if (groups.length) addLog(`Deleted ${groups.length} stale consumer group(s).`);
            this.lastRunSummary = null;
        } finally {
            this.phase = 'idle';
        }
    }

    async verify(): Promise<VerifyResult> {
        const r = this.run;
        if (!r || !r.startedAt) throw new HttpError(409, 'No run to verify');
        if (this.phase === 'provisioning') throw new HttpError(409, 'Run is still provisioning');
        addLog(`Verifying ${r.profile.trackedTopic} for run ${r.runId}...`);
        const res = await verifyTopic(this.admin, r.profile.trackedTopic, r.profile.trackedKind, r.startOffsets, r.totals.onTime + r.totals.late);
        this.lastVerify = res;
        addLog(`Verification: ${res.messagesChecked} events, ${res.missing} missing, ${res.duplicates} duplicates, ${res.keyOrderViolations} key-order violations, ${res.entitiesSpreadAcrossPartitions}/${res.entitiesChecked} entities spread across partitions.`);
        return res;
    }

    async healthCheck() {
        const pool = this.getPool();
        const active = this.run && (this.phase === 'running' || this.phase === 'paused' || this.phase === 'draining');
        const profile = active ? this.run!.profile : null;
        const health = {
            sqlServer: 'UNKNOWN', kafka: 'UNKNOWN', debezium: 'UNKNOWN', flink: 'UNKNOWN',
            expected: profile ? { approach: profile.id, cdcTables: profile.cdcTables, trigger: profile.trigger, connector: this.run!.connector, flinkJob: profile.flinkJob } : null,
            actual: {} as any,
            errors: [] as string[],
        };
        try {
            if (!pool) throw new Error('no connection');
            const tracked = await getCdcTrackedTables(pool);
            const trigger = await isTriggerEnabled(pool);
            health.actual.cdcTables = tracked;
            health.actual.trigger = trigger;
            health.sqlServer = 'OK';
            const want = profile ? profile.cdcTables : [];
            const extra = tracked.filter(t => !want.includes(t));
            const missing = want.filter(t => !tracked.includes(t));
            if (extra.length) health.errors.push(`CDC enabled on tables the current state does not need: ${extra.join(', ')}`);
            if (missing.length) health.errors.push(`CDC missing on: ${missing.join(', ')}`);
            if (trigger !== (profile?.trigger ?? false)) health.errors.push(`Approach 1 trigger is ${trigger ? 'enabled' : 'disabled'} but should be ${trigger ? 'disabled' : 'enabled'}`);
        } catch (e) {
            health.sqlServer = 'ERROR';
            health.errors.push(`SQL Server: ${String((e as Error).message || e)}`);
        }
        try {
            await this.admin.listTopics();
            health.kafka = 'OK';
        } catch (e) {
            health.kafka = 'ERROR';
            health.errors.push(`Kafka: ${String((e as Error).message || e)}`);
        }
        try {
            const conns = (await listConnectors()).filter(isSimConnector);
            health.actual.connectors = conns;
            const expected = profile && this.run!.connector ? [this.run!.connector] : [];
            const unexpected = conns.filter(c => !expected.includes(c));
            if (unexpected.length) health.errors.push(`Unexpected Debezium connectors running: ${unexpected.join(', ')}`);
            health.debezium = 'OK';
            for (const c of expected) {
                const st = await connectorStatus(c);
                if (st?.connector?.state !== 'RUNNING' || st?.tasks?.some((t: any) => t.state !== 'RUNNING')) {
                    health.debezium = 'DEGRADED';
                    health.errors.push(`Debezium connector ${c} is not fully RUNNING`);
                }
            }
        } catch (e) {
            health.debezium = 'ERROR';
            health.errors.push(`Debezium: ${String((e as Error).message || e)}`);
        }
        try {
            const jobs = (await listFlinkJobs()).filter(j => j.state === 'RUNNING');
            health.actual.flinkJobs = jobs.map(j => j.name);
            const want = profile?.flinkJob ? 1 : 0;
            if (jobs.length !== want) health.errors.push(`Expected ${want} running Flink job(s), found ${jobs.length}`);
            health.flink = 'OK';
        } catch (e) {
            health.flink = 'ERROR';
            health.errors.push(`Flink: ${String((e as Error).message || e)}`);
        }
        health.actual.consumerThreads = this.consumers.length;
        const healthy = health.errors.length === 0 && [health.sqlServer, health.kafka, health.debezium, health.flink].every(s => s === 'OK');
        return { healthy, ...health };
    }

    // ------------------------------------------------------------------ stats

    private elapsedSec(r: RunState): number {
        if (!r.startedAt) return 0;
        const end = r.stopRequestedAt ?? Date.now();
        const paused = r.pausedMs + (r.pausedAt ? end - r.pausedAt : 0);
        return Math.max(0, Math.floor((end - r.startedAt - paused) / 1000));
    }

    private evaluateAlerts(r: RunState, lag: number) {
        if (r.flawAlert || !r.measuring) return;
        if (r.profile.smt && r.enrichmentMisses > 0) {
            r.flawAlert = `ENRICHMENT MISS: the JDBC SMT found no invoice for ${r.enrichmentMisses} event(s).`;
        } else if (r.profile.dualWrite && r.totals.partialWrites > 0) {
            r.flawAlert = `DUAL WRITE INCONSISTENCY: ${r.totals.partialWrites} billing change(s) committed without their enrichment request; they can never be joined.`;
        } else if (lag > Math.max(r.targetRps * 10, 5000)) {
            r.flawAlert = `PIPELINE BACKLOG: ${lag.toLocaleString()} committed changes not yet on ${r.profile.trackedTopic} (more than 10s of load behind).`;
        }
        if (r.flawAlert) addLog(r.flawAlert, 'warn');
    }

    /** Called once per second: closes the current metrics window and caches the snapshot. */
    async tick() {
        const r = this.run;
        if (r && r.startOffsets.length && (this.phase === 'running' || this.phase === 'paused' || this.phase === 'draining')) {
            this.lastEnd = await endOffsets(this.admin, r.profile.trackedTopic);
        }
        const committed = r ? r.totals.onTime + r.totals.late : 0;
        const lag = r ? Math.max(0, committed - r.delivered) : 0;
        if (r) this.evaluateAlerts(r, lag);

        const wh = r?.windowHists;
        if (wh) {
            this.recentHists.push(wh);
            if (this.recentHists.length > ROLLING_WINDOWS) this.recentHists.shift();
        }
        const rolling = newHists();
        for (const h of this.recentHists) {
            rolling.pipeline.merge(h.pipeline); rolling.lookup.merge(h.lookup);
            rolling.enrich.merge(h.enrich); rolling.e2e.merge(h.e2e);
        }
        const rates = this.rollingRates(r);
        const db = wh?.db.summary();
        const pipe = rolling.pipeline.summary();
        this.snapshot = {
            runId: r?.runId ?? null,
            ...this.phaseFields(),
            approach: r?.params.approach ?? null,
            trackedTopic: r?.profile.trackedTopic ?? null,
            connector: r?.connector ?? null,
            elapsedSec: r ? this.elapsedSec(r) : 0,
            rps: r?.targetRps ?? 0,
            issuedRps: rates.issued,
            actualRps: rates.committed,
            appLatency: round(db?.avg ?? 0, 1),
            p50: round(db?.p50 ?? 0, 1),
            p95: round(db?.p95 ?? 0, 1),
            p99: round(db?.p99 ?? 0, 1),
            queueLatency: round(wh?.queue.mean() ?? 0, 1),
            queueP95: round(wh?.queue.percentile(0.95) ?? 0, 1),
            recordsModified: r?.totals.onTime ?? 0,
            recordsLate: r?.totals.late ?? 0,
            recordsFailed: r?.totals.failed ?? 0,
            recordsDropped: r?.totals.dropped ?? 0,
            recordsCommitted: committed,
            partialWrites: r?.totals.partialWrites ?? 0,
            opsApplied: r?.totals.ops ?? { c: 0, u: 0, d: 0 },
            recordsInKafka: r?.delivered ?? 0,
            topicMessages: r ? countBetween(r.startOffsets, this.lastEnd) : 0,
            lag,
            pipelineLatency: { avg: round(pipe?.avg ?? 0), p50: round(pipe?.p50 ?? 0), p95: round(pipe?.p95 ?? 0), p99: round(pipe?.p99 ?? 0) },
            captureLagMs: round(pipe?.avg ?? 0),
            enrichmentMisses: r?.enrichmentMisses ?? 0,
            smtLookupAvgMs: round(rolling.lookup.mean(), 2),
            flawAlert: r?.flawAlert ?? null,
            dbStats: getCachedDbStats(),
            containerStats: getCachedContainerStats(),
            subscriberStats: r?.profile.consumers ? {
                numSubscribers: r.params.numSubscribers,
                readySubscribers: r.consumer.readySubscribers,
                totalMessagesConsumed: r.consumer.consumed,
                consumedPerSec: rates.consumed,
                avgEnrichmentLatency: round(rolling.enrich.mean(), 1),
                p95EnrichmentLatency: round(rolling.enrich.percentile(0.95), 1),
                p99EnrichmentLatency: round(rolling.enrich.percentile(0.99), 1),
                avgE2eLatency: round(rolling.e2e.mean()),
                p95E2eLatency: round(rolling.e2e.percentile(0.95)),
                enrichmentsFailed: r.consumer.failed,
                enrichmentMisses: r.consumer.misses,
                skippedEvents: r.consumer.skipped,
                consumerLag: this.consumerLag(),
            } : undefined,
        };
        if (r) {
            r.windowHists = newHists();
        }
        return this.snapshot;
    }

    /**
     * Rates from cumulative counters over the last few seconds. Worker stats arrive on their own
     * 1s timers, so a single 1s window can hold 0 or 2 reports; a short rolling window removes that jitter.
     */
    private rollingRates(r: RunState | null) {
        if (!r || !r.measuring) {
            this.rateSamples = [];
            return { issued: 0, committed: 0, consumed: 0 };
        }
        const now = Date.now();
        this.rateSamples.push({ t: now, runId: r.runId, issued: r.totals.issued, committed: r.totals.onTime + r.totals.late, consumed: r.consumer.consumed });
        this.rateSamples = this.rateSamples.filter(x => x.runId === r.runId && now - x.t <= ROLLING_WINDOWS * 1000 + 500);
        const first = this.rateSamples[0];
        const last = this.rateSamples[this.rateSamples.length - 1];
        const dt = (last.t - first.t) / 1000;
        if (dt < 0.5) return { issued: 0, committed: 0, consumed: 0 };
        return {
            issued: round((last.issued - first.issued) / dt),
            committed: round((last.committed - first.committed) / dt),
            consumed: round((last.consumed - first.consumed) / dt),
        };
    }

    private phaseFields() {
        return {
            phase: this.phase,
            phaseDetail: this.phaseDetail,
            lastError: this.lastError,
            isRunning: this.phase === 'running' || this.phase === 'paused',
            isPaused: this.phase === 'paused',
            isProvisioning: this.phase === 'provisioning',
            isDraining: this.phase === 'draining',
            isCleaning: this.phase === 'cleaning',
            lastRunSummary: this.lastRunSummary,
        };
    }

    /** Last computed metrics window, with live (never stale) lifecycle fields. */
    getSnapshot() {
        return { ...this.snapshot, ...this.phaseFields() };
    }

    private buildSummary() {
        const r = this.run!;
        const h = r.runHists;
        const committed = r.totals.onTime + r.totals.late;
        const total = committed + r.totals.failed + r.totals.dropped;
        const pct = (n: number) => (total > 0 ? round((n / total) * 100, 2) : 0);
        const db = h.db.summary();
        const pipe = h.pipeline.summary();
        const elapsedSec = this.elapsedSec(r);
        return {
            runId: r.runId,
            approach: r.params.approach,
            approachName: r.profile.name,
            params: r.params,
            trackedTopic: r.profile.trackedTopic,
            elapsedSec,
            finalTargetRps: r.targetRps,
            avgCommittedRps: elapsedSec > 0 ? round(committed / elapsedSec, 1) : 0,
            recordsModified: r.totals.onTime,
            recordsLate: r.totals.late,
            recordsFailed: r.totals.failed,
            recordsDropped: r.totals.dropped,
            recordsCommitted: committed,
            partialWrites: r.totals.partialWrites,
            opsApplied: r.totals.ops,
            recordsInKafka: r.delivered,
            lag: Math.max(0, committed - r.delivered),
            slaRate: pct(r.totals.onTime),
            successRate: pct(committed),
            appLatency: round(db.avg, 1), p50: round(db.p50, 1), p95: round(db.p95, 1), p99: round(db.p99, 1), maxLatency: round(db.max, 1),
            queueLatency: round(h.queue.mean(), 1),
            pipelineLatency: { avg: round(pipe.avg), p50: round(pipe.p50), p95: round(pipe.p95), p99: round(pipe.p99), max: round(pipe.max) },
            enrichmentMisses: r.enrichmentMisses,
            subscriber: r.profile.consumers ? {
                numSubscribers: r.params.numSubscribers,
                consumed: r.consumer.consumed,
                failed: r.consumer.failed,
                misses: r.consumer.misses,
                enrichment: h.enrich.summary(),
                e2e: h.e2e.summary(),
            } : undefined,
            flawAlert: r.flawAlert,
            finishedAt: new Date().toISOString(),
        };
    }

    // ------------------------------------------------------------------ lifecycle

    async reconnectWriters() {
        this.writerReady = this.writerReady.map(() => false);
        this.broadcast({ type: 'reconnect' });
    }

    markReady() {
        this.phase = 'idle';
        this.phaseDetail = '';
    }

    async shutdown() {
        this.stopRamp();
        this.abortRequested = true;
        this.broadcast({ type: 'stop' });
        await Promise.race([this.teardown(this.run), sleep(20_000)]).catch(() => undefined);
        await Promise.all(this.writers.map(w => w.terminate()));
    }
}

export type { SerializedHistogram };
