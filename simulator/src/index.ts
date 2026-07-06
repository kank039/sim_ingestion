import express from 'express';
import cors from 'cors';
import os from 'os';
import path from 'path';
import fs from 'fs/promises';
import { Worker } from 'worker_threads';
import { connectDB, getDBStats, getPool, populateInitialData } from './db';
import http from 'http';
import { promisify } from 'util';
import { Kafka } from 'kafkajs';


const kafka = new Kafka({
  clientId: 'simulator-admin',
  brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
});
const admin = kafka.admin();

async function getContainerStats() {
    try {
        const containers: any[] = await new Promise((resolve, reject) => {
            const req = http.request({ socketPath: '/var/run/docker.sock', path: '/containers/json', method: 'GET' }, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try { resolve(JSON.parse(data)); } catch(e) { reject(e); }
                });
            });
            req.on('error', reject);
            req.end();
        });

        const statsPromises = containers.map(c => new Promise<any>((resolve) => {
            const req = http.request({ socketPath: '/var/run/docker.sock', path: `/containers/${c.Id}/stats?stream=false`, method: 'GET' }, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try {
                        const stat = JSON.parse(data);
                        let cpu = 0;
                        let cpuCores: string[] = [];
                        if (stat.cpu_stats && stat.precpu_stats) {
                            const cpuDelta = stat.cpu_stats.cpu_usage.total_usage - stat.precpu_stats.cpu_usage.total_usage;
                            const systemDelta = stat.cpu_stats.system_cpu_usage - stat.precpu_stats.system_cpu_usage;
                            const cpus = stat.cpu_stats.online_cpus || stat.cpu_stats.cpu_usage.percpu_usage?.length || 1;
                            if (systemDelta > 0 && cpuDelta > 0) cpu = (cpuDelta / systemDelta) * cpus * 100.0;
                            
                            // Determine cpu limit. If not explicitly in stats, fallback to a heuristic or container names.
                            // In docker-compose, limits are sqlserver:4, kafka:2, debezium:1.5, simulator:2, taskmanager:1.5, jobmanager:1
                            let limitCpus = 1;
                            const name = c.Names[0].replace('/', '');
                            if (name === 'sqlserver') limitCpus = 4;
                            else if (name === 'kafka' || name === 'simulator') limitCpus = 2;
                            else if (name === 'debezium' || name === 'flink-taskmanager') limitCpus = 1.5;
                            else if (name === 'flink-jobmanager') limitCpus = 1;
                            else limitCpus = Math.ceil(cpu / 100) || 1; // Fallback

                            const numCores = Math.ceil(limitCpus);
                            let remainingCpu = cpu;
                            for (let i = 0; i < numCores; i++) {
                                const maxForThisCore = (i === Math.floor(limitCpus)) ? (limitCpus % 1) * 100 : 100;
                                const coreUsage = Math.min(remainingCpu, maxForThisCore);
                                cpuCores.push((coreUsage).toFixed(2) + '%');
                                remainingCpu = Math.max(0, remainingCpu - coreUsage);
                            }
                        }
                        
                        let mem = 0;
                        let cacheMem = 0;
                        if (stat.memory_stats && stat.memory_stats.limit) {
                            let cache = 0;
                            if (stat.memory_stats.stats) {
                                if (stat.memory_stats.stats.inactive_file !== undefined) {
                                    cache = stat.memory_stats.stats.inactive_file;
                                } else if (stat.memory_stats.stats.cache !== undefined) {
                                    cache = stat.memory_stats.stats.cache;
                                }
                            }
                            const usedMem = Math.max(0, stat.memory_stats.usage - cache);
                            mem = (usedMem / stat.memory_stats.limit) * 100.0;
                            cacheMem = (cache / stat.memory_stats.limit) * 100.0;
                        }

                        resolve({
                            name: c.Names[0].replace('/', ''),
                            cpu: cpu.toFixed(2) + '%',
                            cpuCores,
                            mem: mem.toFixed(2) + '%',
                            cacheMem: cacheMem.toFixed(2) + '%'
                        });
                    } catch(e) { resolve(null); }
                });
            });
            req.on('error', () => resolve(null));
            req.end();
        }));

        const results = await Promise.all(statsPromises);
        return results.filter(r => r !== null && r.name !== 'kafka-ui');
    } catch (e) {
        console.error("Docker stats error:", e);
        return [];
    }
}

const app = express();
app.use(cors());
app.use(express.json());

const numWorkers = 4; // Using 4 worker threads
const workers: Worker[] = [];
let readyWorkers = 0;

// Approach 5: Consumer worker state
const consumerWorkers: Worker[] = [];
let readyConsumers = 0;
let currentNumSubscribers = 0;
const consumerLatencies = new Map<number, any>();
let globalMessagesConsumed = 0;
let globalEnrichmentsFailed = 0;

let isRunning = false;
let isPaused = false;
let currentApproach = 1;
let currentRps = 10;
const workerLatencies = new Map<number, any>();
let globalRecordsModified = 0;
let globalRecordsFailed = 0;
let globalRecordsLate = 0;
let baselineKafkaOffset = 0;
let globalCaptureLagMs = 0;
let isCleaning = false;
let globalFlawAlert: string | null = null;

let gradualInterval: NodeJS.Timeout | null = null;

let systemLogs: { time: string, message: string, level: string, timestamp: number }[] = [];
function addLog(message: string, level: string = 'info') {
    systemLogs.unshift({ time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now() });
    if (systemLogs.length > 500) systemLogs.pop();
}

// Helper: Aggregate subscriber stats from consumer workers
function getSubscriberStats() {
    if (currentApproach !== 5 || consumerWorkers.length === 0) return undefined;
    
    let totalEnrichAvg = 0;
    let totalEnrichP95 = 0;
    let totalEnrichP99 = 0;
    let totalE2eAvg = 0;
    let count = 0;

    consumerLatencies.forEach((val: any) => {
        totalEnrichAvg += val.enrichmentAvg || 0;
        totalEnrichP95 += val.enrichmentP95 || 0;
        totalEnrichP99 += val.enrichmentP99 || 0;
        totalE2eAvg += val.e2eAvg || 0;
        count++;
    });

    return {
        numSubscribers: currentNumSubscribers,
        totalMessagesConsumed: globalMessagesConsumed,
        avgEnrichmentLatency: count > 0 ? Math.round(totalEnrichAvg / count) : 0,
        p95EnrichmentLatency: count > 0 ? Math.round(totalEnrichP95 / count) : 0,
        p99EnrichmentLatency: count > 0 ? Math.round(totalEnrichP99 / count) : 0,
        avgE2eLatency: count > 0 ? Math.round(totalE2eAvg / count) : 0,
        enrichmentsFailed: globalEnrichmentsFailed,
        consumerLag: 0 // TODO: compute from Kafka consumer group lag
    };
}

// Helper: Spawn consumer workers for Approach 5
function spawnConsumerWorkers(numSubscribers: number) {
    stopConsumerWorkers(); // Clean up any existing
    currentNumSubscribers = numSubscribers;
    readyConsumers = 0;
    globalMessagesConsumed = 0;
    globalEnrichmentsFailed = 0;
    consumerLatencies.clear();

    const NUM_CONSUMER_WORKER_THREADS = 4;
    const subscribersPerWorker = Math.ceil(numSubscribers / NUM_CONSUMER_WORKER_THREADS);

    for (let i = 0; i < NUM_CONSUMER_WORKER_THREADS; i++) {
        let assignedSubscribers = Math.min(subscribersPerWorker, numSubscribers - i * subscribersPerWorker);
        if (assignedSubscribers <= 0) break;

        const worker = new Worker(path.join(__dirname, 'consumer-worker.ts'), {
            execArgv: ['-r', 'ts-node/register'],
            workerData: { workerId: i, assignedSubscribers, baseSubscriberIndex: i * subscribersPerWorker }
        });

        worker.on('message', (msg) => {
            if (msg.type === 'consumer-ready') {
                readyConsumers += msg.assignedSubscribers;
                addLog(`Consumer worker thread ${msg.workerId + 1} ready (${readyConsumers}/${numSubscribers} subscribers connected)`);
            } else if (msg.type === 'consumer-stats') {
                consumerLatencies.set(msg.workerId, {
                    enrichmentAvg: msg.enrichmentAvg || 0,
                    enrichmentP95: msg.enrichmentP95 || 0,
                    enrichmentP99: msg.enrichmentP99 || 0,
                    e2eAvg: msg.e2eAvg || 0
                });
                if (msg.messagesConsumed) {
                    globalMessagesConsumed += msg.messagesConsumed;
                }
                if (msg.enrichmentsFailed) {
                    globalEnrichmentsFailed += msg.enrichmentsFailed;
                }
            } else if (msg.type === 'consumer-error') {
                addLog(`Consumer worker thread ${msg.workerId + 1} error: ${msg.error}`, 'error');
            }
        });

        worker.on('error', (err) => console.error(`Consumer worker thread ${i} error:`, err));
        consumerWorkers.push(worker);
    }

    // Start all consumer worker threads
    for (const cw of consumerWorkers) {
        cw.postMessage({ type: 'start' });
    }
    addLog(`Spawned ${consumerWorkers.length} consumer threads to handle ${numSubscribers} subscribers for Approach 5`);
}

// Helper: Stop and clean up consumer workers
function stopConsumerWorkers() {
    for (const cw of consumerWorkers) {
        try {
            cw.postMessage({ type: 'stop' });
        } catch(e) {}
    }
    // Terminate after a brief delay to allow cleanup
    setTimeout(() => {
        for (const cw of consumerWorkers) {
            try { cw.terminate(); } catch(e) {}
        }
    }, 1000);
    consumerWorkers.length = 0;
    consumerLatencies.clear();
    currentNumSubscribers = 0;
    readyConsumers = 0;
}

// Initialize Workers
for (let i = 0; i < numWorkers; i++) {
    // Note: Since we run with ts-node in development, we load the ts file via a wrapper or direct if supported.
    // To ensure ts-node works with worker_threads, we can pass execArgv: ['-r', 'ts-node/register']
    const worker = new Worker(path.join(__dirname, 'simulation.ts'), {
        execArgv: ['-r', 'ts-node/register']
    });

    worker.on('message', (msg) => {
        if (msg.type === 'ready') {
            readyWorkers++;
            console.log(`Worker ${i+1} ready (${readyWorkers}/${numWorkers})`);
        } else if (msg.type === 'stats') {
            workerLatencies.set(i, {
                avgLatency: msg.avgLatency || 0,
                queueAvg: msg.queueAvg || 0,
                p95: msg.p95 || 0,
                p99: msg.p99 || 0
            });
            if (msg.modified) {
                globalRecordsModified += msg.modified;
            }
            if (msg.failed) {
                globalRecordsFailed += msg.failed;
            }
            if (msg.late) {
                globalRecordsLate += msg.late;
            }
        }
    });

    worker.on('error', (err) => console.error(`Worker error:`, err));
    workers.push(worker);
}


app.get('/api/stats', async (req, res) => {
    try {
        const [dbStats, containerStats] = await Promise.all([
            getDBStats(),
            getContainerStats()
        ]);
        
        let recordsInKafka = 0;
        try {
            const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
            // highOffset is string, parse and sum them up
            recordsInKafka = offsets.reduce((sum, partition) => sum + parseInt(partition.high, 10), 0);
        } catch(e) {
            // Topic might not exist yet
        }
        
        const lag = Math.max(0, (globalRecordsModified + globalRecordsLate) - Math.max(0, recordsInKafka - baselineKafkaOffset));
        
        if (!globalFlawAlert && lag > Math.max(currentRps * 10, 5000)) {
            if (currentApproach === 4) {
                globalFlawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
            } else {
                globalFlawAlert = "PIPELINE FLAW DETECTED: CDC pipeline unable to keep up with mutation rate.";
            }
        }
        
        let totalLatency = 0;
        let totalQueueAvg = 0;
        let latCount = 0;
        let queueLatCount = 0;
        let p95Sum = 0;
        let p99Sum = 0;
        
        workerLatencies.forEach((val: any) => {
            if (typeof val === 'number') {
                totalLatency += val;
                latCount++;
            } else {
                totalLatency += val.avgLatency;
                totalQueueAvg += val.queueAvg;
                p95Sum += val.p95;
                p99Sum += val.p99;
                latCount++;
            }
        });
        
        const avgLatency = latCount > 0 ? totalLatency / latCount : 0;
        const avgQueue = latCount > 0 ? totalQueueAvg / latCount : 0;
        const p95 = latCount > 0 ? p95Sum / latCount : 0;
        const p99 = latCount > 0 ? p99Sum / latCount : 0;
        
        const elapsedSec = isRunning ? Math.floor((Date.now() - runStartTime) / 1000) : 0;
        
        const subscriberStats = getSubscriberStats();
        
        res.json({
            runId: currentRunId,
            elapsedSec,
            isRunning,
            isPaused,
            isCleaning,
            approach: currentApproach,
            rps: currentRps,
            appLatency: Math.round(avgLatency),
            queueLatency: Math.round(avgQueue),
            p95: Math.round(p95),
            p99: Math.round(p99),
            flawAlert: globalFlawAlert,
            dbStats,
            containerStats,
            recordsModified: globalRecordsModified,
            recordsFailed: globalRecordsFailed,
            recordsLate: globalRecordsLate,
            recordsInKafka: Math.max(0, recordsInKafka - baselineKafkaOffset),
            lag: Math.max(0, (globalRecordsModified + globalRecordsLate) - Math.max(0, recordsInKafka - baselineKafkaOffset)),
            captureLagMs: globalCaptureLagMs,
            logs: systemLogs, // For GET, return all logs
            subscriberStats
        });
    } catch (e) {
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/simulate/start', async (req, res) => {
    if (isCleaning) {
        return res.status(409).json({ error: 'Cannot start simulation while cleanup is in progress' });
    }
    const { approach, rps, gradual, endRps, timeoutMs, insertsOnly, cardinality, insertWeight, updateWeight, deleteWeight, numSubscribers } = req.body;
    currentApproach = approach;
    currentRps = rps;
    isRunning = true;
    isPaused = false;
    
    globalRecordsModified = 0;
    globalRecordsFailed = 0;
    globalRecordsLate = 0;
    globalFlawAlert = null;
    
    // Reset baseline offset so Kafka records start at 0 for this run
    try {
        const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
        baselineKafkaOffset = offsets.reduce((sum, partition) => sum + parseInt(partition.high, 10), 0);
    } catch (e) {
        baselineKafkaOffset = 0;
    }
    
    currentRunId = Date.now().toString();
    runStartTime = Date.now();
    
    if (gradualInterval) clearInterval(gradualInterval);
    
    const updateWorkers = (newRps: number) => {
        const rpsPerWorker = Math.ceil(newRps / numWorkers);
        for (const worker of workers) {
            worker.postMessage({ 
                type: 'start', approach, rps: rpsPerWorker, timeoutMs, insertsOnly, 
                cardinality, insertWeight, updateWeight, deleteWeight 
            });
        }
    };
    
    updateWorkers(currentRps);
    
    // Approach 5: Spawn consumer workers for subscriber enrichment
    if (approach === 5 && numSubscribers && numSubscribers > 0) {
        spawnConsumerWorkers(numSubscribers);
        addLog(`Simulation started (Approach 5: CDC Push + ${numSubscribers} Consumer Workers) at ${currentRps} RPS`);
    } else {
        stopConsumerWorkers(); // Clean up if switching away from approach 5
        addLog(`Simulation started (Approach ${approach}) at ${currentRps} RPS`);
    }
    
    if (gradual && endRps && endRps > rps) {
        gradualInterval = setInterval(() => {
            if (!isRunning) {
                if (gradualInterval) clearInterval(gradualInterval);
                return;
            }
            currentRps += 50; // Ramp up by 50 RPS per second
            if (currentRps >= endRps) {
                currentRps = endRps;
                if (gradualInterval) clearInterval(gradualInterval);
            }
            updateWorkers(currentRps);
        }, 1000);
    }
    
    res.json({ message: 'Simulation started with workers' });
});

app.post('/api/simulate/stop', (req, res) => {
    isRunning = false;
    isPaused = false;
    addLog(`Simulation stopped`);
    if (gradualInterval) clearInterval(gradualInterval);
    for (const worker of workers) {
        worker.postMessage({ type: 'stop' });
    }
    stopConsumerWorkers();
    res.json({ message: 'Simulation stopped' });
});

app.post('/api/simulate/pause', (req, res) => {
    if (!isRunning) return res.status(400).json({ error: 'Not running' });
    isPaused = true;
    addLog(`Simulation paused (workers stopped)`);
    for (const worker of workers) {
        worker.postMessage({ type: 'pause' });
    }
    res.json({ message: 'Simulation paused' });
});

app.post('/api/simulate/resume', (req, res) => {
    if (!isRunning) return res.status(400).json({ error: 'Not running' });
    isPaused = false;
    addLog(`Simulation resumed (workers restarted)`);
    for (const worker of workers) {
        worker.postMessage({ type: 'resume' });
    }
    res.json({ message: 'Simulation resumed' });
});

let currentRunId = Date.now().toString();
let runStartTime = Date.now();

let sseClients: { res: express.Response, lastLogTimestamp: number }[] = [];
app.get('/api/stats/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    
    const clientObj = { res, lastLogTimestamp: 0 };
    sseClients.push(clientObj);
    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== clientObj);
    });
});

setInterval(async () => {
    if (sseClients.length === 0) return;
    try {
        const [dbStats, containerStats] = await Promise.all([
            getDBStats(),
            getContainerStats()
        ]);
        
        let recordsInKafka = 0;
        try {
            const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
            recordsInKafka = offsets.reduce((sum, partition) => sum + parseInt(partition.high, 10), 0);
        } catch(e) {}
        
        const lag = Math.max(0, (globalRecordsModified + globalRecordsLate) - Math.max(0, recordsInKafka - baselineKafkaOffset));
        
        if (!globalFlawAlert && lag > Math.max(currentRps * 10, 5000)) {
            if (currentApproach === 4) {
                globalFlawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
            } else {
                globalFlawAlert = "PIPELINE FLAW DETECTED: CDC pipeline unable to keep up with mutation rate.";
            }
        }
        
        let totalLatency = 0;
        let totalQueueAvg = 0;
        let latCount = 0;
        let queueLatCount = 0;
        let p95Sum = 0;
        let p99Sum = 0;
        
        workerLatencies.forEach((val: any) => {
            // Check if workerLatencies is storing an object now instead of just a number
            if (typeof val === 'number') {
                totalLatency += val;
                latCount++;
            } else {
                totalLatency += val.avgLatency;
                totalQueueAvg += val.queueAvg;
                p95Sum += val.p95;
                p99Sum += val.p99;
                latCount++;
            }
        });
        
        const avgLatency = latCount > 0 ? totalLatency / latCount : 0;
        const avgQueue = latCount > 0 ? totalQueueAvg / latCount : 0;
        const p95 = latCount > 0 ? p95Sum / latCount : 0;
        const p99 = latCount > 0 ? p99Sum / latCount : 0;
        
        const elapsedSec = isRunning ? Math.floor((Date.now() - runStartTime) / 1000) : 0;
        
        sseClients.forEach(client => {
            const newLogs = systemLogs.filter(log => log.timestamp > client.lastLogTimestamp);
            if (newLogs.length > 0) {
                client.lastLogTimestamp = newLogs[0].timestamp;
            }
            
            const subscriberStats = getSubscriberStats();
            
            const payload = JSON.stringify({
                runId: currentRunId,
                elapsedSec,
                isRunning,
                isPaused,
                approach: currentApproach,
                rps: currentRps,
                appLatency: Math.round(avgLatency),
                queueLatency: Math.round(avgQueue),
                p95: Math.round(p95),
                p99: Math.round(p99),
                flawAlert: globalFlawAlert,
                dbStats,
                containerStats,
                recordsModified: globalRecordsModified,
                recordsFailed: globalRecordsFailed,
                recordsLate: globalRecordsLate,
                recordsInKafka: Math.max(0, recordsInKafka - baselineKafkaOffset),
                lag: Math.max(0, (globalRecordsModified + globalRecordsLate) - Math.max(0, recordsInKafka - baselineKafkaOffset)),
                captureLagMs: globalCaptureLagMs,
                newLogs,
                subscriberStats
            });
            
            client.res.write(`data: ${payload}\n\n`);
        });
    } catch(e) {
        console.error("SSE Broadcast Error:", e);
    }
}, 1000);

app.get('/health', (req, res) => {
    const pool = getPool();
    res.json({ status: 'ok', workers: readyWorkers, dbConnected: !!pool?.connected });
});

app.post('/api/simulate/clean-cdc', async (req, res) => {
    try {
        addLog('Initiating hard TRUNCATE of SQL Server CDC tables...', 'info');
        const cdcTables = [
            'dbo_billing_record_CT',
            'dbo_cdc_events_shadow_CT',
            'dbo_invoice_batch_CT',
            'dbo_outbox_events_CT',
            'dbo_rate_schedule_CT',
            'dbo_subscriber_plan_CT',
            'dbo_subscriber_usage_CT',
            'dbo_enrichment_requests_CT'
        ];

        for (const table of cdcTables) {
            try {
                await getPool().request().query(`TRUNCATE TABLE cdc.${table}`);
                addLog(`CDC table truncated: cdc.${table}`, 'info');
            } catch (e) {
                // Table might not exist or error
            }
        }
        
        addLog('CDC cleanup completed.', 'info');
        res.json({ message: 'CDC cleanup completed' });
    } catch(e) {
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/simulate/clean-kafka', async (req, res) => {
    try {
        addLog('Initiating immediate Kafka topic purge (deleteTopicRecords)...', 'info');
        
        const topics = await admin.listTopics();
        const simTopics = topics.filter((t: string) => t.startsWith('sim.'));
        
        if (simTopics.length > 0) {
            for (const topic of simTopics) {
                try {
                    const offsets = await admin.fetchTopicOffsets(topic);
                    const partitionsToDelete = offsets.map((p: any) => ({
                        partition: p.partition,
                        offset: p.high
                    })).filter((p: any) => p.offset !== '0');
                    
                    if (partitionsToDelete.length > 0) {
                        await admin.deleteTopicRecords({
                            topic,
                            partitions: partitionsToDelete
                        });
                        addLog(`Purged records from topic: ${topic}`, 'info');
                    }
                } catch (err) {
                    addLog(`Failed to purge topic ${topic}: ${String(err)}`, 'error');
                }
            }
            addLog(`Kafka topics purge complete.`, 'info');
        } else {
            addLog('No Kafka topics found to purge.', 'info');
        }

        res.json({ message: 'Kafka purge completed' });
    } catch(e) {
        addLog(`Kafka purge failed: ${String(e)}`, 'error');
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/simulate/clean', async (req, res) => {
    if (isCleaning) {
        return res.status(409).json({ error: 'Cleanup already in progress' });
    }
    isCleaning = true;
    try {
        addLog('Cleaning SQL Server records (Truncating)...');
        const pool = getPool();
        const tables = ['billing_record', 'outbox_events', 'cdc_events_shadow', 'invoice_batch'];
        for (const table of tables) {
            let rowsAffected = 5000;
            while (rowsAffected >= 5000) {
                const result = await pool.request().query(`DELETE TOP (5000) FROM ${table}`);
                rowsAffected = result.rowsAffected[0] || 0;
            }
        }
        
        addLog('Repopulating initial invoice_batch data...');
        await populateInitialData(pool);
        
        try {
            let rowsAffected = 5000;
            while (rowsAffected >= 5000) {
                const result = await pool.request().query(`DELETE TOP (5000) FROM subscriber_usage`);
                rowsAffected = result.rowsAffected[0] || 0;
            }
            addLog('Approach 5 subscriber_usage table truncated.');
        } catch(e) {
            // Table might not exist yet
        }
        
        addLog('Waiting for Debezium to recognize and process deletes...');
        let stableCount = 0;
        let lastOffset = -1;
        
        // Wait up to 60 seconds for Debezium to process the backlog of deletes
        for (let i = 0; i < 60; i++) {
            await new Promise(resolve => setTimeout(resolve, 1000));
            try {
                const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
                const currentOffset = offsets.reduce((sum, partition) => sum + parseInt(partition.high, 10), 0);
                
                if (currentOffset === lastOffset) {
                    stableCount++;
                    if (stableCount >= 2) {
                        baselineKafkaOffset = currentOffset;
                        break;
                    }
                } else {
                    stableCount = 0;
                    lastOffset = currentOffset;
                    baselineKafkaOffset = currentOffset;
                }
            } catch(e) {
                baselineKafkaOffset = 0;
                break;
            }
        }
        
        globalRecordsModified = 0;
        globalRecordsFailed = 0;
        globalRecordsLate = 0;
        globalMessagesConsumed = 0;
        globalEnrichmentsFailed = 0;
        globalFlawAlert = null;
        
        addLog(`Kafka baseline offset stabilized at ${baselineKafkaOffset}.`);
        
        addLog('System perfectly cleaned.');
        res.json({ message: 'Cleaned' });
    } catch (e) {
        addLog('Error cleaning: ' + String(e));
        res.status(500).json({ error: String(e) });
    } finally {
        isCleaning = false;
    }
});

let isReconnecting = false;

app.post('/api/system/reconnect', async (req, res) => {
    if (isReconnecting) {
        return res.status(409).json({ error: 'Reconnect already in progress' });
    }
    isReconnecting = true;
    try {
        addLog('Reconnecting to SQL Server and Kafka...');
        await connectDB();
        try { await admin.disconnect(); } catch(e) {}
        await admin.connect();
        
        for (const worker of workers) {
            worker.postMessage({ type: 'reconnect' });
        }
        for (const cw of consumerWorkers) {
            cw.postMessage({ type: 'reconnect' });
        }
        
        addLog('Successfully reconnected all services.');
        res.json({ message: 'Reconnected' });
    } catch (e) {
        addLog('Error reconnecting: ' + String(e));
        res.status(500).json({ error: String(e) });
    } finally {
        isReconnecting = false;
    }
});

app.get('/api/system/health-check', async (req, res) => {
    const health = {
        sqlServer: 'UNKNOWN',
        kafka: 'UNKNOWN',
        debezium: 'UNKNOWN',
        cdc_rows: 0,
        errors: [] as string[]
    };
    
    try {
        // Check SQL Server
        try {
            let expectedTable = '';
            let ctTableName = 'dbo_billing_record_CT';
            if (currentApproach === 1) { expectedTable = 'billing_record'; ctTableName = 'dbo_billing_record_CT'; }
            else if (currentApproach === 2) { expectedTable = 'outbox_events'; ctTableName = 'dbo_outbox_events_CT'; }
            else if (currentApproach === 3) { expectedTable = 'cdc_events_shadow'; ctTableName = 'dbo_cdc_events_shadow_CT'; }
            else if (currentApproach === 4) { expectedTable = 'invoice_batch'; ctTableName = 'dbo_invoice_batch_CT'; }

            const query = `
                SELECT 1 as alive; 
                SELECT name FROM sys.tables WHERE is_tracked_by_cdc = 1;
                BEGIN TRY
                    SELECT COUNT(*) as count FROM sim_db.cdc.${ctTableName};
                END TRY
                BEGIN CATCH
                    SELECT 0 as count;
                END CATCH
            `;
            
            const result = await getPool().request().query(query);
            health.sqlServer = 'OK';
            health.cdc_rows = (result.recordsets as any)[2]?.[0]?.count || 0;
            
            const trackedTables = (result.recordsets as any)[1]?.map((r: any) => r.name) || [];
            
            if (expectedTable && !trackedTables.includes(expectedTable)) {
                health.errors.push(`Configuration Mismatch: Approach ${currentApproach} requires CDC on '${expectedTable}' but it is not enabled.`);
            }
            
            const extraTables = trackedTables.filter((t: string) => t !== expectedTable);
            if (extraTables.length > 0) {
                health.errors.push(`Performance Warning: CDC is currently enabled on extra tables (${extraTables.join(', ')}). This causes unnecessary double-writes and wastes SQL Server memory/CPU during Approach ${currentApproach}.`);
            }
            
        } catch (e) {
            health.sqlServer = 'ERROR';
            health.errors.push('SQL Server: ' + String(e));
        }

        // Check Kafka
        try {
            const topics = await admin.listTopics();
            if (topics) {
                health.kafka = 'OK';
            }
        } catch (e) {
            health.kafka = 'ERROR';
            health.errors.push('Kafka: ' + String(e));
        }

        // Check Debezium
        try {
            const response = await fetch('http://debezium:8083/connectors/billing_record_connector/status');
            if (response.ok) {
                const data = await response.json() as any;
                if (data.connector?.state === 'RUNNING' && data.tasks?.every((t: any) => t.state === 'RUNNING')) {
                    health.debezium = 'OK';
                } else {
                    health.debezium = 'DEGRADED';
                    health.errors.push('Debezium: Connector or task not running');
                }
            } else {
                health.debezium = 'ERROR';
                health.errors.push('Debezium: ' + response.statusText);
            }
        } catch (e) {
            health.debezium = 'ERROR';
            health.errors.push('Debezium: ' + String(e));
        }

        const isHealthy = health.sqlServer === 'OK' && health.kafka === 'OK' && health.debezium === 'OK';
        res.json({ healthy: isHealthy, ...health });
    } catch (e) {
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/system/induce-failure', async (req, res) => {
    try {
        addLog('Inducing hard failure (Restarting Debezium container)...', 'error');
        const containers: any[] = await new Promise((resolve, reject) => {
            const req = http.request({ socketPath: '/var/run/docker.sock', path: '/containers/json', method: 'GET' }, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => resolve(JSON.parse(data)));
            });
            req.on('error', reject);
            req.end();
        });
        const deb = containers.find(c => c.Names[0].includes('debezium'));
        if (deb) {
            await new Promise((resolve, reject) => {
                const req = http.request({ socketPath: '/var/run/docker.sock', path: `/containers/${deb.Id}/restart`, method: 'POST' }, (res) => {
                    resolve(true);
                });
                req.on('error', reject);
                req.end();
            });
            addLog('Debezium container restarted successfully.', 'info');
            res.json({ message: 'Hard failure induced' });
        } else {
            addLog('Debezium container not found', 'error');
            res.status(404).json({ error: 'Debezium container not found' });
        }
    } catch(e) {
        addLog('Failed to induce failure: ' + String(e), 'error');
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/simulate/verify', async (req, res) => {
    try {
        addLog('Starting verification checks...', 'info');
        const verifyConsumer = kafka.consumer({ groupId: 'verify-group-' + Date.now() });
        await verifyConsumer.connect();
        await verifyConsumer.subscribe({ topic: 'sim.sim_db.dbo.billing_record', fromBeginning: true });

        const duplicateCheck = new Map<number, number>();
        let lastId = -1;
        let isStrictlyMonotonic = true;
        let recordsProcessed = 0;
        let baselineOffsetNum = baselineKafkaOffset;

        const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
        const targetHighOffset = parseInt(offsets[0].high, 10);
        
        let promiseResolve: any;
        const consumePromise = new Promise(resolve => promiseResolve = resolve);

        if (targetHighOffset === 0 || targetHighOffset <= baselineOffsetNum) {
            await verifyConsumer.disconnect();
            return res.json({ ordered: true, duplicates: 0, messagesChecked: 0 });
        }

        await verifyConsumer.run({
            eachMessage: async ({ message, partition }) => {
                const offset = parseInt(message.offset, 10);
                if (offset >= baselineOffsetNum) {
                    try {
                        const val = JSON.parse(message.value?.toString() || '{}');
                        const id = val.payload?.after?.id || val.payload?.id;
                        if (id !== undefined) {
                            duplicateCheck.set(id, (duplicateCheck.get(id) || 0) + 1);
                            if (lastId !== -1 && id <= lastId) {
                                isStrictlyMonotonic = false;
                            }
                            lastId = id;
                            recordsProcessed++;
                        }
                    } catch(e) {}
                }
                if (offset >= targetHighOffset - 1) {
                    promiseResolve();
                }
            }
        });

        setTimeout(() => { promiseResolve(); }, 10000);
        await consumePromise;
        await verifyConsumer.disconnect();

        let duplicatesCount = 0;
        for (const [id, count] of duplicateCheck.entries()) {
            if (count > 1) duplicatesCount++;
        }

        addLog(`Verification complete. Checked ${recordsProcessed} records. Duplicates: ${duplicatesCount}. Monotonic: ${isStrictlyMonotonic}`);
        res.json({
            ordered: isStrictlyMonotonic,
            duplicates: duplicatesCount,
            messagesChecked: recordsProcessed
        });
    } catch(e) {
        addLog('Verification error: ' + String(e), 'error');
        res.status(500).json({ error: String(e) });
    }
});

const RESULTS_DIR = path.join(__dirname, '../../docs/results');

app.post('/api/simulate/save-run', async (req, res) => {
    try {
        await fs.mkdir(RESULTS_DIR, { recursive: true });
        const data = req.body;
        const runId = data.runId || Date.now().toString();
        const filePath = path.join(RESULTS_DIR, `${runId}.json`);
        await fs.writeFile(filePath, JSON.stringify(data, null, 2));
        addLog(`Run ${runId} saved successfully.`);
        res.json({ message: 'Run saved', runId });
    } catch (e) {
        console.error("Save Run Error:", e);
        res.status(500).json({ error: String(e) });
    }
});

app.get('/api/results', async (req, res) => {
    try {
        await fs.mkdir(RESULTS_DIR, { recursive: true });
        const files = await fs.readdir(RESULTS_DIR);
        const results = [];
        for (const file of files) {
            if (file.endsWith('.json')) {
                const content = await fs.readFile(path.join(RESULTS_DIR, file), 'utf-8');
                const data = JSON.parse(content);
                // Return just the summary for listing
                results.push({
                    runId: data.runId,
                    timestamp: data.timestamp,
                    approach: data.approach,
                    rps: data.rps,
                    elapsedSec: data.elapsedSec,
                    recordsModified: data.recordsModified,
                    recordsFailed: data.recordsFailed,
                    successRate: data.successRate,
                    p95: data.p95,
                    p99: data.p99
                });
            }
        }
        // Sort by timestamp desc
        results.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
        res.json(results);
    } catch (e) {
        console.error("List Results Error:", e);
        res.status(500).json({ error: String(e) });
    }
});

app.get('/api/results/:id', async (req, res) => {
    try {
        const filePath = path.join(RESULTS_DIR, `${req.params.id}.json`);
        const content = await fs.readFile(filePath, 'utf-8');
        res.json(JSON.parse(content));
    } catch (e) {
        console.error("Get Result Error:", e);
        res.status(404).json({ error: 'Result not found' });
    }
});

app.post('/api/logs/clear', (req, res) => {
    systemLogs = [];
    res.json({ message: 'Logs cleared' });
});

const PORT = 3001;

async function connectKafkaAdmin() {
    try {
        await admin.connect();
        console.log('Kafka admin connected');
    } catch (e) {
        console.error('Kafka admin connect failed, retrying in 5s...', e);
        setTimeout(connectKafkaAdmin, 5000);
    }
}

const lagConsumer = kafka.consumer({ groupId: 'lag-tracker-group-' + Date.now() });
async function startLagTracker() {
    try {
        await lagConsumer.connect();
        // The topic might not exist initially, so we just attempt to subscribe
        try {
            await lagConsumer.subscribe({ topic: 'sim.sim_db.dbo.billing_record', fromBeginning: false });
        } catch(e) {}
        
        await lagConsumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const now = Date.now();
                    const value = JSON.parse(message.value?.toString() || '{}');
                    if (value.payload && value.payload.source && value.payload.source.ts_ms) {
                        const ts_ms = value.payload.source.ts_ms;
                        globalCaptureLagMs = now - ts_ms;
                    }
                } catch(e) {}
            }
        });
    } catch (e) {
        console.error("Lag tracker error", e);
        setTimeout(startLagTracker, 5000);
    }
}

async function init() {
    // Connect the main thread to DB just for the DMV stats polling
    await connectDB();
    await connectKafkaAdmin();
    startLagTracker();
    
    app.listen(PORT, () => {
        console.log(`Simulator Orchestrator running on port ${PORT}`);
    });
}

process.on('SIGINT', async () => {
    console.log('\nShutting down gracefully...');
    for (const worker of workers) {
        worker.postMessage({ type: 'stop' });
        await worker.terminate();
    }
    // Stop consumer workers
    for (const cw of consumerWorkers) {
        try {
            cw.postMessage({ type: 'stop' });
            await cw.terminate();
        } catch(e) {}
    }
    try { await admin.disconnect(); } catch(e) {}
    const pool = getPool();
    if (pool) {
        try { await pool.close(); } catch(e) {}
    }
    process.exit(0);
});

init().catch(console.error);
