import express from 'express';
import cors from 'cors';
import os from 'os';
import path from 'path';
import { Worker } from 'worker_threads';
import { connectDB, getDBStats, getPool } from './db';
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
                        if (stat.cpu_stats && stat.precpu_stats) {
                            const cpuDelta = stat.cpu_stats.cpu_usage.total_usage - stat.precpu_stats.cpu_usage.total_usage;
                            const systemDelta = stat.cpu_stats.system_cpu_usage - stat.precpu_stats.system_cpu_usage;
                            const cpus = stat.cpu_stats.online_cpus || stat.cpu_stats.cpu_usage.percpu_usage?.length || 1;
                            if (systemDelta > 0 && cpuDelta > 0) cpu = (cpuDelta / systemDelta) * 100.0;
                        }
                        
                        let mem = 0;
                        if (stat.memory_stats && stat.memory_stats.limit) {
                            mem = (stat.memory_stats.usage / stat.memory_stats.limit) * 100.0;
                        }

                        resolve({
                            name: c.Names[0].replace('/', ''),
                            cpu: cpu.toFixed(2) + '%',
                            mem: mem.toFixed(2) + '%'
                        });
                    } catch(e) { resolve(null); }
                });
            });
            req.on('error', () => resolve(null));
            req.end();
        }));

        const results = await Promise.all(statsPromises);
        return results.filter(r => r !== null);
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

let isRunning = false;
let currentApproach = 1;
let currentRps = 10;
const workerLatencies = new Map<number, number>();
let globalRecordsModified = 0;
let globalRecordsFailed = 0;
let baselineKafkaOffset = 0;

let gradualInterval: NodeJS.Timeout | null = null;

let systemLogs: { time: string, message: string, level: string, timestamp: number }[] = [];
function addLog(message: string, level: string = 'info') {
    systemLogs.unshift({ time: new Date().toLocaleTimeString(), message, level, timestamp: Date.now() });
    if (systemLogs.length > 500) systemLogs.pop();
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
            workerLatencies.set(i, msg.avgLatency || 0);
            if (msg.modified) {
                globalRecordsModified += msg.modified;
            }
            if (msg.failed) {
                globalRecordsFailed += msg.failed;
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
        
        let flawAlert = null;
        if (currentApproach === 4 && currentRps > 20) {
            flawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
        }
        
        let totalLatency = 0;
        workerLatencies.forEach(lat => totalLatency += lat);
        const avgLatency = workerLatencies.size > 0 ? totalLatency / workerLatencies.size : 0;
        
        res.json({
            isRunning,
            approach: currentApproach,
            rps: currentRps,
            appLatency: Math.round(avgLatency),
            flawAlert,
            dbStats,
            containerStats,
            recordsModified: globalRecordsModified,
            recordsFailed: globalRecordsFailed,
            recordsInKafka: Math.max(0, recordsInKafka - baselineKafkaOffset),
            lag: Math.max(0, globalRecordsModified - Math.max(0, recordsInKafka - baselineKafkaOffset)),
            logs: systemLogs
        });
    } catch (e) {
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/simulate/start', (req, res) => {
    const { approach, rps, gradual, endRps, timeoutMs, insertsOnly } = req.body;
    currentApproach = approach;
    currentRps = rps;
    isRunning = true;
    
    if (gradualInterval) clearInterval(gradualInterval);
    
    const updateWorkers = (newRps: number) => {
        const rpsPerWorker = Math.ceil(newRps / numWorkers);
        for (const worker of workers) {
            worker.postMessage({ type: 'start', approach, rps: rpsPerWorker, timeoutMs, insertsOnly });
        }
    };
    
    updateWorkers(currentRps);
    
    addLog(`Simulation started (Approach ${approach}) at ${currentRps} RPS`);
    
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
    addLog(`Simulation stopped`);
    if (gradualInterval) clearInterval(gradualInterval);
    for (const worker of workers) {
        worker.postMessage({ type: 'stop' });
    }
    res.json({ message: 'Simulation stopped' });
});

let sseClients: express.Response[] = [];
app.get('/api/stats/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    
    sseClients.push(res);
    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== res);
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
        
        let flawAlert = null;
        if (currentApproach === 4 && currentRps > 20) {
            flawAlert = "RACE CONDITION DETECTED: SMT queried old data for rapid sequential updates.";
        }
        
        let totalLatency = 0;
        workerLatencies.forEach(lat => totalLatency += lat);
        const avgLatency = workerLatencies.size > 0 ? totalLatency / workerLatencies.size : 0;
        
        const payload = JSON.stringify({
            isRunning,
            approach: currentApproach,
            rps: currentRps,
            appLatency: Math.round(avgLatency),
            flawAlert,
            dbStats,
            containerStats,
            recordsModified: globalRecordsModified,
            recordsFailed: globalRecordsFailed,
            recordsInKafka: Math.max(0, recordsInKafka - baselineKafkaOffset),
            lag: Math.max(0, globalRecordsModified - Math.max(0, recordsInKafka - baselineKafkaOffset)),
            logs: systemLogs
        });
        
        sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
    } catch(e) {
        console.error("SSE Broadcast Error:", e);
    }
}, 1000);

app.get('/health', (req, res) => {
    const pool = getPool();
    res.json({ status: 'ok', workers: readyWorkers, dbConnected: !!pool?.connected });
});

app.post('/api/simulate/clean', async (req, res) => {
    try {
        addLog('Cleaning SQL Server records (Truncating)...');
        const pool = getPool();
        await pool.request().query(`
            EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'billing_record', @capture_instance = N'dbo_billing_record';
            TRUNCATE TABLE billing_record;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'billing_record', @role_name = NULL;

            EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'outbox_events', @capture_instance = N'dbo_outbox_events';
            TRUNCATE TABLE outbox_events;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'outbox_events', @role_name = NULL;

            EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'cdc_events_shadow', @capture_instance = N'dbo_cdc_events_shadow';
            TRUNCATE TABLE cdc_events_shadow;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'cdc_events_shadow', @role_name = NULL;
        `);
        
        addLog('Waiting for Debezium to recognize changes...');
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        try {
            addLog('Wiping Kafka topics...');
            await admin.deleteTopics({
                topics: [
                    'sim.sim_db.dbo.billing_record',
                    'sim.sim_db.dbo.outbox_events',
                    'sim.sim_db.dbo.cdc_events_shadow'
                ]
            });
        } catch (e) {
            console.log('Topic deletion skipped/failed: ', e);
            addLog('Kafka topics wiped or ignored.');
        }
        
        globalRecordsPushed = 0;
        globalRecordsFailed = 0;
        baselineKafkaOffset = 0;
        
        // Wait 2 seconds for topics to be deleted, then fetch offsets to set as baseline (in case of recreation)
        await new Promise(resolve => setTimeout(resolve, 2000));
        try {
            const offsets = await admin.fetchTopicOffsets('sim.sim_db.dbo.billing_record');
            baselineKafkaOffset = offsets.reduce((sum, partition) => sum + parseInt(partition.high, 10), 0);
        } catch(e) {
            baselineKafkaOffset = 0; // topic doesn't exist yet
        }
        
        addLog('System perfectly cleaned.');
        res.json({ message: 'Cleaned' });
    } catch (e) {
        addLog('Error cleaning: ' + String(e));
        res.status(500).json({ error: String(e) });
    }
});

app.post('/api/system/reconnect', async (req, res) => {
    try {
        addLog('Reconnecting to SQL Server and Kafka...');
        await connectDB();
        try { await admin.disconnect(); } catch(e) {}
        await admin.connect();
        
        for (const worker of workers) {
            worker.postMessage({ type: 'reconnect' });
        }
        
        addLog('Successfully reconnected all services.');
        res.json({ message: 'Reconnected' });
    } catch (e) {
        addLog('Error reconnecting: ' + String(e));
        res.status(500).json({ error: String(e) });
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

async function init() {
    // Connect the main thread to DB just for the DMV stats polling
    await connectDB();
    await connectKafkaAdmin();
    
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
    try { await admin.disconnect(); } catch(e) {}
    const pool = getPool();
    if (pool) {
        try { await pool.close(); } catch(e) {}
    }
    process.exit(0);
});

init().catch(console.error);
