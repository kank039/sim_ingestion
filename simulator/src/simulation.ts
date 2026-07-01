import { parentPort } from 'worker_threads';
import { connectDB, getPool } from './db';
import sql from 'mssql';

let isRunning = false;
let currentApproach = 1;
let currentRps = 10;
let timeoutMs = 3000;
let isInsertsOnly = false;
let cardinality = 100;
let insertWeight = 0.5;
let updateWeight = 0.3;
let deleteWeight = 0.2;

const MAX_LATENCY_HISTORY = 500;
let dbLatencyHistory: number[] = new Array(MAX_LATENCY_HISTORY);
let queueLatencyHistory: number[] = new Array(MAX_LATENCY_HISTORY);
let latencyIndex = 0;
let latencyCount = 0;

let recordsModified = 0;
let recordsFailed = 0;

// Connect to DB and notify parent
connectDB().then(() => {
    parentPort?.postMessage({ type: 'ready' });
});

parentPort?.on('message', (msg) => {
    if (msg.type === 'start') {
        currentApproach = msg.approach;
        currentRps = msg.rps; // RPS assigned to this specific worker
        timeoutMs = msg.timeoutMs || 3000;
        isInsertsOnly = !!msg.insertsOnly;
        if (msg.cardinality) cardinality = msg.cardinality;
        if (msg.insertWeight !== undefined) insertWeight = msg.insertWeight;
        if (msg.updateWeight !== undefined) updateWeight = msg.updateWeight;
        if (msg.deleteWeight !== undefined) deleteWeight = msg.deleteWeight;
        
        if (!isRunning) {
            isRunning = true;
            simulationLoop();
        }
    } else if (msg.type === 'stop') {
        isRunning = false;
    } else if (msg.type === 'reconnect') {
        connectDB().catch(console.error);
    }
});

function calculatePercentiles(latencies: number[], count: number) {
    if (count === 0) return { avg: 0, p50: 0, p95: 0, p99: 0 };
    const validLatencies = latencies.slice(0, count).sort((a, b) => a - b);
    const sum = validLatencies.reduce((a, b) => a + b, 0);
    return {
        avg: sum / count,
        p50: validLatencies[Math.floor(count * 0.50)],
        p95: validLatencies[Math.floor(count * 0.95)],
        p99: validLatencies[Math.floor(count * 0.99)]
    };
}

// Periodically send stats to the main thread
setInterval(() => {
    const dbStats = calculatePercentiles(dbLatencyHistory, latencyCount);
    const queueStats = calculatePercentiles(queueLatencyHistory, latencyCount);
    
    parentPort?.postMessage({ 
        type: 'stats', 
        avgLatency: dbStats.avg,
        p50: dbStats.p50,
        p95: dbStats.p95,
        p99: dbStats.p99,
        queueAvg: queueStats.avg,
        count: latencyCount,
        modified: recordsModified,
        failed: recordsFailed
    });
    // Reset for next batch
    latencyIndex = 0;
    latencyCount = 0;
    recordsModified = 0;
    recordsFailed = 0;
}, 1000);

function createLimiter(concurrency: number) {
    let active = 0;
    const queue: (() => void)[] = [];
    return async <T>(fn: () => Promise<T>): Promise<T> => {
        if (active >= concurrency) {
            await new Promise<void>(resolve => queue.push(resolve));
        }
        active++;
        try {
            return await fn();
        } finally {
            active--;
            if (queue.length > 0) {
                const next = queue.shift();
                if (next) next();
            }
        }
    };
}

const limit = createLimiter(50);

async function simulationLoop() {
    while (isRunning) {
        // Divide RPS into 10 chunks per second
        const batchSize = Math.max(1, Math.floor(currentRps / 10)); 
        
        for (let i = 0; i < batchSize; i++) {
            const startWait = performance.now();
            limit(async () => {
                const waitTime = performance.now() - startWait;
                if (waitTime > timeoutMs) {
                    recordsFailed++;
                    return;
                }
                try {
                    const execPromise = executeOperation(waitTime);
                    const timeoutPromise = new Promise((_, reject) => 
                        setTimeout(() => reject(new Error('TIMEOUT')), Math.max(0, timeoutMs - waitTime))
                    );
                    await Promise.race([execPromise, timeoutPromise]);
                    recordsModified++;
                } catch (e: any) {
                    if (e.message === 'TIMEOUT') {
                        recordsFailed++;
                    } else {
                        console.error("Op Error:", e);
                    }
                }
            }).catch(e => console.error("Limit Error:", e));
        }
        
        await new Promise(resolve => setTimeout(resolve, 100)); // Sleep 100ms
    }
}

async function executeOperation(waitTime: number) {
    const pool = getPool();
    if (!pool) return;
    
    const batchId = Math.floor(Math.random() * cardinality) + 1;
    const amount = (Math.random() * 1000).toFixed(2);
    const payload = JSON.stringify({ batchId, amount, invoiceNumber: `INV-${1000 + batchId}` });
    
    const start = performance.now();
    
    let operationType = 'insert';
    if (!isInsertsOnly) {
        const rand = Math.random();
        if (rand < deleteWeight) {
            operationType = 'delete';
        } else if (rand < deleteWeight + updateWeight) {
            operationType = 'update';
        }
    }

    try {
        if (currentApproach === 2) {
            // Transactional Outbox
            const transaction = new sql.Transaction(pool);
            await transaction.begin();
            const request = new sql.Request(transaction);
            request.input('batchId', sql.Int, batchId);
            request.input('amount', sql.Decimal(10, 2), amount);
            request.input('payload', sql.NVarChar, payload);
            
            if (operationType === 'delete') {
                const res = await request.query(`DELETE TOP (1) FROM billing_record WHERE batch_id = @batchId;`);
                if (res.rowsAffected[0] === 0) {
                    await request.query(`
                        INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount);
                        INSERT INTO outbox_events (aggregate_id, payload) VALUES (@batchId, @payload);
                    `);
                } else {
                    await request.query(`INSERT INTO outbox_events (aggregate_id, payload) VALUES (@batchId, @payload);`);
                }
            } else if (operationType === 'update') {
                const res = await request.query(`UPDATE TOP (1) billing_record SET amount = @amount WHERE batch_id = @batchId;`);
                if (res.rowsAffected[0] === 0) {
                    await request.query(`
                        INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount);
                        INSERT INTO outbox_events (aggregate_id, payload) VALUES (@batchId, @payload);
                    `);
                } else {
                    await request.query(`INSERT INTO outbox_events (aggregate_id, payload) VALUES (@batchId, @payload);`);
                }
            } else {
                await request.query(`
                    INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount);
                    INSERT INTO outbox_events (aggregate_id, payload) VALUES (@batchId, @payload);
                `);
            }
            await transaction.commit();
        } else {
            // Triggers / Flink / SMT Approaches
            const request = pool.request();
            request.input('batchId', sql.Int, batchId);
            request.input('amount', sql.Decimal(10, 2), amount);
            
            let didWrite = false;
            
            if (operationType === 'delete') {
                const res = await request.query(`DELETE TOP (1) FROM billing_record WHERE batch_id = @batchId`);
                if (res.rowsAffected[0] === 0) {
                    await request.query(`INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount)`);
                }
                didWrite = true;
            } else if (operationType === 'update') {
                const res = await request.query(`UPDATE TOP (1) billing_record SET amount = @amount WHERE batch_id = @batchId`);
                if (res.rowsAffected[0] === 0) {
                    await request.query(`INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount)`);
                }
                didWrite = true;
            } else {
                await request.query(`INSERT INTO billing_record (batch_id, amount) VALUES (@batchId, @amount)`);
                didWrite = true;
            }
            
            if (didWrite && currentApproach === 3) {
                // Flink: Simulate dual-write pattern
                const req2 = pool.request();
                req2.input('batchId', sql.Int, batchId);
                req2.input('payload', sql.NVarChar, payload);
                await req2.query(`INSERT INTO enrichment_requests (batch_id, payload) VALUES (@batchId, @payload)`);
            }
            
            if (didWrite && currentApproach === 4) {
                // SMT: Simulate JDBC lookup AFTER write, with artificial latency
                await new Promise(resolve => setTimeout(resolve, 5)); // Artificial latency
                const req3 = pool.request();
                req3.input('batchId', sql.Int, batchId);
                await req3.query(`SELECT invoice_number FROM invoice_batch WHERE id = @batchId`);
            }
        }
    } catch (e) {
        console.error(e);
        throw e;
    }

    const end = performance.now();
    
    dbLatencyHistory[latencyIndex] = (end - start);
    queueLatencyHistory[latencyIndex] = waitTime;
    latencyIndex = (latencyIndex + 1) % MAX_LATENCY_HISTORY;
    if (latencyCount < MAX_LATENCY_HISTORY) latencyCount++;
}
