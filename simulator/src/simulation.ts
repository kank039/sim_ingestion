import { parentPort } from 'worker_threads';
import { connectDB, getPool } from './db';
import sql from 'mssql';

let isRunning = false;
let currentApproach = 1;
let currentRps = 10;

const MAX_LATENCY_HISTORY = 500;
let latencyHistory: number[] = new Array(MAX_LATENCY_HISTORY);
let latencyIndex = 0;
let latencyCount = 0;

let recordsPushed = 0;
let recordsFailed = 0;
let timeoutMs = 3000;

// Connect to DB and notify parent
connectDB().then(() => {
    parentPort?.postMessage({ type: 'ready' });
});

parentPort?.on('message', (msg) => {
    if (msg.type === 'start') {
        currentApproach = msg.approach;
        currentRps = msg.rps; // RPS assigned to this specific worker
        timeoutMs = msg.timeoutMs || 3000;
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

// Periodically send stats to the main thread
setInterval(() => {
    let sum = 0;
    for(let i = 0; i < latencyCount; i++) sum += latencyHistory[i];
    const avg = latencyCount > 0 ? sum / latencyCount : 0;
    parentPort?.postMessage({ 
        type: 'stats', 
        avgLatency: avg, 
        count: latencyCount,
        pushed: recordsPushed,
        failed: recordsFailed
    });
    // Reset for next batch
    latencyIndex = 0;
    latencyCount = 0;
    recordsPushed = 0;
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
                    const execPromise = executeOperation();
                    const timeoutPromise = new Promise((_, reject) => 
                        setTimeout(() => reject(new Error('TIMEOUT')), Math.max(0, timeoutMs - waitTime))
                    );
                    await Promise.race([execPromise, timeoutPromise]);
                    recordsPushed++;
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

async function executeOperation() {
    const pool = getPool();
    if (!pool) return;
    
    const batchId = Math.floor(Math.random() * 100) + 1;
    const amount = (Math.random() * 1000).toFixed(2);
    const start = performance.now();
    
    const operation = Math.random();

    try {
        if (currentApproach === 2) {
            // Transactional Outbox
            const transaction = new sql.Transaction(pool);
            await transaction.begin();
            const request = new sql.Request(transaction);
            
            const payload = JSON.stringify({ batchId, amount, invoiceNumber: `INV-${1000 + batchId}` });
            
            if (operation > 0.8) {
                // Delete
                await request.query(`
                    DELETE FROM billing_record WHERE batch_id = ${batchId};
                    INSERT INTO outbox_events (aggregate_id, payload) VALUES (${batchId}, '${payload}');
                `);
            } else if (operation > 0.5) {
                // Update
                await request.query(`
                    UPDATE billing_record SET amount = ${amount} WHERE batch_id = ${batchId};
                    INSERT INTO outbox_events (aggregate_id, payload) VALUES (${batchId}, '${payload}');
                `);
            } else {
                // Insert
                await request.query(`
                    INSERT INTO billing_record (batch_id, amount) VALUES (${batchId}, ${amount});
                    INSERT INTO outbox_events (aggregate_id, payload) VALUES (${batchId}, '${payload}');
                `);
            }
            
            await transaction.commit();
        } else {
            // Triggers / Flink / SMT Approaches
            if (operation > 0.8) {
                await pool.request().query(`DELETE FROM billing_record WHERE batch_id = ${batchId}`);
            } else if (operation > 0.5) {
                await pool.request().query(`UPDATE billing_record SET amount = ${amount} WHERE batch_id = ${batchId}`);
            } else {
                await pool.request().query(`INSERT INTO billing_record (batch_id, amount) VALUES (${batchId}, ${amount})`);
            }
        }
    } catch (e) {
        console.error(e);
        throw e;
    }

    const end = performance.now();
    
    latencyHistory[latencyIndex] = (end - start);
    latencyIndex = (latencyIndex + 1) % MAX_LATENCY_HISTORY;
    if (latencyCount < MAX_LATENCY_HISTORY) latencyCount++;
}
