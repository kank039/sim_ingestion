import { parentPort } from 'worker_threads';
import { connectDB, getPool } from './db';
import sql from 'mssql';

let isRunning = false;
let currentApproach = 1;
let currentRps = 10;
let latencyHistory: number[] = [];
let recordsPushed = 0;

// Connect to DB and notify parent
connectDB().then(() => {
    parentPort?.postMessage({ type: 'ready' });
});

parentPort?.on('message', (msg) => {
    if (msg.type === 'start') {
        currentApproach = msg.approach;
        currentRps = msg.rps; // RPS assigned to this specific worker
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
    const sum = latencyHistory.reduce((a, b) => a + b, 0);
    const avg = latencyHistory.length > 0 ? sum / latencyHistory.length : 0;
    parentPort?.postMessage({ 
        type: 'stats', 
        avgLatency: avg, 
        count: latencyHistory.length,
        pushed: recordsPushed
    });
    latencyHistory = []; // Reset for next batch
    recordsPushed = 0;
}, 1000);

async function simulationLoop() {
    while (isRunning) {
        // Divide RPS into 10 chunks per second
        const batchSize = Math.max(1, Math.floor(currentRps / 10)); 
        
        for (let i = 0; i < batchSize; i++) {
            executeOperation().catch(e => console.error("Op Error:", e));
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
        recordsPushed++;
    } catch (e) {
        console.error(e);
    }

    const end = performance.now();
    latencyHistory.push(end - start);
    if (latencyHistory.length > 500) latencyHistory.shift();
}
