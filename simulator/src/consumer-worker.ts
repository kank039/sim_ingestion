import { parentPort, workerData } from 'worker_threads';
import sql from 'mssql';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

const workerId: number = workerData?.workerId ?? 0;
const assignedSubscribers: number = workerData?.assignedSubscribers ?? 1;
const baseSubscriberIndex: number = workerData?.baseSubscriberIndex ?? 0;

const MAX_LATENCY_HISTORY = 500;
let enrichmentLatencies: number[] = new Array(MAX_LATENCY_HISTORY);
let e2eLatencies: number[] = new Array(MAX_LATENCY_HISTORY);
let latencyIndex = 0;
let latencyCount = 0;
let messagesConsumed = 0;
let enrichmentsFailed = 0;

let isRunning = false;
let pool: sql.ConnectionPool;
let consumers: Consumer[] = [];

const dbConfig: sql.config = {
    user: 'sa',
    password: 'Password123!',
    server: process.env.DB_HOST || 'localhost',
    database: 'sim_db',
    options: {
        encrypt: true,
        trustServerCertificate: true
    },
    pool: {
        max: Math.min(50, assignedSubscribers * 5),  // Larger pool to handle concurrent queries from multiple subscribers
        min: 1
    }
};

const ENRICHMENT_QUERY = `
    SELECT
        br.id            AS billing_id,
        br.batch_id,
        br.amount,
        br.created_at,
        ib.invoice_number,
        ib.status        AS invoice_status,
        sp.plan_name,
        sp.plan_type,
        sp.base_rate,
        sp.discount_pct,
        su.usage_type,
        su.quantity,
        su.unit,
        rs.rate_per_unit,
        rs.min_charge,
        rs.max_charge
    FROM billing_record br WITH (NOLOCK)
    INNER JOIN invoice_batch ib WITH (NOLOCK)
        ON br.batch_id = ib.id
    INNER JOIN subscriber_plan sp WITH (NOLOCK)
        ON ib.plan_id = sp.id
    LEFT JOIN subscriber_usage su WITH (NOLOCK)
        ON br.batch_id = su.batch_id
    LEFT JOIN rate_schedule rs WITH (NOLOCK)
        ON sp.id = rs.plan_id
        AND su.usage_type = rs.usage_type
    WHERE br.batch_id = @batchId;
`;

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
    const enrichStats = calculatePercentiles(enrichmentLatencies, latencyCount);
    const e2eStats = calculatePercentiles(e2eLatencies, latencyCount);

    parentPort?.postMessage({
        type: 'consumer-stats',
        workerId,
        enrichmentAvg: enrichStats.avg,
        enrichmentP95: enrichStats.p95,
        enrichmentP99: enrichStats.p99,
        e2eAvg: e2eStats.avg,
        e2eP95: e2eStats.p95,
        e2eP99: e2eStats.p99,
        messagesConsumed,
        enrichmentsFailed
    });

    // Reset for next interval
    latencyIndex = 0;
    latencyCount = 0;
    messagesConsumed = 0;
    enrichmentsFailed = 0;
}, 1000);

async function connectDB() {
    try {
        pool = await sql.connect(dbConfig);
        console.log(`[Consumer ${workerId}] Connected to SQL Server`);
    } catch (e) {
        console.error(`[Consumer ${workerId}] DB connection failed:`, e);
        throw e;
    }
}

async function connectKafka() {
    const kafka = new Kafka({
        clientId: `consumer-worker-${workerId}`,
        brokers: [process.env.KAFKA_BROKERS || 'localhost:9092']
    });

    for (let i = 0; i < assignedSubscribers; i++) {
        const subIndex = baseSubscriberIndex + i;
        const groupId = `subscriber-${subIndex}`;
        const consumer = kafka.consumer({ groupId });
        await consumer.connect();
        await consumer.subscribe({ topic: 'sim.sim_db.dbo.billing_record', fromBeginning: false });
        consumers.push(consumer);
    }
    console.log(`[Consumer Thread ${workerId}] ${assignedSubscribers} Kafka consumers connected`);
}

async function handleMessage({ message }: EachMessagePayload) {
    if (!isRunning || !pool) return;

    const messageTimestamp = message.timestamp ? parseInt(message.timestamp, 10) : Date.now();
    
    let batchId: number | null = null;
    try {
        if (message.value) {
            const payload = JSON.parse(message.value.toString());
            // Debezium CDC format: payload.after contains the row data
            if (payload.payload?.after?.batch_id) {
                batchId = payload.payload.after.batch_id;
            } else if (payload.after?.batch_id) {
                batchId = payload.after.batch_id;
            } else if (payload.batch_id) {
                batchId = payload.batch_id;
            }
        }
    } catch (e) {
        // If we can't parse, use a random batch_id as fallback
        batchId = Math.floor(Math.random() * 100) + 1;
    }

    if (!batchId) {
        batchId = Math.floor(Math.random() * 100) + 1;
    }

    const enrichStart = performance.now();
    try {
        const request = pool.request();
        request.input('batchId', sql.Int, batchId);
        await request.query(ENRICHMENT_QUERY);

        const enrichEnd = performance.now();
        const enrichmentLatency = enrichEnd - enrichStart;
        const e2eLatency = Date.now() - messageTimestamp;

        enrichmentLatencies[latencyIndex] = enrichmentLatency;
        e2eLatencies[latencyIndex] = e2eLatency;
        latencyIndex = (latencyIndex + 1) % MAX_LATENCY_HISTORY;
        if (latencyCount < MAX_LATENCY_HISTORY) latencyCount++;

        messagesConsumed++;
    } catch (e) {
        enrichmentsFailed++;
        console.error(`[Consumer ${workerId}] Enrichment failed:`, e);
    }
}

async function startConsuming() {
    await Promise.all(consumers.map(consumer => 
        consumer.run({
            eachMessage: handleMessage
        })
    ));
}

// Listen for messages from main thread
parentPort?.on('message', async (msg) => {
    if (msg.type === 'start') {
        isRunning = true;
        try {
            await connectDB();
            await connectKafka();
            await startConsuming();
            parentPort?.postMessage({ type: 'consumer-ready', workerId, assignedSubscribers });
        } catch (e) {
            console.error(`[Consumer ${workerId}] Start failed:`, e);
            parentPort?.postMessage({ type: 'consumer-error', workerId, error: String(e) });
        }
    } else if (msg.type === 'stop') {
        isRunning = false;
        try {
            await Promise.all(consumers.map(c => c.disconnect()));
            consumers = [];
            if (pool) await pool.close();
        } catch (e) {
            console.error(`[Consumer ${workerId}] Cleanup error:`, e);
        }
    } else if (msg.type === 'reconnect') {
        try {
            await Promise.all(consumers.map(c => c.disconnect()));
            consumers = [];
            if (pool) await pool.close();
        } catch (e) {
            console.error(`[Consumer ${workerId}] Cleanup before reconnect error:`, e);
        }
        try {
            await connectDB();
            await connectKafka();
            if (isRunning) {
                await startConsuming();
            }
        } catch (e) {
            console.error(`[Consumer ${workerId}] Reconnect failed:`, e);
        }
    }
});
