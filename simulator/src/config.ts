import path from 'path';
import dotenv from 'dotenv';

// Load .env from the repo root first, then simulator/.env (later values do not override earlier ones).
dotenv.config({ path: path.resolve(__dirname, '../../.env'), quiet: true });
dotenv.config({ quiet: true });

const env = process.env;

function num(name: string, fallback: number): number {
    const raw = env[name];
    if (raw === undefined || raw === '') return fallback;
    const n = Number(raw);
    return Number.isFinite(n) ? n : fallback;
}

export const config = {
    port: num('PORT', 3001),

    // How the simulator itself reaches SQL Server.
    db: {
        host: env.DB_HOST || 'localhost',
        port: num('DB_PORT', 1433),
        user: env.DB_USER || 'sa',
        password: env.DB_PASS || 'Password123!',
        database: env.DB_NAME || 'sim_db',
    },

    // How the simulator reaches Kafka / Kafka Connect / Flink / Docker.
    kafkaBrokers: (env.KAFKA_BROKERS || 'localhost:9092').split(',').map(s => s.trim()).filter(Boolean),
    debeziumUrl: env.DEBEZIUM_URL || 'http://localhost:8083',
    flinkUrl: env.FLINK_URL || 'http://localhost:8081',
    dockerSocket: env.DOCKER_SOCKET || (process.platform === 'win32' ? '\\\\.\\pipe\\docker_engine' : '/var/run/docker.sock'),
    containers: {
        debezium: env.DEBEZIUM_CONTAINER || 'debezium',
        flinkJobManager: env.FLINK_JOBMANAGER_CONTAINER || 'flink-jobmanager',
    },

    // How services *inside* the Docker network reach each other (used in connector / Flink configs).
    internal: {
        dbHost: env.CONNECTOR_DB_HOST || 'sqlserver',
        dbPort: num('CONNECTOR_DB_PORT', 1433),
        kafkaBootstrap: env.CONNECTOR_KAFKA_BOOTSTRAP || 'kafka:29092',
    },

    load: {
        writerThreads: num('WRITER_THREADS', 8),
        concurrencyPerThread: num('WRITER_CONCURRENCY', 50),
        queueLimitPerThread: num('WRITER_QUEUE_LIMIT', 5000),
        consumerThreads: num('CONSUMER_THREADS', 4),
        maxCardinality: 1_000_000,
    },

    timeouts: {
        connectorReadyMs: num('CONNECTOR_READY_TIMEOUT_MS', 90_000),
        probeMs: num('PROBE_TIMEOUT_MS', 120_000),
        drainMs: num('DRAIN_TIMEOUT_MS', 60_000),
    },

    resultsDir: env.RESULTS_DIR || path.resolve(__dirname, '../../docs/results'),
};

export type Config = typeof config;
