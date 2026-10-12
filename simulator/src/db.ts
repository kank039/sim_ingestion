import sql from 'mssql';
import { config } from './config';
import { createDatabaseSql, schemaBatches, backfillChangeTokensSql, ensureReferenceDataSql } from './schema';
import { ALL_CDC_TABLES } from './approaches';

export function dbConfig(overrides: Partial<sql.config> = {}, poolMax = 10): sql.config {
    return {
        user: config.db.user,
        password: config.db.password,
        server: config.db.host,
        port: config.db.port,
        database: config.db.database,
        options: { encrypt: true, trustServerCertificate: true },
        pool: { max: poolMax, min: 0, idleTimeoutMillis: 30_000 },
        requestTimeout: 30_000,
        ...overrides,
    };
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

/**
 * Opens a dedicated pool (never the mssql global pool, so threads/modules don't close each
 * other's connections), retrying until SQL Server is reachable.
 */
export async function openPool(poolMax: number, opts: { retries?: number; label?: string; overrides?: Partial<sql.config> } = {}): Promise<sql.ConnectionPool> {
    const retries = opts.retries ?? 60;
    let lastErr: unknown;
    for (let attempt = 0; attempt <= retries; attempt++) {
        const pool = new sql.ConnectionPool(dbConfig(opts.overrides, poolMax));
        try {
            await pool.connect();
            return pool;
        } catch (e) {
            lastErr = e;
            try { await pool.close(); } catch { /* ignore */ }
            if (attempt < retries) await sleep(2000);
        }
    }
    throw new Error(`${opts.label || 'DB'} connection failed after ${retries + 1} attempts: ${String((lastErr as Error)?.message || lastErr)}`);
}

/** Creates the database and applies the idempotent schema. Main thread only. */
export async function initializeDatabase(log: (msg: string) => void): Promise<sql.ConnectionPool> {
    const master = await openPool(1, { label: 'SQL Server (master)', overrides: { database: 'master' } });
    try {
        await master.request().query(createDatabaseSql(config.db.database));
    } finally {
        await master.close();
    }

    const pool = await openPool(20, { label: 'SQL Server' });
    for (const batch of schemaBatches) {
        await pool.request().query(batch.sql);
    }
    log('Database schema verified.');
    return pool;
}

/** mssql supports per-request timeout overrides at runtime, but its typings don't expose them. */
function longRequest(pool: sql.ConnectionPool, requestTimeout: number): sql.Request {
    return (pool as any).request({ requestTimeout }) as sql.Request;
}

export async function backfillChangeTokens(pool: sql.ConnectionPool) {
    await longRequest(pool, 600_000).query(backfillChangeTokensSql);
}

export async function ensureReferenceData(pool: sql.ConnectionPool, cardinality: number) {
    const req = longRequest(pool, 900_000);
    req.input('n', sql.Int, cardinality);
    await req.query(ensureReferenceDataSql);
}

export async function getCdcTrackedTables(pool: sql.ConnectionPool): Promise<string[]> {
    const res = await pool.request().query(`SELECT name FROM sys.tables WHERE is_tracked_by_cdc = 1`);
    return res.recordset.map((r: any) => r.name);
}

/** Enables CDC on exactly `wanted` (among the simulator's tables) and disables it everywhere else. */
export async function setCdcTables(pool: sql.ConnectionPool, wanted: string[], log: (msg: string) => void) {
    const tracked = new Set(await getCdcTrackedTables(pool));
    for (const table of ALL_CDC_TABLES) {
        if (tracked.has(table) && !wanted.includes(table)) {
            await pool.request().query(`EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'${table}', @capture_instance = N'all';`);
            log(`CDC disabled on dbo.${table}`);
        }
    }
    for (const table of wanted) {
        if (!tracked.has(table)) {
            await pool.request().query(`EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'${table}', @role_name = NULL, @supports_net_changes = 0;`);
            log(`CDC enabled on dbo.${table}`);
        }
    }
}

export async function setTriggerEnabled(pool: sql.ConnectionPool, enabled: boolean) {
    await pool.request().query(`${enabled ? 'ENABLE' : 'DISABLE'} TRIGGER trg_billing_record_cdc ON dbo.billing_record;`);
}

export async function isTriggerEnabled(pool: sql.ConnectionPool): Promise<boolean> {
    const res = await pool.request().query(`SELECT is_disabled FROM sys.triggers WHERE name = 'trg_billing_record_cdc'`);
    return res.recordset.length > 0 && !res.recordset[0].is_disabled;
}

/** TRUNCATE is rejected on CDC-enabled tables, so callers disable CDC on these tables first. */
export async function truncateTables(pool: sql.ConnectionPool, tables: string[]) {
    for (const t of tables) {
        await pool.request().query(`TRUNCATE TABLE dbo.${t};`);
    }
}

/** Makes sure the CDC capture job is running (it can be stopped after an agent restart). */
export async function startCdcCaptureJob(pool: sql.ConnectionPool) {
    try {
        await pool.request().query(`EXEC sys.sp_cdc_start_job @job_type = N'capture';`);
    } catch {
        // Already running: SQL Server raises an error, which is fine.
    }
}

let lastIO: { mb: number; at: number } | null = null;

const DB_STATS_SQL = `
SET NOCOUNT ON;
SELECT TOP 1
    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization
FROM (
    SELECT CAST(record AS XML) AS record
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' AND record LIKE '%<SystemHealth>%'
) AS x
ORDER BY record.value('(./Record/@id)[1]', 'int') DESC;
SELECT SUM(num_of_bytes_read + num_of_bytes_written) / 1024.0 / 1024.0 AS TotalIOMB FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL);
SELECT COUNT(*) AS WaitTasks FROM sys.dm_os_waiting_tasks WHERE session_id > 50;
SELECT COUNT(*) AS ActiveLocks FROM sys.dm_tran_locks WHERE request_session_id > 50 AND request_mode IN ('X', 'U', 'IX');
SELECT used_log_space_in_bytes / 1024.0 / 1024.0 AS LdfSizeMB FROM sys.dm_db_log_space_usage;
SELECT cntr_value AS PLE FROM sys.dm_os_performance_counters WHERE counter_name = 'Page life expectancy' AND object_name LIKE '%Buffer Manager%';
SELECT SUM(version_store_reserved_page_count + user_object_reserved_page_count + internal_object_reserved_page_count + mixed_extent_page_count) * 8.0 / 1024.0 AS TempDbMB FROM tempdb.sys.dm_db_file_space_usage;
SELECT COUNT(*) AS ActiveConnections FROM sys.dm_exec_sessions WHERE database_id = DB_ID() AND session_id > 50;`;

export interface DbStats {
    cpu: number;
    io: number;
    wait_tasks: number;
    active_locks: number;
    ldfSizeMB: number;
    ple: number;
    tempDbMB: number;
    activeConnections: number;
}

export const EMPTY_DB_STATS: DbStats = { cpu: 0, io: 0, wait_tasks: 0, active_locks: 0, ldfSizeMB: 0, ple: 0, tempDbMB: 0, activeConnections: 0 };

/** All DMV reads in one round trip. Throws on failure; the telemetry loop decides what to keep. */
export async function getDBStats(pool: sql.ConnectionPool): Promise<DbStats> {
    const res = await pool.request().query(DB_STATS_SQL);
    const sets = res.recordsets as any[];
    const first = (i: number, col: string) => Number(sets[i]?.[0]?.[col] ?? 0) || 0;

    const ioMB = first(1, 'TotalIOMB');
    const now = Date.now();
    let ioRate = 0;
    if (lastIO && ioMB >= lastIO.mb && now > lastIO.at) {
        ioRate = (ioMB - lastIO.mb) / ((now - lastIO.at) / 1000);
    }
    lastIO = { mb: ioMB, at: now };

    const round2 = (n: number) => Math.round(n * 100) / 100;
    return {
        cpu: first(0, 'SQLProcessUtilization'),
        io: round2(ioRate),
        wait_tasks: first(2, 'WaitTasks'),
        active_locks: first(3, 'ActiveLocks'),
        ldfSizeMB: round2(first(4, 'LdfSizeMB')),
        ple: first(5, 'PLE'),
        tempDbMB: round2(first(6, 'TempDbMB')),
        activeConnections: first(7, 'ActiveConnections'),
    };
}
