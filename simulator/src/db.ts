import sql from 'mssql';
import { initSqlBatches } from './initSql';

const config: sql.config = {
    user: 'sa',
    password: 'Password123!',
    server: process.env.DB_HOST || 'localhost',
    database: 'sim_db',
    options: {
        encrypt: true,
        trustServerCertificate: true
    },
    pool: {
        max: 2000,
        min: 1
    }
};

let pool: sql.ConnectionPool;
let lastIOMB = 0;
let lastIOTime = Date.now();

export async function connectDB() {
    try {
        if (pool) await pool.close();
    } catch(e) {}
    
    try {
        pool = await sql.connect(config);
        console.log("Connected to SQL Server");
    } catch (e: any) {
        if (e.code === 'ELOGIN' || e.message?.includes('Login failed')) {
            console.warn("Database 'sim_db' might not exist. Attempting to initialize it...");
            await initializeDatabaseAndConnector();
            // Retry connecting to sim_db after initialization
            pool = await sql.connect(config);
            console.log("Connected to SQL Server after initialization");
        } else {
            throw e;
        }
    }

    await populateInitialData(pool);
}

export async function populateInitialData(pool: sql.ConnectionPool) {
    // Insert some initial data to invoice_batch (ignore errors if other workers are doing it)
    try {
        for (let i = 1; i <= 100; i++) {
            const planId = ((i - 1) % 100) + 1;
            await pool.request().query(`
                IF NOT EXISTS (SELECT 1 FROM invoice_batch WHERE id = ${i})
                BEGIN
                    INSERT INTO invoice_batch (id, invoice_number, plan_id) VALUES (${i}, 'INV-${1000 + i}', ${planId})
                END
            `);
        }
        // Ensure plan_id is set for any existing rows (e.g., from earlier init without plan_id)
        await pool.request().query(`
            UPDATE invoice_batch SET plan_id = ((id - 1) % 100) + 1 WHERE plan_id IS NULL
        `);
    } catch (e) {
        // ignore
    }
}

async function initializeDatabaseAndConnector() {
    const masterConfig = { ...config, database: undefined }; // Connect to default database (master)
    let tempPool: sql.ConnectionPool | null = null;
    try {
        tempPool = await sql.connect(masterConfig);
        console.log("Connected to master DB. Creating sim_db...");
        
        try {
            await tempPool.request().query(initSqlBatches[0]); // CREATE DATABASE
        } catch (e: any) {
            console.log("Database creation skipped or failed:", e.message);
        }

        await tempPool.close();
        
        // Reconnect directly to sim_db to run the rest of the scripts
        tempPool = await sql.connect(config);
        console.log("Connected to sim_db. Running initialization scripts...");

        for (let i = 1; i < initSqlBatches.length; i++) {
            const batch = initSqlBatches[i].trim();
            if (batch === 'USE sim_db;') continue;
            try {
                await tempPool.request().query(batch);
            } catch (err: any) {
                console.error("Error executing SQL batch:", err.message);
            }
        }
        console.log("Database initialized successfully.");
    } catch (err) {
        console.error("Failed to initialize database:", err);
        throw err;
    } finally {
        if (tempPool) {
            await tempPool.close();
        }
    }

    console.log("Registering Debezium connector...");
    const debeziumConfig = {
        name: "billing_record_connector",
        config: {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": "sqlserver",
            "database.port": "1433",
            "database.user": "sa",
            "database.password": "Password123!",
            "database.names": "sim_db",
            "topic.prefix": "sim",
            "table.include.list": "dbo.billing_record,dbo.invoice_batch,dbo.cdc_events_shadow,dbo.outbox_events",
            "database.encrypt": "false",
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
            "schema.history.internal.kafka.topic": "schema-changes.billing.reset"
        }
    };

    try {
        // First delete it if it exists (ignore errors if it doesn't)
        await fetch("http://debezium:8083/connectors/billing_record_connector", { method: "DELETE" }).catch(() => {});
        
        // Then register it
        const res = await fetch("http://debezium:8083/connectors", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(debeziumConfig)
        });
        
        if (!res.ok) {
            const text = await res.text();
            console.error("Failed to register Debezium connector:", text);
        } else {
            console.log("Debezium connector registered successfully.");
        }
    } catch (err) {
        console.error("Failed to communicate with Debezium:", err);
    }
}

export function getPool() {
    return pool;
}

export async function getDBStats() {
    if (!pool) return { cpu: 0, io: 0 };
    
    // Simplistic CPU polling from dm_os_ring_buffers
    const cpuQuery = `
        SELECT TOP 1 
            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as SystemIdle,
            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as SQLProcessUtilization 
        FROM (
            SELECT CAST(record AS XML) AS record 
            FROM sys.dm_os_ring_buffers 
            WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
            AND record LIKE '%<SystemHealth>%'
        ) AS x 
        ORDER BY record.value('(./Record/@id)[1]', 'int') DESC;
    `;
    
    // Simplistic IO polling
    const ioQuery = `
        SELECT SUM(num_of_bytes_read + num_of_bytes_written) / 1024 / 1024 as TotalIOMB
        FROM sys.dm_io_virtual_file_stats(DB_ID('sim_db'), NULL);
    `;

    try {
        const cpuRes = await pool.request().query(cpuQuery);
        const ioRes = await pool.request().query(ioQuery);
        
        const waitRes = await pool.request().query(`SELECT count(*) as WaitTasks FROM sys.dm_os_waiting_tasks WHERE session_id > 50;`);
        const lockRes = await pool.request().query(`SELECT count(*) as ActiveLocks FROM sys.dm_tran_locks WHERE request_session_id > 50 AND request_mode IN ('X', 'U', 'IX');`);
        const ldfRes = await pool.request().query(`SELECT (used_log_space_in_bytes / 1024.0 / 1024.0) as LdfSizeMB FROM sys.dm_db_log_space_usage;`);
        const pleRes = await pool.request().query(`SELECT cntr_value as PLE FROM sys.dm_os_performance_counters WHERE counter_name = 'Page life expectancy' AND object_name LIKE '%Buffer Manager%';`);
        const tempdbRes = await pool.request().query(`SELECT SUM(unallocated_extent_page_count + version_store_reserved_page_count + user_object_reserved_page_count + internal_object_reserved_page_count + mixed_extent_page_count) * 8.0 / 1024.0 as TempDbMB FROM tempdb.sys.dm_db_file_space_usage;`);
        const connRes = await pool.request().query(`SELECT count(*) as ActiveConnections FROM sys.dm_exec_sessions WHERE database_id = DB_ID('sim_db') AND session_id > 50;`);

        const currentIOMB = ioRes.recordset[0]?.TotalIOMB || 0;
        const now = Date.now();
        
        let deltaMB = 0;
        if (lastIOMB > 0 && currentIOMB >= lastIOMB) {
            const elapsedSec = (now - lastIOTime) / 1000;
            if (elapsedSec > 0) {
                deltaMB = (currentIOMB - lastIOMB) / elapsedSec;
            }
        }
        
        lastIOMB = currentIOMB;
        lastIOTime = now;

        return {
            cpu: cpuRes.recordset[0]?.SQLProcessUtilization || 0,
            io: Math.round(deltaMB * 100) / 100, // rounded to 2 decimals
            wait_tasks: waitRes.recordset[0]?.WaitTasks || 0,
            active_locks: lockRes.recordset[0]?.ActiveLocks || 0,
            ldfSizeMB: Math.round((ldfRes.recordset[0]?.LdfSizeMB || 0) * 100) / 100,
            ple: pleRes.recordset[0]?.PLE || 0,
            tempDbMB: Math.round((tempdbRes.recordset[0]?.TempDbMB || 0) * 100) / 100,
            activeConnections: connRes.recordset[0]?.ActiveConnections || 0
        };
    } catch (e: any) {
        require('fs').appendFileSync('d:\\tmp\\db_error.log', new Date().toISOString() + ': ' + e.message + '\n' + (e.stack || '') + '\n');
        console.error("Failed to get DB stats", e);
        return { cpu: 0, io: 0, wait_tasks: 0, active_locks: 0, ldfSizeMB: 0, ple: 0, tempDbMB: 0, activeConnections: 0 };
    }
}
