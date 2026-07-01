import sql from 'mssql';

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
        max: 200,
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
    pool = await sql.connect(config);
    console.log("Connected to SQL Server");
    // Insert some initial data to invoice_batch (ignore errors if other workers are doing it)
    try {
        for (let i = 1; i <= 100; i++) {
            await pool.request().query(`
                IF NOT EXISTS (SELECT 1 FROM invoice_batch WHERE id = ${i})
                BEGIN
                    INSERT INTO invoice_batch (id, invoice_number) VALUES (${i}, 'INV-${1000 + i}')
                END
            `);
        }
    } catch (e) {
        // ignore
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
            active_locks: lockRes.recordset[0]?.ActiveLocks || 0
        };
    } catch (e) {
        console.error("Failed to get DB stats", e);
        return { cpu: 0, io: 0, wait_tasks: 0, active_locks: 0 };
    }
}
