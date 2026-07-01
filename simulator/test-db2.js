const sql = require('mssql'); 
const config = {
  user: 'sa', 
  password: 'Password123!', 
  server: 'localhost', 
  database: 'sim_db', 
  options: {encrypt: true, trustServerCertificate: true}
}; 
sql.connect(config).then(async pool => { 
  try { 
    console.log("Testing CPU query...");
    console.log(await pool.request().query(`
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
    `));
    console.log("Testing IO query...");
    console.log(await pool.request().query(`
        SELECT SUM(num_of_bytes_read + num_of_bytes_written) / 1024 / 1024 as TotalIOMB
        FROM sys.dm_io_virtual_file_stats(DB_ID('sim_db'), NULL);
    `));
    process.exit(0); 
  } catch(e) { 
    console.error("ERROR:");
    console.error(e); 
    process.exit(1); 
  }
});
