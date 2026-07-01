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
    console.log(await pool.request().query("SELECT count(*) as WaitTasks FROM sys.dm_os_waiting_tasks WHERE session_id > 50;")); 
    console.log(await pool.request().query("SELECT count(*) as ActiveLocks FROM sys.dm_tran_locks WHERE request_session_id > 50 AND request_mode IN ('X', 'U', 'IX');")); 
    console.log(await pool.request().query("SELECT cntr_value as PLE FROM sys.dm_os_performance_counters WHERE counter_name = 'Page life expectancy' AND object_name LIKE '%Buffer Manager%';")); 
    console.log(await pool.request().query("SELECT SUM(unallocated_extent_page_count + version_store_reserved_page_count + user_object_reserved_page_count + internal_object_reserved_page_count + mixed_extent_page_count) * 8.0 / 1024.0 as TempDbMB FROM tempdb.sys.dm_db_file_space_usage;")); 
    console.log(await pool.request().query("SELECT count(*) as ActiveConnections FROM sys.dm_exec_sessions WHERE database_id = DB_ID('sim_db') AND session_id > 50;")); 
    process.exit(0); 
  } catch(e) { 
    console.error(e); 
    process.exit(1); 
  }
});
