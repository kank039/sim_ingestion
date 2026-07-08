import { connectDB, getPool } from './db';

async function test() {
    await connectDB();
    const pool = getPool();
    try {
        console.log("Adding filegroup...");
        await pool.request().query(`
            IF NOT EXISTS (SELECT * FROM sys.filegroups WHERE type = 'FX')
            BEGIN
                ALTER DATABASE sim_db ADD FILEGROUP sim_db_mod CONTAINS MEMORY_OPTIMIZED_DATA;
                ALTER DATABASE sim_db ADD FILE (NAME='sim_db_mod', FILENAME='/var/opt/mssql/data/sim_db_mod') TO FILEGROUP sim_db_mod;
            END
        `);
        console.log("Creating MO table...");
        await pool.request().query(`
            CREATE TABLE test_mo (
                id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,
                data VARCHAR(50)
            ) WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
        `);
        console.log("Enabling CDC...");
        await pool.request().query(`
            EXEC sys.sp_cdc_enable_table 
                @source_schema = 'dbo', 
                @source_name = 'test_mo', 
                @role_name = NULL,
                @capture_instance = 'dbo_test_mo_CT';
        `);
        console.log("SUCCESS!");
    } catch (e: any) {
        console.log("ERROR: " + e.message);
    }
    process.exit(0);
}
test();
