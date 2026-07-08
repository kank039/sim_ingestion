import { connectDB, getPool } from './db';

async function disableExtraCdc() {
    await connectDB();
    const pool = getPool();
    console.log("Disabling CDC for non-Approach 2 tables...");
    const tablesToDisable = ['billing_record', 'invoice_batch', 'cdc_events_shadow', 'rate_schedule', 'subscriber_plan', 'subscriber_usage'];
    
    for (const table of tablesToDisable) {
        try {
            await pool.request().query(`EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = '${table}', @capture_instance = 'all';`);
            console.log(`CDC disabled for ${table}`);
        } catch (e: any) {
            console.log(`Could not disable CDC for ${table}: ${e.message}`);
        }
    }
    
    console.log("Done.");
    process.exit(0);
}

disableExtraCdc();
