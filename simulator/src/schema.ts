/**
 * Single source of truth for the database schema. Every batch is idempotent and is run by the
 * simulator's main thread on startup (scripts/init.sql no longer exists).
 */

export const createDatabaseSql = (db: string) => `
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '${db}')
BEGIN
    CREATE DATABASE [${db}];
END`;

export const schemaBatches: { name: string; sql: string }[] = [
    {
        name: 'enable CDC on database',
        sql: `
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()) = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END`,
    },
    {
        name: 'create tables',
        sql: `
IF OBJECT_ID(N'dbo.invoice_batch', N'U') IS NULL
    CREATE TABLE dbo.invoice_batch (
        id INT PRIMARY KEY,
        invoice_number VARCHAR(50) NOT NULL,
        status VARCHAR(20) DEFAULT 'PENDING',
        plan_id INT NULL
    );

IF OBJECT_ID(N'dbo.billing_record', N'U') IS NULL
    CREATE TABLE dbo.billing_record (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        change_token UNIQUEIDENTIFIER NULL
    );

IF OBJECT_ID(N'dbo.cdc_events_shadow', N'U') IS NULL
    CREATE TABLE dbo.cdc_events_shadow (
        id INT IDENTITY(1,1) PRIMARY KEY,
        billing_id INT,
        batch_id INT,
        amount DECIMAL(10,2),
        invoice_number VARCHAR(50),
        operation_type VARCHAR(10),
        captured_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );

IF OBJECT_ID(N'dbo.outbox_events', N'U') IS NULL
    CREATE TABLE dbo.outbox_events (
        id INT IDENTITY(1,1) PRIMARY KEY,
        aggregate_id INT,
        aggregate_type VARCHAR(50) DEFAULT 'BillingRecord',
        payload NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );

IF OBJECT_ID(N'dbo.enrichment_requests', N'U') IS NULL
    CREATE TABLE dbo.enrichment_requests (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        payload NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        billing_record_id INT NULL,
        op CHAR(1) NULL,
        change_token UNIQUEIDENTIFIER NULL
    );

IF OBJECT_ID(N'dbo.subscriber_plan', N'U') IS NULL
    CREATE TABLE dbo.subscriber_plan (
        id INT PRIMARY KEY,
        plan_name VARCHAR(100) NOT NULL,
        plan_type VARCHAR(50) NOT NULL,
        base_rate DECIMAL(10,2) NOT NULL,
        discount_pct DECIMAL(5,2) DEFAULT 0,
        effective_from DATETIME2 DEFAULT SYSUTCDATETIME(),
        effective_to DATETIME2 NULL
    );

IF OBJECT_ID(N'dbo.subscriber_usage', N'U') IS NULL
    CREATE TABLE dbo.subscriber_usage (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        usage_type VARCHAR(50) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        unit VARCHAR(20) NOT NULL,
        recorded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );

IF OBJECT_ID(N'dbo.rate_schedule', N'U') IS NULL
    CREATE TABLE dbo.rate_schedule (
        id INT PRIMARY KEY,
        plan_id INT NOT NULL,
        usage_type VARCHAR(50) NOT NULL,
        rate_per_unit DECIMAL(10,4) NOT NULL,
        min_charge DECIMAL(10,2) DEFAULT 0,
        max_charge DECIMAL(10,2) NULL,
        valid_from DATETIME2 DEFAULT SYSUTCDATETIME(),
        valid_to DATETIME2 NULL
    );`,
    },
    {
        name: 'migrate columns from older versions',
        sql: `
IF COL_LENGTH('dbo.invoice_batch', 'plan_id') IS NULL
    ALTER TABLE dbo.invoice_batch ADD plan_id INT NULL;
IF COL_LENGTH('dbo.billing_record', 'change_token') IS NULL
    ALTER TABLE dbo.billing_record ADD change_token UNIQUEIDENTIFIER NULL;
IF COL_LENGTH('dbo.enrichment_requests', 'billing_record_id') IS NULL
    ALTER TABLE dbo.enrichment_requests ADD billing_record_id INT NULL;
IF COL_LENGTH('dbo.enrichment_requests', 'op') IS NULL
    ALTER TABLE dbo.enrichment_requests ADD op CHAR(1) NULL;
IF COL_LENGTH('dbo.enrichment_requests', 'change_token') IS NULL
    ALTER TABLE dbo.enrichment_requests ADD change_token UNIQUEIDENTIFIER NULL;`,
    },
    {
        name: 'create indexes',
        sql: `
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_billing_record_batch_id' AND object_id = OBJECT_ID('dbo.billing_record'))
    CREATE INDEX IX_billing_record_batch_id ON dbo.billing_record (batch_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_subscriber_usage_batch_id' AND object_id = OBJECT_ID('dbo.subscriber_usage'))
    CREATE INDEX IX_subscriber_usage_batch_id ON dbo.subscriber_usage (batch_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_rate_schedule_plan_usage' AND object_id = OBJECT_ID('dbo.rate_schedule'))
    CREATE INDEX IX_rate_schedule_plan_usage ON dbo.rate_schedule (plan_id, usage_type);`,
    },
    {
        name: 'seed subscriber_plan and rate_schedule',
        sql: `
IF NOT EXISTS (SELECT 1 FROM dbo.subscriber_plan)
BEGIN
    WITH n AS (SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects)
    INSERT INTO dbo.subscriber_plan (id, plan_name, plan_type, base_rate, discount_pct)
    SELECT i,
        CONCAT(CHOOSE(i % 5 + 1, 'Enterprise', 'Starter', 'Professional', 'Premium', 'Ultimate'), ' Plan ', i),
        CHOOSE(i % 3 + 1, 'PREPAID', 'POSTPAID', 'HYBRID'),
        CAST(10.00 + (i * 5.50) AS DECIMAL(10,2)),
        CAST((i % 20) * 0.5 AS DECIMAL(5,2))
    FROM n;
END

IF NOT EXISTS (SELECT 1 FROM dbo.rate_schedule)
BEGIN
    WITH n AS (SELECT TOP (400) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS j FROM sys.all_objects)
    INSERT INTO dbo.rate_schedule (id, plan_id, usage_type, rate_per_unit, min_charge, max_charge)
    SELECT j,
        ((j - 1) / 4) + 1,
        CHOOSE((j - 1) % 4 + 1, 'DATA', 'VOICE', 'SMS', 'ROAMING'),
        CAST(0.01 + (j * 0.005) AS DECIMAL(10,4)),
        CAST(1.00 + (((j - 1) % 4) * 0.50) AS DECIMAL(10,2)),
        CASE WHEN j % 3 = 0 THEN CAST(100.00 + ((((j - 1) / 4) + 1) * 2) AS DECIMAL(10,2)) ELSE NULL END
    FROM n;
END`,
    },
    {
        name: 'create Approach 1 trigger',
        sql: `
IF NOT EXISTS (SELECT 1 FROM sys.triggers WHERE name = 'trg_billing_record_cdc')
EXEC('
CREATE TRIGGER trg_billing_record_cdc ON dbo.billing_record
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM inserted) AND NOT EXISTS (SELECT 1 FROM deleted)
        INSERT INTO dbo.cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
        SELECT i.id, i.batch_id, i.amount, b.invoice_number, ''INSERT''
        FROM inserted i LEFT JOIN dbo.invoice_batch b ON i.batch_id = b.id;

    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
        INSERT INTO dbo.cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
        SELECT i.id, i.batch_id, i.amount, b.invoice_number, ''UPDATE''
        FROM inserted i LEFT JOIN dbo.invoice_batch b ON i.batch_id = b.id;

    IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
        INSERT INTO dbo.cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
        SELECT d.id, d.batch_id, d.amount, b.invoice_number, ''DELETE''
        FROM deleted d LEFT JOIN dbo.invoice_batch b ON d.batch_id = b.id;
END');`,
    },
];

/** Rows from older versions have no change_token; Approach 3 joins on it. Run while CDC is off. */
export const backfillChangeTokensSql = `
WHILE 1 = 1
BEGIN
    UPDATE TOP (50000) dbo.billing_record SET change_token = NEWID() WHERE change_token IS NULL;
    IF @@ROWCOUNT = 0 BREAK;
END`;

/**
 * Makes sure every batch id the workload can pick (1..@n) has an invoice_batch row and two
 * subscriber_usage rows, so lookups and joins never silently return nothing.
 */
export const ensureReferenceDataSql = `
SET NOCOUNT ON;
DECLARE @have INT = (SELECT COUNT(*) FROM dbo.invoice_batch WHERE id BETWEEN 1 AND @n);
IF @have < @n
BEGIN
    WITH n AS (
        SELECT TOP (@n) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
        FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
    )
    INSERT INTO dbo.invoice_batch (id, invoice_number, plan_id)
    SELECT i, CONCAT('INV-', 1000 + i), ((i - 1) % 100) + 1
    FROM n WHERE NOT EXISTS (SELECT 1 FROM dbo.invoice_batch ib WHERE ib.id = n.i);
END
UPDATE dbo.invoice_batch SET plan_id = ((id - 1) % 100) + 1 WHERE plan_id IS NULL;

DECLARE @usageHave INT = (SELECT COUNT(DISTINCT batch_id) FROM dbo.subscriber_usage WHERE batch_id BETWEEN 1 AND @n);
IF @usageHave < @n
BEGIN
    WITH n AS (
        SELECT TOP (@n) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
        FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c
    )
    INSERT INTO dbo.subscriber_usage (batch_id, usage_type, quantity, unit)
    SELECT n.i,
        CHOOSE((n.i + v.k) % 4 + 1, 'DATA', 'VOICE', 'SMS', 'ROAMING'),
        CASE v.k WHEN 0 THEN CAST(10.0 + (n.i * 1.5) AS DECIMAL(15,4)) ELSE CAST(5.0 + (n.i * 0.8) AS DECIMAL(15,4)) END,
        CHOOSE((n.i + v.k) % 4 + 1, 'MB', 'MIN', 'COUNT', 'MB')
    FROM n CROSS JOIN (VALUES (0), (1)) AS v(k)
    WHERE NOT EXISTS (SELECT 1 FROM dbo.subscriber_usage su WHERE su.batch_id = n.i);
END
SELECT @n AS cardinality;`;
