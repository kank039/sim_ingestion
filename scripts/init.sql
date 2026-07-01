IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'sim_db')
BEGIN
    CREATE DATABASE sim_db;
END
GO

USE sim_db;
GO

-- Enable CDC on the database
IF sys.fn_cdc_has_db_access() = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- 1. Helper Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[invoice_batch]') AND type in (N'U'))
BEGIN
    CREATE TABLE invoice_batch (
        id INT PRIMARY KEY,
        invoice_number VARCHAR(50) NOT NULL,
        status VARCHAR(20) DEFAULT 'PENDING'
    );
END
GO

-- 2. Main Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[billing_record]') AND type in (N'U'))
BEGIN
    CREATE TABLE billing_record (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 3. Shadow Table for Approach 1
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[cdc_events_shadow]') AND type in (N'U'))
BEGIN
    CREATE TABLE cdc_events_shadow (
        id INT IDENTITY(1,1) PRIMARY KEY,
        billing_id INT,
        batch_id INT,
        amount DECIMAL(10,2),
        invoice_number VARCHAR(50),
        operation_type VARCHAR(10),
        captured_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 4. Outbox Table for Approach 2
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[outbox_events]') AND type in (N'U'))
BEGIN
    CREATE TABLE outbox_events (
        id INT IDENTITY(1,1) PRIMARY KEY,
        aggregate_id INT,
        aggregate_type VARCHAR(50) DEFAULT 'BillingRecord',
        payload NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 5. Enrichment Requests Table for Approach 3 (Flink)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[enrichment_requests]') AND type in (N'U'))
BEGIN
    CREATE TABLE enrichment_requests (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        payload NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 6. Subscriber Plan Table for Approach 5 (Multi-Table JOIN Enrichment)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[subscriber_plan]') AND type in (N'U'))
BEGIN
    CREATE TABLE subscriber_plan (
        id INT PRIMARY KEY,
        plan_name VARCHAR(100) NOT NULL,
        plan_type VARCHAR(50) NOT NULL,
        base_rate DECIMAL(10,2) NOT NULL,
        discount_pct DECIMAL(5,2) DEFAULT 0,
        effective_from DATETIME2 DEFAULT SYSUTCDATETIME(),
        effective_to DATETIME2 NULL
    );
END
GO

-- 7. Subscriber Usage Table for Approach 5
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[subscriber_usage]') AND type in (N'U'))
BEGIN
    CREATE TABLE subscriber_usage (
        id INT IDENTITY(1,1) PRIMARY KEY,
        batch_id INT NOT NULL,
        usage_type VARCHAR(50) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        unit VARCHAR(20) NOT NULL,
        recorded_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 8. Rate Schedule Table for Approach 5
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[rate_schedule]') AND type in (N'U'))
BEGIN
    CREATE TABLE rate_schedule (
        id INT PRIMARY KEY,
        plan_id INT NOT NULL,
        usage_type VARCHAR(50) NOT NULL,
        rate_per_unit DECIMAL(10,4) NOT NULL,
        min_charge DECIMAL(10,2) DEFAULT 0,
        max_charge DECIMAL(10,2) NULL,
        valid_from DATETIME2 DEFAULT SYSUTCDATETIME(),
        valid_to DATETIME2 NULL
    );
END
GO

-- Add plan_id column to invoice_batch if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[invoice_batch]') AND name = 'plan_id')
BEGIN
    ALTER TABLE invoice_batch ADD plan_id INT NULL;
END
GO

-- Seed subscriber_plan data (100 plans)
IF NOT EXISTS (SELECT 1 FROM subscriber_plan WHERE id = 1)
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 100
    BEGIN
        INSERT INTO subscriber_plan (id, plan_name, plan_type, base_rate, discount_pct)
        VALUES (
            @i,
            CONCAT(
                CASE @i % 5
                    WHEN 0 THEN 'Enterprise'
                    WHEN 1 THEN 'Starter'
                    WHEN 2 THEN 'Professional'
                    WHEN 3 THEN 'Premium'
                    WHEN 4 THEN 'Ultimate'
                END,
                ' Plan ', @i
            ),
            CASE @i % 3
                WHEN 0 THEN 'PREPAID'
                WHEN 1 THEN 'POSTPAID'
                WHEN 2 THEN 'HYBRID'
            END,
            CAST(10.00 + (@i * 5.50) AS DECIMAL(10,2)),
            CAST((@i % 20) * 0.5 AS DECIMAL(5,2))
        );
        SET @i = @i + 1;
    END
END
GO

-- Seed rate_schedule data (4 usage types per plan = 400 rows)
IF NOT EXISTS (SELECT 1 FROM rate_schedule WHERE id = 1)
BEGIN
    DECLARE @j INT = 1;
    DECLARE @planId INT;
    DECLARE @usageIdx INT;
    WHILE @j <= 400
    BEGIN
        SET @planId = ((@j - 1) / 4) + 1;
        SET @usageIdx = (@j - 1) % 4;
        INSERT INTO rate_schedule (id, plan_id, usage_type, rate_per_unit, min_charge, max_charge)
        VALUES (
            @j,
            @planId,
            CASE @usageIdx
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'VOICE'
                WHEN 2 THEN 'SMS'
                WHEN 3 THEN 'ROAMING'
            END,
            CAST(0.01 + (@j * 0.005) AS DECIMAL(10,4)),
            CAST(1.00 + (@usageIdx * 0.50) AS DECIMAL(10,2)),
            CASE WHEN @j % 3 = 0 THEN CAST(100.00 + (@planId * 2) AS DECIMAL(10,2)) ELSE NULL END
        );
        SET @j = @j + 1;
    END
END
GO

-- Seed subscriber_usage data (2 usage records per batch_id for 100 batches = 200 rows)
IF NOT EXISTS (SELECT 1 FROM subscriber_usage WHERE batch_id = 1)
BEGIN
    DECLARE @k INT = 1;
    WHILE @k <= 100
    BEGIN
        INSERT INTO subscriber_usage (batch_id, usage_type, quantity, unit)
        VALUES (@k,
            CASE @k % 4
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'VOICE'
                WHEN 2 THEN 'SMS'
                WHEN 3 THEN 'ROAMING'
            END,
            CAST(10.0 + (@k * 1.5) AS DECIMAL(15,4)),
            CASE @k % 4
                WHEN 0 THEN 'MB'
                WHEN 1 THEN 'MIN'
                WHEN 2 THEN 'COUNT'
                WHEN 3 THEN 'MB'
            END
        );
        INSERT INTO subscriber_usage (batch_id, usage_type, quantity, unit)
        VALUES (@k,
            CASE (@k + 1) % 4
                WHEN 0 THEN 'DATA'
                WHEN 1 THEN 'VOICE'
                WHEN 2 THEN 'SMS'
                WHEN 3 THEN 'ROAMING'
            END,
            CAST(5.0 + (@k * 0.8) AS DECIMAL(15,4)),
            CASE (@k + 1) % 4
                WHEN 0 THEN 'MB'
                WHEN 1 THEN 'MIN'
                WHEN 2 THEN 'COUNT'
                WHEN 3 THEN 'MB'
            END
        );
        SET @k = @k + 1;
    END
END
GO

-- Update invoice_batch plan_id if not already set
UPDATE invoice_batch SET plan_id = ((id - 1) % 100) + 1 WHERE plan_id IS NULL;
GO

-- Create Trigger for Approach 1
IF NOT EXISTS (SELECT * FROM sys.triggers WHERE name = 'trg_billing_record_cdc')
BEGIN
    EXEC('
    CREATE TRIGGER trg_billing_record_cdc 
    ON billing_record
    AFTER INSERT, UPDATE, DELETE
    AS
    BEGIN
        SET NOCOUNT ON;
        
        -- Handle Inserts
        IF EXISTS(SELECT * FROM inserted) AND NOT EXISTS(SELECT * FROM deleted)
        BEGIN
            INSERT INTO cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
            SELECT i.id, i.batch_id, i.amount, b.invoice_number, ''INSERT''
            FROM inserted i
            LEFT JOIN invoice_batch b ON i.batch_id = b.id;
        END

        -- Handle Updates
        IF EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted)
        BEGIN
            INSERT INTO cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
            SELECT i.id, i.batch_id, i.amount, b.invoice_number, ''UPDATE''
            FROM inserted i
            LEFT JOIN invoice_batch b ON i.batch_id = b.id;
        END

        -- Handle Deletes
        IF EXISTS(SELECT * FROM deleted) AND NOT EXISTS(SELECT * FROM inserted)
        BEGIN
            INSERT INTO cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
            SELECT d.id, d.batch_id, d.amount, b.invoice_number, ''DELETE''
            FROM deleted d
            LEFT JOIN invoice_batch b ON d.batch_id = b.id;
        END
    END;
    ');
END
GO

-- Enable CDC on Tables
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'billing_record' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'billing_record',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'invoice_batch' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'invoice_batch',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'cdc_events_shadow' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'cdc_events_shadow',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'outbox_events' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'outbox_events',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'enrichment_requests' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'enrichment_requests',
        @role_name     = NULL;
END

-- Enable CDC on Approach 5 tables
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'subscriber_plan' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'subscriber_plan',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'subscriber_usage' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'subscriber_usage',
        @role_name     = NULL;
END

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'rate_schedule' AND is_tracked_by_cdc = 1)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'rate_schedule',
        @role_name     = NULL;
END
GO
