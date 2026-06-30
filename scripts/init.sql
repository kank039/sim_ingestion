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
IF sys.fn_cdc_has_column_changed() IS NULL -- this is just a dummy way to check, better to check sys.tables
    -- Wait, checking CDC enabled:
GO

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
GO
