CREATE DATABASE sim_db;
GO

USE sim_db;
GO

-- Enable CDC on the database
EXEC sys.sp_cdc_enable_db;
GO

-- 1. Helper Table
CREATE TABLE invoice_batch (
    id INT PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING'
);

-- 2. Main Table
CREATE TABLE billing_record (
    id INT IDENTITY(1,1) PRIMARY KEY,
    batch_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- 3. Shadow Table for Approach 1
CREATE TABLE cdc_events_shadow (
    id INT IDENTITY(1,1) PRIMARY KEY,
    billing_id INT,
    batch_id INT,
    amount DECIMAL(10,2),
    invoice_number VARCHAR(50),
    operation_type VARCHAR(10),
    captured_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- 4. Outbox Table for Approach 2
CREATE TABLE outbox_events (
    id INT IDENTITY(1,1) PRIMARY KEY,
    aggregate_id INT,
    aggregate_type VARCHAR(50) DEFAULT 'BillingRecord',
    payload NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

-- Create Trigger for Approach 1
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
        SELECT i.id, i.batch_id, i.amount, b.invoice_number, 'INSERT'
        FROM inserted i
        LEFT JOIN invoice_batch b ON i.batch_id = b.id;
    END

    -- Handle Updates
    IF EXISTS(SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted)
    BEGIN
        INSERT INTO cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
        SELECT i.id, i.batch_id, i.amount, b.invoice_number, 'UPDATE'
        FROM inserted i
        LEFT JOIN invoice_batch b ON i.batch_id = b.id;
    END

    -- Handle Deletes
    IF EXISTS(SELECT * FROM deleted) AND NOT EXISTS(SELECT * FROM inserted)
    BEGIN
        INSERT INTO cdc_events_shadow (billing_id, batch_id, amount, invoice_number, operation_type)
        SELECT d.id, d.batch_id, d.amount, b.invoice_number, 'DELETE'
        FROM deleted d
        LEFT JOIN invoice_batch b ON d.batch_id = b.id;
    END
END;
GO

-- Enable CDC on Tables
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'billing_record',
    @role_name     = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'invoice_batch',
    @role_name     = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'cdc_events_shadow',
    @role_name     = NULL;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'outbox_events',
    @role_name     = NULL;
GO
