import sql from 'mssql';
import { randomUUID } from 'crypto';
import type { ApproachProfile } from './approaches';

export type RequestedOp = 'c' | 'u' | 'd';

export interface WriteResult {
    /** The operation that actually happened ('c' when an update/delete found no row and inserted instead). */
    op: RequestedOp;
    billingId: number;
}

/**
 * Core billing_record mutation shared by every approach, executed as one round trip.
 * An update/delete that finds no row for the batch falls back to an insert so every
 * operation changes exactly one row (and therefore produces exactly one change event).
 */
const CORE_WRITE = `
DECLARE @op CHAR(1) = @requestedOp, @id INT = NULL, @tok UNIQUEIDENTIFIER = NULL;
IF @op = 'u'
BEGIN
    UPDATE TOP (1) dbo.billing_record SET amount = @amount, change_token = @token, @id = id
    WHERE batch_id = @batchId;
    IF @id IS NOT NULL SET @tok = @token;
END
ELSE IF @op = 'd'
BEGIN
    SELECT TOP (1) @id = id, @tok = change_token FROM dbo.billing_record WITH (UPDLOCK, ROWLOCK) WHERE batch_id = @batchId;
    IF @id IS NOT NULL
    BEGIN
        DELETE FROM dbo.billing_record WHERE id = @id;
        -- A concurrent operation may have deleted it first; then this one inserts instead.
        IF @@ROWCOUNT = 0 SELECT @id = NULL, @tok = NULL;
    END
END
IF @id IS NULL
BEGIN
    SET @op = 'c';
    INSERT INTO dbo.billing_record (batch_id, amount, change_token) VALUES (@batchId, @amount, @token);
    SELECT @id = CAST(SCOPE_IDENTITY() AS INT), @tok = @token;
END`;

// No table variables or temp tables anywhere in the write path: at high concurrency their
// creation contends on tempdb metadata pages and would dominate every approach's latency.
const OUTBOX_INSERT = `
INSERT INTO dbo.outbox_events (aggregate_id, aggregate_type, payload)
VALUES (@id, 'BillingRecord',
    (SELECT @op AS op, @id AS billingId, @batchId AS batchId, CAST(@amount AS VARCHAR(20)) AS amount, @invoiceNumber AS invoiceNumber
     FOR JSON PATH, WITHOUT_ARRAY_WRAPPER));`;

const SELECT_RESULT = `SELECT @op AS op, @id AS id, CAST(@tok AS VARCHAR(36)) AS change_token;`;

export function buildWriteBatch(profile: ApproachProfile): string {
    if (profile.outbox) {
        // Transactional outbox: domain row + event row commit or roll back together.
        return `SET NOCOUNT ON; SET XACT_ABORT ON;
BEGIN TRAN;
${CORE_WRITE}
${OUTBOX_INSERT}
COMMIT TRAN;
${SELECT_RESULT}`;
    }
    return `SET NOCOUNT ON;
${CORE_WRITE}
${SELECT_RESULT}`;
}

const DUAL_WRITE = `
INSERT INTO dbo.enrichment_requests (billing_record_id, batch_id, op, change_token, payload)
VALUES (@billingId, @batchId, @op, @token, @payload);`;

export interface WriteParams {
    batchId: number;
    amount: string;
    requestedOp: RequestedOp;
}

export async function executeWrite(pool: sql.ConnectionPool, profile: ApproachProfile, batchSql: string, p: WriteParams): Promise<WriteResult> {
    const req = pool.request();
    req.input('requestedOp', sql.Char(1), p.requestedOp);
    req.input('batchId', sql.Int, p.batchId);
    req.input('amount', sql.Decimal(10, 2), p.amount);
    req.input('token', sql.UniqueIdentifier, randomUUID());
    if (profile.outbox) req.input('invoiceNumber', sql.VarChar(50), `INV-${1000 + p.batchId}`);
    const res = await req.query(batchSql);
    const row = res.recordset?.[0];
    if (!row) throw new Error('Write batch returned no result row');
    const result: WriteResult = { op: row.op, billingId: row.id };

    if (profile.dualWrite) {
        // Approach 3 dual write: a second, independent transaction. If it fails the billing
        // change is already committed - that inconsistency is the pattern's known weakness.
        const req2 = pool.request();
        req2.input('billingId', sql.Int, row.id);
        req2.input('batchId', sql.Int, p.batchId);
        req2.input('op', sql.Char(1), row.op);
        req2.input('token', sql.UniqueIdentifier, row.change_token);
        req2.input('payload', sql.NVarChar(sql.MAX), JSON.stringify({ invoiceNumber: `INV-${1000 + p.batchId}`, amount: p.amount }));
        try {
            await req2.query(DUAL_WRITE);
        } catch (e) {
            const err = new Error(`Dual write failed after billing commit: ${(e as Error).message}`);
            (err as any).partialWrite = true;
            throw err;
        }
    }
    return result;
}

export function pickOp(insertsOnly: boolean, weights: { insert: number; update: number; delete: number }, rand = Math.random()): RequestedOp {
    if (insertsOnly) return 'c';
    const total = weights.insert + weights.update + weights.delete;
    if (!(total > 0)) return 'c';
    const r = rand * total;
    if (r < weights.delete) return 'd';
    if (r < weights.delete + weights.update) return 'u';
    return 'c';
}
