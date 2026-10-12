/**
 * Shared outcome math so every view (cards, chart, flow diagram, summary, CSV) agrees.
 *
 *   modified = committed within the SLA timeout
 *   late     = committed, but slower than the SLA timeout
 *   failed   = errored (nothing committed, or a partial dual write)
 *   dropped  = shed by the load generator before reaching the DB (queue full / expired in queue)
 */
export interface OutcomeCounts {
  recordsModified?: number;
  recordsLate?: number;
  recordsFailed?: number;
  recordsDropped?: number;
}

export function outcomeRates(c: OutcomeCounts) {
  const modified = c.recordsModified || 0;
  const late = c.recordsLate || 0;
  const failed = c.recordsFailed || 0;
  const dropped = c.recordsDropped || 0;
  const total = modified + late + failed + dropped;
  const pct = (n: number) => (total > 0 ? Math.round((n / total) * 10000) / 100 : 100);
  return {
    total,
    committed: modified + late,
    /** Share of all attempted operations that committed within the SLA. */
    slaRate: pct(modified),
    /** Share of all attempted operations that committed at all. */
    successRate: pct(modified + late),
  };
}
