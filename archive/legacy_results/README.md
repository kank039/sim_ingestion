# Legacy results (before the metrics fixes)

These CSVs were produced by the old simulator and are kept only for reference. Their numbers are not
comparable with new runs:

- the actual request rate did not match the requested one (low targets ran at ~80 RPS);
- every approach also paid for the Approach 1 trigger and CDC on all tables;
- p95/p99 were averages of per-thread percentiles;
- `Lag` / `RecordsInKafka` always counted the `billing_record` topic, so Approach 2 showed an ever-growing lag.

New results are written to `output/` (ignored by git) by the PowerShell runners.
