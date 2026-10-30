# V4: CDC progress during low or no change activity

This focused experiment revisits whether CDC advances its database-wide
high endpoint during a quiet period, including when the latest secondary
`last_commit_lsn` comes from a transaction against a table with no CDC capture
instance. It records CDC progress and primary capture-scan diagnostics alongside
the readable secondary's CDC and HADR positions.

## Run

From this directory, install the existing Python dependency if needed and run
against the test AG provisioned by `scripts/provision.sh`:

```bash
python3 -m pip install -r ../requirements.txt
MSSQL_SA_PASSWORD='YourStrong!Passw0rd' \
  python3 probe_idle_cdc_progress.py --duration 180 --interval 1
```

The password is read from `MSSQL_SA_PASSWORD`; alternatively, pass `--password`.
Use the actual SQL login for the test environment.

The probe requires database CDC to be enabled and both SQL endpoints to resolve
to the same AG database, with one writable primary and one readable secondary.
Stop other test workloads first; the probe records subsequent DMV movement so
unexpected database activity is visible in the report.
It creates `dbo.watermark_probe_v4_non_cdc` on the primary if absent, verifies
the table has no CDC capture instance, inserts one unique marker row, and waits
for that row and its corresponding secondary DMV progress to appear. It then
performs no intentional database writes during the observation period.

The `--out` path defaults to `../../runs/v4`, which resolves to the repository's
ignored `runs/` directory when invoked here. Each run writes `meta.json`,
`samples.jsonl`, and `report.json`. Samples include:

- `sys.fn_cdc_get_max_lsn()` and the latest `cdc.lsn_time_mapping` row, read on
  the primary.
- The latest `sys.dm_cdc_log_scan_sessions` row and CDC capture-job activity,
  read on the primary.
- `sys.fn_cdc_get_max_lsn()`, `last_commit_lsn`, and `last_redone_lsn`, read on
  the readable secondary.

Exit code `0` means CDC reached the first observed secondary `last_commit_lsn`
after the marker became visible during the observation period. Exit code `2`
means it did not. Neither outcome alone proves a universal SQL Server contract;
interpret it with the scan-session and job diagnostics in the report. The marker
row is intentionally retained for inspection.

The report also indicates whether the secondary DMV advanced beyond the target
during the hold period. If it did, other database commits occurred after the
marker boundary, so treat the run as an active/low-activity observation rather
than a perfectly idle hold. Re-run after stopping those writers to isolate the
no-change case.

## Testing capture-agent commits as boundary markers

`probe_capture_commit_boundary.py` tests whether a capture-agent transaction
commit after a non-CDC marker can act as evidence that earlier CDC changes have
been processed. It inserts one control row into CDC-enabled `dbo.orders`, then
one marker into the non-CDC probe table. After the marker is visible on the
secondary, it records a bounded observation period and inspects the primary
transaction log for commits belonging to the CDC scan-agent session, including
whether each transaction wrote to `cdc.dbo_orders_CT`.

Run against the test AG:

```bash
MSSQL_SA_PASSWORD='YourStrong!Passw0rd' \
  python3 probe_capture_commit_boundary.py --duration 45 --interval 1
```

The log inspection uses undocumented `sys.fn_dblog` and requires sufficient
permissions; it is an exploratory test, not a supported production mechanism.
The output distinguishes agent commits after the DMV boundary from agent
transactions that wrote CDC rows. Even an observed post-boundary commit is only
empirical evidence: it does not establish a supported guarantee that the commit
certifies all earlier LSNs were scanned. Each run leaves its control row and
marker in the test database and writes metadata, samples, and a report under
`../../runs/v4`.
