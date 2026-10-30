# SQL Server AG + CDC ordering test

Tests what ordering guarantee exists between a snapshot-isolated read on an
Always On readable secondary and the CDC LSN reported by
`sys.fn_cdc_get_max_lsn()` on that same secondary -- the property a Debezium-style
incremental snapshot algorithm would need to hold if ported from MySQL to SQL Server.

Background: CDC's capture job only ever runs on the **primary** replica. It writes
into `cdc.<capture_instance>_CT` and `cdc.lsn_time_mapping` as ordinary transactions,
which then replicate to the secondary and get redone like any other DML. So on the
secondary, `fn_cdc_get_max_lsn()` is reading a system table subject to the same redo
stream and the same snapshot-isolation machinery as your base table -- the interesting
question is whether redo ordering + snapshot-isolation visibility keep those two
things correlated the way the algorithm needs.

## Requirements

- Docker + Docker Compose
- `msodbcsql18` (ODBC Driver 18 for SQL Server) installed on the host, for the Python harness
- Python 3.9+

## Layout

```
docker-compose.yml        Two SQL Server on Linux containers (node1 = primary, node2 = secondary)
scripts/provision.sh      One-shot setup: certs, endpoints, AG, join, CDC enable
sql/ground_truth_lsn.sql  Query to see the real LSN CDC assigned to each row
sql/health_checks.sql     AG sync state / CDC capture lag DMVs, useful while a test runs
python/harness.py         Writer (primary) + observer (secondary) threads
python/analyze.py         Joins observer output against ground-truth LSNs, flags violations
```

## 1. Provision the AG

```bash
chmod +x scripts/provision.sh
./scripts/provision.sh
```

This brings up both containers, sets up certificate-based AG endpoints (no AD needed),
creates a `CLUSTER_TYPE = NONE` availability group (no WSFC/Pacemaker required), joins
`mssql-node2` as a readable secondary with automatic seeding, and enables CDC on
`dbo.orders` on the primary. Takes a few minutes, mostly waiting on seeding.

If it fails partway through, `docker compose down -v` wipes both containers'
volumes so you can start clean rather than trying to resume mid-script.

Sanity-check it worked:

```bash
docker exec -it mssql-node1 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa \
  -P 'YourStrong!Passw0rd' -Q "SELECT * FROM sys.dm_hadr_database_replica_states"

docker exec -it mssql-node2 /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa \
  -P 'YourStrong!Passw0rd' -Q "SELECT sys.fn_cdc_get_max_lsn()"
```

## 2. Run the workload + observer

```bash
cd python
pip install -r requirements.txt
python harness.py --rows 5000
```

This inserts 5000 rows on the primary as fast as possible while an observer loop on
the secondary repeatedly opens a `SNAPSHOT`-isolation transaction, reads
`sys.fn_cdc_get_max_lsn()` and `MAX(id)` within that same transaction, and logs both.
Two files come out: `writer_log.jsonl` and `observer_log.jsonl`.

Useful variations once the basic run works:

- `--delay 0.01` to slow the writer down and see if the violation rate (if any) changes
- Edit `docker-compose.yml`/`provision.sh` to use `ASYNCHRONOUS_COMMIT` instead of
  `SYNCHRONOUS_COMMIT` and re-provision, to see whether commit mode affects the result
- Run `sql/health_checks.sql` against both nodes in another terminal while the harness
  runs, to correlate any violations with redo-queue depth or capture-job lag

## 3. Analyze

```bash
python analyze.py
```

This pulls ground-truth LSNs straight from `cdc.dbo_orders_CT` on the primary and
checks, for every observation, whether the `fn_cdc_get_max_lsn()` value read inside
that snapshot transaction was always at least as new as the true LSN of the newest
row that transaction's snapshot could see. Violations (if any) are printed with
timestamps so you can correlate them back into `writer_log.jsonl` /
`sql/health_checks.sql` output from the same window.

## Notes / gotchas

- A long-running snapshot transaction on the secondary blocks the redo thread from
  advancing past it (to preserve a consistent view) -- keep the observer's transactions
  short, which the provided loop already does, or you'll mostly be testing "does redo
  catch up after I commit" rather than the actual race.
- The capture job's own scan lag (`sys.dm_cdc_log_scan_sessions` on the primary) is a
  separate, additive source of delay on top of AG replication lag -- worth logging
  distinctly if you see gaps rather than assuming it's all secondary-redo lag.
- `analyze.py` treats "row visible on secondary but not yet captured by CDC on the
  primary at all" as expected latency, not a violation, since that's a timing artifact
  of when you happened to run the analysis, not the ordering property under test.
  Re-run `analyze.py` a little later if that count looks high.

## Focused DMV watermark validation (v3)

The v3 harness also checks DMV lower/upper bounds around its snapshot reads and
records the SQL Server replica identity before the transaction, during the chunk
read, and after commit. From `python/v3`, run
`python3 analyze.py <run-directory>` after a v3 workload; the analyzer reports
and fails on any observation whose watermark and chunk read came from different
replica identities.

To test the open question about commits to a table that is not CDC-enabled, run this bounded probe against a test AG database:

```bash
cd python/v3
python3 probe_non_cdc_catchup.py --timeout 60
```

The probe records SQL Server version, edition, AG mode, replica identities, the secondary DMV position, and whether `fn_cdc_get_max_lsn()` reaches the first observed `last_commit_lsn` after a marker commit to `dbo.watermark_probe_non_cdc`. It creates that table on the primary if needed and leaves the marker row in place. Exit status `0` means CDC reached the target within the timeout; `2` means it did not or the target was not observed in time. A timeout is evidence to design a bounded/alternative close-window behavior, not proof that CDC will never catch up.

Run the probe and the v3 analyzer against each SQL Server version and AG topology being evaluated. For separate host names, use `--primary-host` and `--secondary-host`; the usual `--primary-port`, `--secondary-port`, `--database`, and `--encrypt` options are also available. The pure-Python tests cover numeric-to-binary LSN component boundaries and replica-identity comparisons:

```bash
python3 test_analysis_core.py
```

These tests validate the watermark prerequisites, not the connector's end-to-end chunk reconciliation flow.

## V4: low-activity CDC progress probe

For the specific question of whether CDC advances during a quiet period after a
non-CDC-table commit, see [`python/v4/README.md`](python/v4/README.md). The v4
probe samples CDC progress and capture diagnostics on the primary together with
the CDC and HADR positions on the readable secondary. It also documents a
focused exploratory test of whether capture-agent transaction commits can act
as scan-boundary evidence.
