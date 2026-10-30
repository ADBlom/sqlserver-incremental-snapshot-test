# Read-only incremental snapshot watermarks for SQL Server (Debezium) - findings for discussion

Environment: SQL Server 2022 CU27, 2-node Always On AG (readable secondary), CDC on `dbo.wm`, Docker, one
host clock. Experiment code, raw method and full tables: `README.md` and `exp.py` in this directory.

## 1. Problem

Chunked incremental snapshots need a **low** and a **high** LSN watermark around each chunk read, so stream
events in `(low, high]` can be de-duplicated against the chunk. Without writing a signal table:

* `low` = `fn_cdc_get_max_lsn()` read before the chunk. Always safe (0 violations in all runs).
* `high` must be **safe** (>= every commit the chunk can see) and **reachable** (the CDC stream position will
  eventually reach it, otherwise the window never closes).

## 2. Why the obvious candidates fail

CDC's max LSN (`fn_cdc_get_max_lsn`) advances only to commit LSNs of **tracked** transactions (plus a heartbeat
every 300 s, see 4). Any log-position value also moves for untracked commits (other tables, capture's own writes).

| Candidate (as `high`) | Safe | Reachable | Verdict |
|---|---|---|---|
| `fn_cdc_get_max_lsn()` after the read | **no** (HIGH violations in ~40-85 % of chunks) | yes | capture lags the writes |
| `dm_db_log_stats.log_end_lsn` (primary) | yes | **no** when idle / untracked commits | and unsafe as *low* under SNAPSHOT isolation |
| `dm_hadr_database_replica_states` commit / end-of-log / redone LSN | yes | **no** when idle / untracked commits | primary and secondary |
| DMV hardened LSN (secondary) | no (ahead of redo) | - | |
| `dm_tran_database_transactions` | no (min begin) | no | primary only, nothing on the secondary |
| `dm_cdc_log_scan_sessions`: "wait for a completed scan, then read cdc max" | **no under load** | yes | non-empty scans end at 500 txns |
| `fn_dblog` (maintainers' suggestion) | yes (see 5) | yes | needs sysadmin/db_owner (error 9010) |

## 3. Candidates that need only `db_datareader` (no objects, no DBA step, no `fn_dblog`)

Measured as a reader login that is only `db_datareader` (writes verified denied), on **both** the secondary and
the primary, primary additionally under SNAPSHOT isolation, with writers: updates, delete+reinsert, multi-row
and straggler transactions (up to 8 s), sparse writes, idle phases, non-CDC-table writes.
Ground truth: `cdc.dbo_wm_CT` row-version timeline; a violation is a chunk row older than the state at `low` (LOW)
or newer than the state at `high` (HIGH).

### A. Time watermark (works everywhere, plain SELECTs on `cdc.*`)

```
low  = fn_cdc_get_max_lsn()                       -- before the chunk read
... read chunk ...
T    = SYSDATETIME() on the primary (or reading node)  -- after the chunk read
high = SELECT TOP 1 start_lsn FROM cdc.lsn_time_mapping
       WHERE tran_end_time > DATEADD(ms, @margin, @T) ORDER BY start_lsn    -- NULL: not yet captured, retry
window closes when fn_cdc_get_max_lsn() >= high
```

Why safe: every commit visible to the chunk committed before the read ended, so its `tran_end_time <= T` and a
later mapping entry has a larger LSN. `high` is a real captured commit, so it is reachable as soon as *any*
captured table commits after `T`.

| Margin added to T | HIGH violations, primary (1129 chunks) | secondary (2562 chunks) | LOW |
|---|---|---|---|
| -3000 ms | 1063 | 2392 | 0 |
| -1000 ms | 344 | 1193 | 0 |
| -300 ms | 35 | 703 | 0 |
| -100 ms | 11 | 20 | 0 |
| **0 / +50 / +300 ms** | **0** | **0** | 0 |

Closing time: 2-6 s with continuous tracked writes (= capture lag), 3-6 s with sparse writes (0.1-3 s apart),
up to **300 s** if nothing is tracked (heartbeat).
Clock: `tran_end_time` is the primary's local time. Reading `T` on the primary makes this skew-free; on a
secondary use a margin >= worst clock skew (the error tolerance measured is a few tens of ms at most).

### B. Empty-scan barrier (idle fast path; needs `VIEW DATABASE PERFORMANCE STATE` + a primary connection)

```
s0 = SELECT TOP 1 session_id, empty_scan_count, scan_phase FROM sys.dm_cdc_log_scan_sessions ORDER BY session_id DESC
if s0.empty_scan_count = 0: not applicable (capture is busy, use A)
need = 1 if s0.scan_phase = 'Done' else 2       -- a running scan may have started before the read
wait until (same session AND empty_scan_count >= s0.empty_scan_count + need) OR (newer session AND empty_scan_count >= need)
high = fn_cdc_get_max_lsn() on the PRIMARY       -- all tracked commits visible to the chunk are now captured
```

Result: 0 LOW / 0 HIGH over 35 idle/untracked-write windows per node (primary + secondary chunks), window
closes in ~5 s (capture polling interval) instead of <= 300 s. Not valid under load; combine with A.

## 4. The enabling observation: the CDC idle heartbeat

If no tracked commit occurs, the capture job inserts a row with `tran_id = 0x00` into `cdc.lsn_time_mapping`
every 300 s (measured at +46 s, +347 s, +648 s, also during non-CDC write load). Its LSN is the capture scan end,
so it is the only signal that moves the secondary's CDC position over untracked log. DMV/log positions can
never be reached because capture's own commits sit above that LSN. Undocumented behaviour, version
dependent; the 300 s value was not configurable in my tests.

## 5. Reference: `fn_dblog` method (needs a DBA-created procedure)

High = max commit LSN of a user transaction that modified the chunk table, scanned from the log after the CDC
position (`least_privilege_setup.sql`, `WITH EXECUTE AS OWNER`). 0 LOW/0 HIGH over ~4,000 chunks on both nodes,
closes ~0.03 s after capture catches up, no clock assumption, ~85 ms/chunk. Downsides: undocumented function,
not available on Azure SQL / RDS, the connector login cannot call it directly (sysadmin/db_owner only), requires a
DBA-installed object, table filter by allocation-unit name, system-transaction name heuristics.

## 6. Open questions for the maintainers

1. Is "close within 300 s on an idle table" acceptable for A alone, or is B (primary connection + one DMV grant) wanted?
2. Is relying on `cdc.lsn_time_mapping.tran_end_time` (primary clock) acceptable? It needs the margin discussion in 3A.
3. A relies on every chunk window eventually getting a captured commit; with the 300 s heartbeat this holds as
   long as the capture job runs. If it is stopped the window stays open (fails closed).

## 7. Not tested / limits

Cross-host clock skew and NTP/DST steps (containers share one clock), AG failover mid-window, multi-table
windows (A is table-agnostic; the stream position is global), other SQL Server versions, a non-default capture
polling interval for B, columnstore, very large capture backlogs. Test workload is small (1,500 hot rows).
Test-only environment changes: `ro` login and `dbz_tracked_commits` procedure on both nodes.

## Reproduce

```
cd python/v5; python exp.py setup
python exp.py run --duration 600 --straggler-max 8 --stragglers 3 --min-user ro --no-trk --scanbar
python exp.py analyze ../../runs/v5/<run> --margins=-3000,-1000,-300,-100,0,300
```
