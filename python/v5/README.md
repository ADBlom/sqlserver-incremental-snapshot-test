# V5: watermarks for read-only chunked (incremental) snapshots

Question: which **read-only** values can bracket a chunk read (low before, high after) so that a
Debezium-style window (`dedupe stream events in (low, high]`) is correct, on the AG **secondary**
and on the **primary**? Debezium can only close the window once the CDC stream position
(`fn_cdc_get_max_lsn()`) reaches `high`, so a mark must be *safe* **and** *reachable*.

## Method (`exp.py`)

`python exp.py setup; python exp.py run --duration 130 [--iso snapshot]; python exp.py analyze <run>`

Workload on the primary (`dbo.wm`, CDC-enabled): updates, delete+reinsert txns, multi-row txns,
stragglers (txn open 0.3-8 s), standalone inserts/deletes, plus a non-CDC noise table. Phases:
60 % load, 20 % idle (no writes), 20 % non-CDC writes only. Chunk readers run on **both** nodes
(whole-table read in 200-row fetches with jitter, so the read is long). Each read records
candidate marks before (low) and after (high). Ground truth = commit LSN of every row version
from `cdc.dbo_wm_CT`. Per key:

* **LOW violation** - chunk older than a change with commit LSN <= low (permanent regression).
* **HIGH violation** - chunk contains a change with commit LSN > high (stale stream event later
  overwrites the fresher chunk row -> transient regression).
* **Reach delay** - time until the same node's `fn_cdc_get_max_lsn()` >= high ("never" = window
  cannot close).

## Results (runs `20261030-120747` RC, `20261030-121142` SNAPSHOT; ~1 400 chunks, 0 violations for the winner)

| Candidate | Where | Safe as high | Reachable | Notes |
|---|---|---|---|---|
| `fn_cdc_get_max_lsn()` | both | **No** (HIGH viol. in ~40 % of chunks) | yes | Lags writes. Safe as *low* (0 viol.). |
| `dm_db_log_stats.log_end_lsn` | primary | yes | **never when idle / non-CDC commits** | **Unsafe as low under SNAPSHOT** (8/312 chunks): commit logged but not yet visible to the snapshot. |
| `dm_hadr_database_replica_states` `last_commit_lsn` / `end_of_log_lsn` | primary (AG only) | yes | never when idle | same as above, 45/312 LOW viol. under SNAPSHOT |
| same DMV `last_commit_lsn` / `last_redone_lsn` | secondary | yes (0 viol.) | **never when idle** (117/126 chunks) | Safe bounds but the CDC position cannot reach them: capture/non-CDC commits move the DMV, not CDC. |
| DMV `last_hardened_lsn` / `end_of_log_lsn` | secondary | n/a | - | Ahead of redo: LOW violations. Not usable as low. |
| `dm_db_log_stats` | secondary | - | - | Returns NULLs. |
| `dm_tran_database_transactions` min begin / max last LSN | primary | max_last: yes; min_begin: **No** (HIGH viol.) | not when idle | Only active txns; empty most of the time. Useful only to bound a log scan (see below). Not available on secondary (no rows). |
| `dm_cdc_log_scan_sessions` "scan barrier" (wait for completed scans, then `cdc_max`) | primary | **No under load** (1-3 HIGH viol. per run); **yes if the awaited scan is empty** (see Candidate B) | ~5 s when idle | Non-empty scans stop at 500 transactions, so a completed scan proves nothing; an empty scan after the read proves capture is caught up. Empty scans report zero LSNs. |
| **`fn_dblog`: max commit LSN of a user transaction that modified the chunk table, after the stream position (`trk_*`)** | **both** | **yes: 0 LOW / 0 HIGH over all runs** | **yes: ~0.03 s when idle / non-CDC writes; = capture lag (2-5 s) under load** | See below. |

### Why the winner works

CDC only ever advances to *tracked* commit LSNs, so the only high mark the stream can reach is a
tracked commit LSN. The chunk can only contain changes of transactions that touched the chunk
table, so `high = max commit LSN of any (user) transaction that modified the table, found in the
log after the stream position` is both an upper bound of everything the read could see and a
value CDC will reach. If no such transaction exists, `high = low` (window closes immediately).
It ignores untracked commits (capture agent, non-CDC tables, checkpoints), which is what makes
`log_end`/DMV marks unreachable.

## Candidate for Debezium

`low` = stream position (`fn_cdc_get_max_lsn()` read before the chunk; lags => wider window, safe
on both nodes, both isolation levels).

`high` (after the chunk read, same connection/node), `trk_exact` in `exp.py`:

```sql
-- @s = '0x' + low formatted as 00000079:000045E8:0003 (fn_dblog takes literal-style LSN text)
SELECT [Transaction ID],
  MAX(CASE WHEN Operation='LOP_BEGIN_XACT' THEN [Transaction Name] END),
  MAX(CASE WHEN Operation='LOP_COMMIT_XACT' THEN [Current LSN] END),
  MAX(CASE WHEN Operation IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS','LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS')
            AND Context IN ('LCX_CLUSTERED','LCX_HEAP') AND AllocUnitName LIKE 'dbo.wm.%' THEN 1 ELSE 0 END),
  MIN(CASE WHEN Operation<>'LOP_BEGIN_XACT' THEN [Previous LSN] END)
FROM fn_dblog(@s, NULL) WHERE [Transaction ID] <> '0000:00000000' GROUP BY [Transaction ID];
```

Client side: keep transactions named `user_transaction/INSERT/UPDATE/DELETE/MERGE/implicit_transaction`
(drops `GhostCleanupTask`, `SplitPage`, ...) that touch the table and committed above `low`;
`high = max(commit LSN)`. Transactions that committed in range but have no `BEGIN` (started before
`low`) are chased: re-run from their `[Previous LSN]` until the BEGIN is found (otherwise a long
transaction that modified the table before `low` but committed later would be missed). Window closes
when the stream position >= `high`. Cost 40-80 ms per chunk in these runs (scales with log between
the stream position and now, i.e. capture lag).

Usability and caveats:

* Uses `fn_dblog` (undocumented, unsupported; unavailable on Azure SQL / RDS). Runs on the readable
  secondary too (single-node, no DMV) and on a plain primary without AG. **Privileges: see the next section.**
* Fails closed: capture job stopped -> stream never reaches `high` -> window stays open (no
  corruption). The log from the stream position must still exist (it does while capture runs; a
  truncated log on the secondary makes `fn_dblog` error -> treat as retry).
* `AllocUnitName` must match the chunk table (schema.table; schema/table renames, partitioned
  tables, columnstore not tested). Transaction-name filter is heuristic and version-dependent.
* Naive variants without straddler chasing (`trk_cdc`, fixed lookback `trk_lb256k/4m`,
  active-transaction start `trk_active`) also showed 0 violations here, but are only safe when no
  transaction spans the start position; use `trk_exact`.
* Not tested: AG failover mid-window, capture stopped, SYNCHRONOUS_COMMIT, very long (minutes)
  transactions, multi-table windows, other SQL Server versions (tested 2022 CU27 only).
* Environment note: `ALLOW_SNAPSHOT_ISOLATION` was toggled ON for the SNAPSHOT run and restored to OFF.
  Capture `pollinginterval` shows ~5 s effective idle cadence.

## Least privilege (can a read-only login do this?)

Tested with a login that is NOT sysadmin, NOT db_owner, member of `db_datareader` only
(`exp.py run --min-user ro`; INSERT verified denied on both nodes):

| Grants of the login | `cdc_max` + chunk read | `fn_dblog` | HADR DMV / scan sessions / `dm_tran_*` / `dm_db_log_stats` |
|---|---|---|---|
| `db_datareader` | yes | **denied** | denied |
| + `VIEW DATABASE PERFORMANCE STATE` | yes | denied | `dm_hadr_database_replica_states`, `dm_cdc_log_scan_sessions` ok |
| + `VIEW SERVER PERFORMANCE STATE` | yes | denied | all ok (`dm_db_log_stats`, `dm_tran_*`) |
| + `db_owner` + `GRANT SELECT ON sys.fn_dblog` (in master) | yes | ok | ok - but db_owner can write on the primary |
| `GRANT CONTROL SERVER` | yes | ok | ok (nearly sysadmin) |

`fn_dblog` is hard-gated: error 9010 "Only members of the sysadmin fixed server role and the db_owner
fixed database role have this permission"; a `GRANT SELECT` alone is not enough. On a readable
secondary db_owner cannot write (database is read-only), but the same login is db_owner on the primary.

**Working least-privilege setup** (`least_privilege_setup.sql`): a DBA creates, once on the primary
(replicates to the secondary), `dbo.dbz_tracked_commits` = the `fn_dblog` aggregation `WITH EXECUTE AS OWNER`,
and grants the connector login only `db_datareader` + `EXECUTE` on it (login must exist on each replica).
No TRUSTWORTHY, no certificate needed (checked with `is_trustworthy_on = 0` on both nodes). Result of the full
workload (run `20261030-122441`: 90 s load, stragglers up to 8 s, 30 s idle, 30 s non-CDC writes, primary
under SNAPSHOT) with the readers running as that login:

| Node | chunks | LOW viol. | HIGH viol. | reach delay load p50/p95 | idle | non-CDC writes | cost/chunk |
|---|---|---|---|---|---|---|---|
| primary | 627 | 0 | 0 | 2.4 / 4.7 s | 0.04 s (p95 2.6) | 0.03 s | 84 ms |
| secondary | 613 | 0 | 0 | 2.6 / 4.8 s | 0.03 s (p95 2.7) | 0.02 s | 91 ms |
| (`cdc_max` as high, for contrast) | | 0 | 291 / 281 | | | | |

What remains privileged is the one-time DBA step (create a wrapper procedure; it exposes the log
aggregation of the database to anyone granted EXECUTE) - *not* the connector's runtime login. Same footprint as
Debezium's signal table (DBA-created object), but no writes. The proc hardcodes the table name
(`AllocUnitName LIKE 'dbo.wm.%'`); a Debezium version would take it as a parameter or one proc per table.

## Candidates without any DBA-installed object (db_datareader only)

Found after the privilege work above. Both use only data the connector can already read; nothing is written.
Run: `exp.py run --min-user ro --no-trk --scanbar` (readers as `db_datareader`; `--scanbar` also needs
`VIEW DATABASE PERFORMANCE STATE` for the primary scan-session view). Runs `20261030-144933`, `20261030-145744`
(40 % load with 8 s stragglers, 30 % sparse tracked writes every 0.1-3 s, 15 % idle, 15 % non-CDC writes).

**Key finding - the CDC "idle heartbeat".** When no tracked commit arrives, the capture job inserts a row with
`tran_id = 0x00` into `cdc.lsn_time_mapping` **every 300 s** (measured: 46.0 s, 346.7 s, 648.1 s; also during
non-CDC write load). Its `start_lsn` is the capture scan end, so it covers all untracked commits before it. That is the only
progress signal for untracked log that the secondary ever receives, but it is slow. Capture's own commits
(heartbeat, CT writes) sit *above* the last mapped LSN, which is why any log-position mark (`log_end`, DMV commit
LSN) is unreachable until a later heartbeat/tracked commit.

### Candidate A - time watermark (no extra privilege, primary or secondary)

`low` = `fn_cdc_get_max_lsn()` before the chunk read. After the read take `T` = server time
(`SYSDATETIME()` of the reading node, which must be in the same clock/time zone as the primary: `tran_end_time`
is the primary's local commit time). `high` = `start_lsn` of the first `cdc.lsn_time_mapping` row with
`tran_end_time > T + margin`; the window closes when the stream position >= `high`. Why it is safe: every commit
visible to the chunk happened (on the primary) before the chunk finished reading, hence has `tran_end_time <= T`, and
a later commit has a larger LSN. Any later tracked commit - of *any* captured table - or the 5-min heartbeat closes it.

| Margin (ms, added to T) | chunks (prim/sec) | HIGH violations | LOW violations |
|---|---|---|---|
| -3000 / -1000 / -300 / -100 | 1129 / 2562 | 1063/2392, 344/1193, 35/703, 11/20 | 0 |
| **0 / +50 / +300** | 1129 / 2562 | **0 / 0** | 0 |

Reach delay: load 2-6 s (capture lag), sparse tracked writes 3-6 s, truly idle table <= 300 s (heartbeat).
Cost: one extra `SELECT` on the mapping table. Safety depends on the clock: it tolerates ~0 ms error, so use a margin
>= worst NTP skew (1-5 s), or read `T` from the primary connection (single clock, no skew). Not tested: DST/NTP steps,
real cross-host skew (containers share one clock).

### Candidate B - empty-scan barrier (idle fast path, needs a primary connection)

Needs `VIEW DATABASE PERFORMANCE STATE` and a connection to the **primary** (capture scans exist only there). After
the chunk read (primary or secondary) read `sys.dm_cdc_log_scan_sessions` (latest session: `session_id`, `empty_scan_count`,
`scan_phase`) and wait for 1 further *empty* scan (2 if `scan_phase <> 'Done'`, because a scan running now may have
started before the read) in the same session (or >= that many in a newer one). Then every tracked commit visible to
the chunk has been captured, so `high` = the primary's `fn_cdc_get_max_lsn()` read after the barrier (the secondary
stream must reach it, i.e. wait for replication). Apply it only if the last scan was empty (`empty_scan_count > 0`);
under tracked load use candidate A.
Result: 0 LOW / 0 HIGH (35 + 35 barrier windows in the idle and non-CDC-write phases, both nodes); closes in
**~5 s** (capture polls every 5 s) instead of up to 300 s. My earlier barrier attempt failed only because it
also waited under load, where scans are never empty (sessions end after 500 transactions) and the position read is not a proof.

### Recommended combination

`high` via A (+margin). If the last capture scan is empty and a primary connection is acceptable, close early
with B. Together: load 2-6 s, sparse 3-6 s, idle/non-CDC ~5 s (B) or <= 5 min (A only). Both are plain SELECTs
on `cdc.*` / one DMV; no `fn_dblog`, no procedure, no sysadmin/db_owner. The fn_dblog procedure above remains the
only mechanism that closes under load in ~0.03 s after capture catches up without a clock assumption, but it needs the DBA step.

## Rejected ideas

* Idle-only proofs from `dm_cdc_log_scan_sessions` (no LSN on empty scans).
* Time based (`lsn_time_mapping`, `fn_cdc_map_time_to_lsn`): clock-based, same lag as `cdc_max`.
* XSN / `@@DBTS`: not comparable to LSNs.
* Writing watermark/heartbeat rows: not read-only (the existing Debezium signal-table approach).
