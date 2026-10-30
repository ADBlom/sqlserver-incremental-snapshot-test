# Repository instructions

## Build, test, and lint

There is no separate build step or configured linter. Install the Python dependency with:

```bash
python3 -m pip install -r python/requirements.txt
```

The pure-Python analyzer tests are standalone assertion scripts, not pytest tests. Run each from its version directory:

```bash
cd python/v2 && python3 test_analysis_core.py
cd python/v3 && python3 test_analysis_core.py
```

There is no per-test selector; each command runs that version's complete synthetic-data test script. To check Python syntax across the repository:

```bash
python3 -m compileall -q python
```

For the original `orders` experiment, follow the README: run `./scripts/provision.sh`, then from `python/` install requirements, run `python3 harness.py --rows 5000`, and run `python3 analyze.py`. The harness needs Docker Compose, SQL Server ODBC Driver 18, and the provisioned SQL Server availability group.

The `python/v2` and `python/v3` experiments are separate command-line programs. From the corresponding version directory, run `python3 harness.py setup`, then `python3 harness.py run [options]`, then `python3 analyze.py [run-directory]`. Each uses `common.py` and `analysis_core.py` from its own directory; keep the working directory there so sibling-module imports and the default `runs/` paths resolve correctly.

The v3 `probe_non_cdc_catchup.py` is a bounded SQL integration probe for whether CDC reaches the secondary DMV position after a non-CDC-table commit. V4 in `python/v4` samples CDC progress, `cdc.lsn_time_mapping`, primary capture scan/job diagnostics, and secondary DMV positions through an otherwise idle observation period; `probe_capture_commit_boundary.py` also correlates capture-agent transaction-log commits with a CDC control change and non-CDC marker. Both probes write marker/test rows and leave them in the database; only run them against test AG databases. V4 log inspection uses undocumented `sys.fn_dblog` and is exploratory, not a production mechanism. V4 usage and output fields are documented in `python/v4/README.md`. `python3 test_analysis_core.py` in v3 also checks numeric-LSN component bounds and replica identity matching.

## Architecture

The project tests whether a CDC LSN read on a readable SQL Server Always On secondary can safely describe the base-table snapshot visible in the same transaction. Docker Compose defines the two SQL Server nodes; `scripts/provision.sh` configures certificate-authenticated HADR endpoints, the availability group, and CDC. The SQL scripts provide CDC ground truth and replication/capture health queries.

The root-level Python harness is the original, simpler `dbo.orders` insert experiment: a writer commits rows on the primary while an observer reads `MAX(id)` and `sys.fn_cdc_get_max_lsn()` in a snapshot transaction on the secondary. Its analyzer fetches captured row LSNs from the primary and checks observations for ordering violations.

`python/v2` and `python/v3` are independent evolutions of the experiment, targeting `dbo.hot` with concurrent updates, delete/reinsert transactions, multi-row transactions, and long-running “straggler” transactions. Their observers exercise `pre`, `in_first`, and `snap_first` watermark/snapshot orderings. The harness records per-thread JSONL events and run metadata under `runs/<timestamp>/`; the analyzers compare committed row versions with CDC change-table ground truth and check completeness. V3 additionally samples HADR DMV positions and uses them to evaluate watermark bounds and CDC catch-up delay.

## Codebase conventions

- Each threaded writer, observer, and watcher owns its own ODBC connection/cursor. Do not share a `pyodbc` connection across threads.
- In the v2/v3 analyzers, CDC LSNs are represented as fixed-width, lowercase 20-character hex strings (10-byte binary values); lexicographic comparison preserves LSN ordering. V3 converts numeric HADR DMV LSNs into this same representation before comparing them.
- V3 position reads and chunk observations include server, database, and replica identity. Keep both watermarks and the chunk read on the same replica; analyzer affinity violations indicate that an observation crossed replicas or database endpoints.
- Treat each harness generation as a separate experiment. The versioned directories intentionally have their own `common.py`, `analysis_core.py`, harness, analyzer, and tests; make changes in the matching generation rather than assuming root-level code is shared.
- Preserve the distinction between `pre` (watermark before transaction), `in_first` (watermark read first inside transaction), and `snap_first` (snapshot fixed by a table read before watermark). These orderings encode different test hypotheses.
- Run outputs are JSON Lines: each record is an independent event with a monotonic timestamp. V2/v3 runs also store `meta.json` and separate writer, observer, watcher, and chaos-event logs in the run directory.
