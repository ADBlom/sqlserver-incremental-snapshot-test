#!/usr/bin/env python3
"""Observe CDC progress after a commit to a table with no CDC capture instance.

The marker commit is the final intentional database write in the default test.
Then this script samples CDC state on the primary and the DMV/CDC positions on
the readable secondary while the database is otherwise idle.
"""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

import pyodbc

PROBE_TABLE = "dbo.watermark_probe_v4_non_cdc"


def lsn_hex(value):
    return value.hex() if value is not None else None


def numeric_lsn_hex(value):
    if value is None:
        return None
    n = int(value)
    if n < 0:
        raise ValueError(f"negative numeric LSN: {n}")
    vlf, remainder = divmod(n, 10**15)
    block, slot = divmod(remainder, 10**5)
    if vlf > 0xFFFFFFFF or block > 0xFFFFFFFF or slot > 0xFFFF:
        raise ValueError(f"numeric LSN component exceeds binary(10) range: {n}")
    return f"{vlf:08x}{block:08x}{slot:04x}"


def connect(args, port):
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={args.host},{port};DATABASE={args.database};UID=sa;"
        f"PWD={args.password};Encrypt={args.encrypt};TrustServerCertificate=yes",
        autocommit=True,
        timeout=args.login_timeout,
    )


def one_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip((column[0] for column in cur.description), row))


def endpoint_info(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT @@SERVERNAME AS server, DB_NAME() AS database_name, "
        "CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version, "
        "CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)) AS product_level, "
        "CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) AS edition, "
        "CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS nvarchar(20)) AS updateability, "
        "sys.fn_hadr_is_primary_replica(DB_NAME()) AS is_primary"
    )
    info = one_dict(cur)
    cur.execute(
        "SELECT ar.availability_mode_desc, drs.synchronization_state_desc, "
        "CONVERT(varchar(36), drs.replica_id) AS replica_id "
        "FROM sys.dm_hadr_database_replica_states AS drs "
        "JOIN sys.availability_replicas AS ar ON ar.replica_id = drs.replica_id "
        "WHERE drs.database_id = DB_ID() AND drs.is_local = 1"
    )
    info["ag"] = one_dict(cur)
    return info


def secondary_position(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT @@SERVERNAME AS server, DB_ID() AS database_id, "
        "CONVERT(varchar(36), drs.replica_id) AS replica_id, "
        "sys.fn_cdc_get_max_lsn() AS cdc_max_lsn, drs.last_commit_lsn, drs.last_redone_lsn "
        "FROM sys.dm_hadr_database_replica_states AS drs "
        "WHERE drs.database_id = DB_ID() AND drs.is_local = 1"
    )
    row = one_dict(cur)
    if row is None:
        raise RuntimeError("secondary has no local HADR database-replica state row")
    return {
        "server": row["server"],
        "database_id": row["database_id"],
        "replica_id": row["replica_id"],
        "cdc_max_lsn": lsn_hex(row["cdc_max_lsn"]),
        "last_commit_lsn": numeric_lsn_hex(row["last_commit_lsn"]),
        "last_redone_lsn": numeric_lsn_hex(row["last_redone_lsn"]),
    }


def primary_cdc_state(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT sys.fn_cdc_get_max_lsn() AS cdc_max_lsn, "
        "(SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping) AS mapping_max_lsn, "
        "(SELECT MAX(tran_end_time) FROM cdc.lsn_time_mapping) AS mapping_max_time"
    )
    mapping = one_dict(cur)
    mapping["cdc_max_lsn"] = lsn_hex(mapping["cdc_max_lsn"])
    mapping["mapping_max_lsn"] = lsn_hex(mapping["mapping_max_lsn"])

    cur.execute(
        "SELECT TOP (1) session_id, start_time, end_time, duration, scan_phase, error_count, "
        "start_lsn, current_lsn, end_lsn, tran_count, last_commit_lsn, last_commit_time, "
        "log_record_count, command_count, last_commit_cdc_lsn, failed_sessions_count, "
        "empty_scan_count "
        "FROM sys.dm_cdc_log_scan_sessions ORDER BY session_id DESC"
    )
    mapping["latest_scan"] = one_dict(cur)

    cur.execute(
        "SELECT TOP (1) j.enabled, a.run_requested_date, a.start_execution_date, "
        "a.stop_execution_date "
        "FROM msdb.dbo.sysjobs AS j "
        "LEFT JOIN msdb.dbo.sysjobactivity AS a ON a.job_id = j.job_id "
        "WHERE j.name = N'cdc.' + DB_NAME() + N'_capture' "
        "AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)"
    )
    mapping["capture_job"] = one_dict(cur)
    return mapping


def wait_for_marker(conn, marker, baseline_lsn, deadline, interval):
    cur = conn.cursor()
    while time.monotonic() < deadline:
        cur.execute(f"SELECT COUNT(*) FROM {PROBE_TABLE} WHERE id = ?", marker)
        visible = cur.fetchone()[0] == 1
        position = secondary_position(conn)
        commit_lsn = position["last_commit_lsn"]
        if visible and commit_lsn is not None and commit_lsn > baseline_lsn:
            return position
        time.sleep(interval)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--primary-port", type=int, default=14331)
    parser.add_argument("--secondary-port", type=int, default=14332)
    parser.add_argument("--database", default="testdb")
    parser.add_argument("--password", default=os.environ.get("MSSQL_SA_PASSWORD"))
    parser.add_argument("--encrypt", choices=("yes", "no"), default="yes")
    parser.add_argument("--login-timeout", type=int, default=60)
    parser.add_argument("--duration", type=float, default=180,
                        help="idle observation duration after the marker reaches the secondary")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between paired primary/secondary samples")
    parser.add_argument("--marker-timeout", type=float, default=60,
                        help="seconds to wait for the marker and its DMV position on the secondary")
    parser.add_argument("--out", default="../../runs/v4")
    args = parser.parse_args()
    if not args.password:
        parser.error("set MSSQL_SA_PASSWORD or pass --password")
    if args.duration <= 0 or args.interval <= 0 or args.marker_timeout <= 0:
        parser.error("--duration, --interval, and --marker-timeout must be positive")

    primary = connect(args, args.primary_port)
    secondary = connect(args, args.secondary_port)
    run_dir = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=False)
    samples_path = run_dir / "samples.jsonl"
    try:
        primary_info = endpoint_info(primary)
        secondary_info = endpoint_info(secondary)
        if primary_info["updateability"] != "READ_WRITE" or primary_info["is_primary"] is not True:
            raise RuntimeError(f"primary endpoint is not the writable AG primary: {primary_info}")
        if secondary_info["updateability"] != "READ_ONLY" or secondary_info["is_primary"] is not False:
            raise RuntimeError(f"secondary endpoint is not a readable AG secondary: {secondary_info}")
        if primary_info["database_name"] != secondary_info["database_name"]:
            raise RuntimeError("primary and secondary connections resolved to different databases")

        cur = primary.cursor()
        cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()")
        if not cur.fetchone()[0]:
            raise RuntimeError(f"CDC is not enabled for database {args.database}")
        cur.execute(
            f"IF OBJECT_ID(N'{PROBE_TABLE}', N'U') IS NULL "
            f"CREATE TABLE {PROBE_TABLE} ("
            "id uniqueidentifier NOT NULL PRIMARY KEY, "
            "committed_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME())"
        )
        cur.execute(
            "SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(?)",
            PROBE_TABLE,
        )
        if cur.fetchone()[0]:
            raise RuntimeError(f"{PROBE_TABLE} must not have a CDC capture instance")

        table_deadline = time.monotonic() + args.marker_timeout
        while time.monotonic() < table_deadline:
            cur = secondary.cursor()
            cur.execute("SELECT OBJECT_ID(?)", PROBE_TABLE)
            if cur.fetchone()[0] is not None:
                break
            time.sleep(args.interval)
        else:
            raise TimeoutError("probe table did not become visible on the secondary")

        baseline_secondary = secondary_position(secondary)
        if baseline_secondary["last_commit_lsn"] is None:
            raise RuntimeError("secondary last_commit_lsn is NULL")
        baseline_primary = primary_cdc_state(primary)

        marker = str(uuid.uuid4())
        cur = primary.cursor()
        cur.execute(f"INSERT INTO {PROBE_TABLE}(id) VALUES (?)", marker)
        marker_commit_time = time.time()
        target = wait_for_marker(
            secondary,
            marker,
            baseline_secondary["last_commit_lsn"],
            time.monotonic() + args.marker_timeout,
            args.interval,
        )
        if target is None:
            raise TimeoutError("marker did not become visible with an advanced secondary DMV LSN")

        meta = {
            "primary": primary_info,
            "secondary": secondary_info,
            "database": args.database,
            "marker": marker,
            "marker_commit_wall_time": marker_commit_time,
            "baseline_primary_cdc": baseline_primary,
            "baseline_secondary": baseline_secondary,
            "target_dmv_last_commit_lsn": target["last_commit_lsn"],
            "target_observed_secondary": target,
            "duration_seconds": args.duration,
            "interval_seconds": args.interval,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")

        deadline = time.monotonic() + args.duration
        with samples_path.open("w") as samples_file:
            while True:
                sampled_at = time.time()
                primary_state = primary_cdc_state(primary)
                secondary_state = secondary_position(secondary)
                record = {
                    "t": sampled_at,
                    "elapsed_seconds": args.duration - max(0, deadline - time.monotonic()),
                    "primary": primary_state,
                    "secondary": secondary_state,
                }
                samples_file.write(json.dumps(record, default=str) + "\n")
                samples_file.flush()
                if time.monotonic() >= deadline:
                    break
                time.sleep(args.interval)

        with samples_path.open() as samples_file:
            samples = [json.loads(line) for line in samples_file if line.strip()]
        target_lsn = meta["target_dmv_last_commit_lsn"]
        reached = [
            sample for sample in samples
            if sample["primary"]["cdc_max_lsn"] is not None
            and sample["primary"]["cdc_max_lsn"] >= target_lsn
        ]
        mapping_reached = [
            sample for sample in samples
            if sample["primary"]["mapping_max_lsn"] is not None
            and sample["primary"]["mapping_max_lsn"] >= target_lsn
        ]
        advanced_after_target = [
            sample for sample in samples
            if sample["secondary"]["last_commit_lsn"] is not None
            and sample["secondary"]["last_commit_lsn"] > target_lsn
        ]
        report = {
            "status": "cdc_reached_target" if reached else "cdc_did_not_reach_target",
            "target_dmv_last_commit_lsn": target_lsn,
            "cdc_reached_target": bool(reached),
            "mapping_reached_target": bool(mapping_reached),
            "secondary_dmv_advanced_beyond_target": bool(advanced_after_target),
            "samples_with_secondary_dmv_beyond_target": len(advanced_after_target),
            "max_secondary_last_commit_lsn": (
                max((sample["secondary"]["last_commit_lsn"] for sample in samples
                     if sample["secondary"]["last_commit_lsn"] is not None), default=None)
            ),
            "first_cdc_reach_elapsed_seconds": reached[0]["elapsed_seconds"] if reached else None,
            "first_mapping_reach_elapsed_seconds": (
                mapping_reached[0]["elapsed_seconds"] if mapping_reached else None
            ),
            "sample_count": len(samples),
            "start": samples[0] if samples else None,
            "end": samples[-1] if samples else None,
            "run_dir": str(run_dir),
        }
        (run_dir / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(json.dumps(report, indent=2, default=str))
        return 0 if reached else 2
    except (pyodbc.Error, OSError, RuntimeError, TimeoutError, ValueError) as exc:
        print(f"v4 probe failed: {exc}", file=sys.stderr)
        return 1
    finally:
        primary.close()
        secondary.close()


if __name__ == "__main__":
    sys.exit(main())
