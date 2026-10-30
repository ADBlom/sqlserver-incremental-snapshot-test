#!/usr/bin/env python3
"""Test whether CDC-agent transaction commits are a useful scan boundary.

Writes one CDC-enabled control row, then one row to a non-CDC table. After the
second row is visible on the readable secondary, observes whether CDC-agent
transactions commit beyond that secondary DMV position, and inspects whether
those agent transactions contain CDC change-table writes.
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
CDC_TABLE = "cdc.dbo_orders_CT"


def to_hex_lsn(value):
    return value.hex() if value is not None else None


def to_numeric_lsn_hex(value):
    if value is None:
        return None
    n = int(value)
    vlf, remainder = divmod(n, 10**15)
    block, slot = divmod(remainder, 10**5)
    if n < 0 or vlf > 0xFFFFFFFF or block > 0xFFFFFFFF or slot > 0xFFFF:
        raise ValueError(f"numeric LSN outside binary(10) component ranges: {n}")
    return f"{vlf:08x}{block:08x}{slot:04x}"


def normalize_log_lsn(value):
    return value.replace(":", "").replace("\x00", "").lower() if value else None


def connect(args, port):
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={args.host},{port};DATABASE={args.database};UID=sa;"
        f"PWD={args.password};Encrypt={args.encrypt};TrustServerCertificate=yes",
        autocommit=True,
        timeout=args.login_timeout,
    )


def fetch_one_dict(cur):
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip((column[0] for column in cur.description), row))


def endpoint_info(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT @@SERVERNAME AS server, DB_NAME() AS database_name, "
        "CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version, "
        "CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) AS edition, "
        "CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS nvarchar(20)) AS updateability, "
        "sys.fn_hadr_is_primary_replica(DB_NAME()) AS is_primary"
    )
    return fetch_one_dict(cur)


def primary_state(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT sys.fn_cdc_get_max_lsn() AS cdc_max_lsn, "
        "(SELECT MAX(start_lsn) FROM cdc.lsn_time_mapping) AS mapping_max_lsn, "
        "(SELECT MAX(tran_end_time) FROM cdc.lsn_time_mapping) AS mapping_max_time"
    )
    state = fetch_one_dict(cur)
    state["cdc_max_lsn"] = to_hex_lsn(state["cdc_max_lsn"])
    state["mapping_max_lsn"] = to_hex_lsn(state["mapping_max_lsn"])
    cur.execute(
        "SELECT TOP (1) session_id, start_time, end_time, duration, scan_phase, error_count, "
        "start_lsn, current_lsn, end_lsn, tran_count, last_commit_lsn, last_commit_time, "
        "last_commit_cdc_lsn, failed_sessions_count, empty_scan_count "
        "FROM sys.dm_cdc_log_scan_sessions ORDER BY session_id DESC"
    )
    state["latest_scan"] = fetch_one_dict(cur)
    return state


def secondary_state(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT @@SERVERNAME AS server, DB_ID() AS database_id, "
        "CONVERT(varchar(36), drs.replica_id) AS replica_id, "
        "sys.fn_cdc_get_max_lsn() AS cdc_max_lsn, drs.last_commit_lsn, drs.last_redone_lsn "
        "FROM sys.dm_hadr_database_replica_states AS drs "
        "WHERE drs.database_id = DB_ID() AND drs.is_local = 1"
    )
    state = fetch_one_dict(cur)
    if state is None:
        raise RuntimeError("no local HADR replica-state row on the secondary")
    state["cdc_max_lsn"] = to_hex_lsn(state["cdc_max_lsn"])
    state["last_commit_lsn"] = to_numeric_lsn_hex(state["last_commit_lsn"])
    state["last_redone_lsn"] = to_numeric_lsn_hex(state["last_redone_lsn"])
    return state


def capture_agent_spid(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT TOP (1) s.session_id, s.program_name "
        "FROM sys.dm_exec_sessions AS s "
        "JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id "
        "OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) AS txt "
        "WHERE s.program_name LIKE N'SQLAgent - TSQL JobStep%' "
        "AND txt.text LIKE N'%sp_cdc_scan%' "
        "ORDER BY r.start_time DESC"
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {"spid": row[0], "program_name": row[1]}


def agent_log_commits(conn, spid):
    cur = conn.cursor()
    cur.execute(
        """
        WITH log_records AS
        (
            SELECT [Current LSN], [Transaction ID], [Operation], [Transaction Name],
                   TRY_CONVERT(int, [SPID]) AS spid, [AllocUnitName]
            FROM sys.fn_dblog(NULL, NULL)
        ),
        transactions AS
        (
            SELECT [Transaction ID],
                   MAX(CASE WHEN [Operation] = N'LOP_BEGIN_XACT' THEN [Current LSN] END) AS begin_lsn,
                   MAX(CASE WHEN [Operation] = N'LOP_BEGIN_XACT' THEN [Transaction Name] END) AS transaction_name,
                   MAX(CASE WHEN [Operation] = N'LOP_BEGIN_XACT' THEN spid END) AS begin_spid,
                   MAX(CASE WHEN [Operation] = N'LOP_COMMIT_XACT' THEN [Current LSN] END) AS commit_lsn,
                   SUM(CASE WHEN [AllocUnitName] LIKE N'cdc.dbo_orders_CT%' THEN 1 ELSE 0 END) AS ct_log_records,
                   SUM(CASE WHEN [AllocUnitName] LIKE N'cdc.lsn_time_mapping%' THEN 1 ELSE 0 END) AS mapping_log_records
            FROM log_records
            GROUP BY [Transaction ID]
        )
        SELECT begin_lsn, commit_lsn, transaction_name, ct_log_records, mapping_log_records
        FROM transactions
        WHERE begin_spid = ? AND commit_lsn IS NOT NULL
        ORDER BY commit_lsn
        """,
        spid,
    )
    columns = [column[0] for column in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    for row in rows:
        row["commit_lsn_hex"] = normalize_log_lsn(row.pop("commit_lsn"))
        row["begin_lsn"] = normalize_log_lsn(row["begin_lsn"])
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--primary-port", type=int, default=14331)
    parser.add_argument("--secondary-port", type=int, default=14332)
    parser.add_argument("--database", default="testdb")
    parser.add_argument("--password", default=os.environ.get("MSSQL_SA_PASSWORD"))
    parser.add_argument("--encrypt", choices=("yes", "no"), default="yes")
    parser.add_argument("--login-timeout", type=int, default=60)
    parser.add_argument("--duration", type=float, default=45,
                        help="seconds to observe after the secondary sees the marker")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--marker-timeout", type=float, default=60)
    parser.add_argument("--out", default="../../runs/v4")
    args = parser.parse_args()
    if not args.password:
        parser.error("set MSSQL_SA_PASSWORD or pass --password")
    if min(args.duration, args.interval, args.marker_timeout) <= 0:
        parser.error("--duration, --interval, and --marker-timeout must be positive")

    primary = connect(args, args.primary_port)
    secondary = connect(args, args.secondary_port)
    run_dir = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        pinfo, sinfo = endpoint_info(primary), endpoint_info(secondary)
        if pinfo["updateability"] != "READ_WRITE" or pinfo["is_primary"] is not True:
            raise RuntimeError(f"primary endpoint is not the writable AG primary: {pinfo}")
        if sinfo["updateability"] != "READ_ONLY" or sinfo["is_primary"] is not False:
            raise RuntimeError(f"secondary endpoint is not a readable AG secondary: {sinfo}")

        cur = primary.cursor()
        cur.execute("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()")
        if not cur.fetchone()[0]:
            raise RuntimeError("CDC is not enabled for this database")
        cur.execute("SELECT is_tracked_by_cdc FROM sys.tables WHERE object_id = OBJECT_ID(N'dbo.orders')")
        row = cur.fetchone()
        if row is None or not row[0]:
            raise RuntimeError("dbo.orders must be CDC-enabled for the control change")
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
            raise RuntimeError(f"{PROBE_TABLE} unexpectedly has a CDC capture instance")

        baseline = secondary_state(secondary)
        if baseline["last_commit_lsn"] is None:
            raise RuntimeError("secondary last_commit_lsn is NULL")

        control_payload = f"v4-cdc-control-{uuid.uuid4()}"
        cur.execute(
            "INSERT INTO dbo.orders(payload) OUTPUT INSERTED.id VALUES (?)",
            control_payload,
        )
        control_id = cur.fetchone()[0]
        marker = str(uuid.uuid4())
        cur.execute(f"INSERT INTO {PROBE_TABLE}(id) VALUES (?)", marker)
        marker_commit_time = time.time()
        deadline = time.monotonic() + args.marker_timeout
        target = None
        while time.monotonic() < deadline:
            cur = secondary.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {PROBE_TABLE} WHERE id = ?", marker)
            marker_visible = cur.fetchone()[0] == 1
            state = secondary_state(secondary)
            if marker_visible and state["last_commit_lsn"] > baseline["last_commit_lsn"]:
                target = state["last_commit_lsn"]
                break
            time.sleep(args.interval)
        if target is None:
            raise TimeoutError("marker and advanced secondary DMV boundary not observed in time")

        meta = {
            "primary": pinfo,
            "secondary": sinfo,
            "database": args.database,
            "control_order_id": control_id,
            "control_payload": control_payload,
            "non_cdc_marker": marker,
            "marker_commit_wall_time": marker_commit_time,
            "baseline_secondary": baseline,
            "target_dmv_last_commit_lsn": target,
            "target_observed_secondary": state,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")

        samples_path = run_dir / "samples.jsonl"
        deadline = time.monotonic() + args.duration
        with samples_path.open("w") as output:
            while True:
                sampled_at = time.time()
                pstate = primary_state(primary)
                sstate = secondary_state(secondary)
                output.write(json.dumps({
                    "t": sampled_at,
                    "elapsed_seconds": args.duration - max(0, deadline - time.monotonic()),
                    "primary": pstate,
                    "secondary": sstate,
                }, default=str) + "\n")
                output.flush()
                if time.monotonic() >= deadline:
                    break
                time.sleep(args.interval)

        cur = primary.cursor()
        cur.execute(
            f"SELECT TOP (1) [__$start_lsn] FROM {CDC_TABLE} "
            "WHERE id = ? AND [__$operation] = 2 ORDER BY [__$start_lsn] DESC",
            control_id,
        )
        control_row = cur.fetchone()
        control_lsn = to_hex_lsn(control_row[0]) if control_row else None
        agent = capture_agent_spid(primary)
        if agent is None:
            raise RuntimeError("could not identify the running CDC capture-agent SPID")
        commits = agent_log_commits(primary, agent["spid"])
        beyond_target = [
            record for record in commits
            if record["commit_lsn_hex"] is not None
            and record["commit_lsn_hex"] > target
        ]
        cdc_write_commits_after_target = [
            record for record in beyond_target if record["ct_log_records"] > 0
        ]
        with samples_path.open() as input_file:
            samples = [json.loads(line) for line in input_file if line.strip()]
        scan_ends = [
            normalize_log_lsn(sample["primary"]["latest_scan"].get("end_lsn"))
            for sample in samples
            if sample["primary"].get("latest_scan")
            and sample["primary"]["latest_scan"].get("end_lsn")
            and normalize_log_lsn(sample["primary"]["latest_scan"]["end_lsn"])
        ]
        target_reached_by_cdc = any(
            sample["primary"]["cdc_max_lsn"] is not None
            and sample["primary"]["cdc_max_lsn"] >= target
            for sample in samples
        )
        report = {
            "status": "cdc_reached_target" if target_reached_by_cdc else "cdc_below_target",
            "target_dmv_last_commit_lsn": target,
            "cdc_control_row_captured": control_lsn is not None,
            "cdc_control_row_lsn": control_lsn,
            "cdc_max_reached_target": target_reached_by_cdc,
            "capture_scan_end_lsn_reached_target": any(end_lsn >= target for end_lsn in scan_ends),
            "max_capture_scan_end_lsn": max(scan_ends, default=None),
            "capture_agent": agent,
            "capture_agent_commits_after_target": beyond_target,
            "capture_agent_commits_with_change_table_writes_after_target": (
                cdc_write_commits_after_target
            ),
            "agent_commit_after_target_with_cdc_writes": bool(cdc_write_commits_after_target),
            "sample_count": len(samples),
            "first_sample": samples[0] if samples else None,
            "last_sample": samples[-1] if samples else None,
            "run_dir": str(run_dir),
        }
        (run_dir / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(json.dumps(report, indent=2, default=str))
        return 0 if target_reached_by_cdc else 2
    except (pyodbc.Error, OSError, RuntimeError, TimeoutError, ValueError) as exc:
        print(f"v4 capture-boundary probe failed: {exc}", file=sys.stderr)
        return 1
    finally:
        primary.close()
        secondary.close()


if __name__ == "__main__":
    sys.exit(main())
