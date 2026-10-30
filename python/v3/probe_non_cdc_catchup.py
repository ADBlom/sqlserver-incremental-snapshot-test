#!/usr/bin/env python3
"""Probe whether CDC reaches the replica-local watermark after a non-CDC commit.

Run only against a disposable/test database. The probe creates
dbo.watermark_probe_non_cdc if needed and leaves its marker row in place.
"""

import argparse
import json
import sys
import time
import uuid
from argparse import Namespace

import pyodbc

from common import add_conn_args, connect, fetch_pos

PROBE_TABLE = "dbo.watermark_probe_non_cdc"


def endpoint_info(args, port):
    conn = connect(args, port)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT @@SERVERNAME, DB_NAME(), "
            "CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), "
            "CAST(SERVERPROPERTY('ProductLevel') AS nvarchar(128)), "
            "CAST(SERVERPROPERTY('Edition') AS nvarchar(128)), "
            "CAST(DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS nvarchar(20)), "
            "sys.fn_hadr_is_primary_replica(DB_NAME())"
        )
        row = cur.fetchone()
        cur.execute(
            "SELECT ar.availability_mode_desc, drs.synchronization_state_desc "
            "FROM sys.dm_hadr_database_replica_states AS drs "
            "JOIN sys.availability_replicas AS ar ON ar.replica_id = drs.replica_id "
            "WHERE drs.database_id = DB_ID() AND drs.is_local = 1"
        )
        ag = cur.fetchone()
        return {
            "server": row[0],
            "database": row[1],
            "product_version": row[2],
            "product_level": row[3],
            "edition": row[4],
            "updateability": row[5],
            "is_primary": bool(row[6]) if row[6] is not None else None,
            "availability_mode": ag[0] if ag else None,
            "synchronization_state": ag[1] if ag else None,
        }
    finally:
        conn.close()


def wait_for_probe_table(cur, deadline, interval):
    while time.monotonic() < deadline:
        cur.execute("SELECT OBJECT_ID(?)", PROBE_TABLE)
        if cur.fetchone()[0] is not None:
            return True
        time.sleep(interval)
    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_conn_args(parser)
    parser.add_argument("--primary-host", help="override --host for the primary connection")
    parser.add_argument("--secondary-host", help="override --host for the secondary connection")
    parser.add_argument("--timeout", type=float, default=60,
                        help="seconds to wait for replica visibility and CDC catch-up")
    parser.add_argument("--interval", type=float, default=0.25,
                        help="seconds between secondary position samples")
    args = parser.parse_args()
    if args.timeout <= 0 or args.interval <= 0:
        parser.error("--timeout and --interval must be positive")

    primary_args = Namespace(**vars(args))
    primary_args.host = args.primary_host or args.host
    secondary_args = Namespace(**vars(args))
    secondary_args.host = args.secondary_host or args.host
    primary = endpoint_info(primary_args, args.primary_port)
    secondary = endpoint_info(secondary_args, args.secondary_port)
    if primary["updateability"] != "READ_WRITE" or primary["is_primary"] is not True:
        raise SystemExit(f"primary endpoint is not the writable AG primary: {primary}")
    if secondary["updateability"] != "READ_ONLY" or secondary["is_primary"] is not False:
        raise SystemExit(f"secondary endpoint is not a readable AG secondary: {secondary}")
    if primary["database"] != secondary["database"]:
        raise SystemExit("primary and secondary connections resolved to different databases")

    pconn = connect(primary_args, args.primary_port)
    sconn = connect(secondary_args, args.secondary_port)
    try:
        pcur, scur = pconn.cursor(), sconn.cursor()
        pcur.execute(
            f"IF OBJECT_ID(N'{PROBE_TABLE}', N'U') IS NULL "
            f"CREATE TABLE {PROBE_TABLE} ("
            "id uniqueidentifier NOT NULL PRIMARY KEY, "
            "committed_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME())"
        )
        pcur.execute(
            "SELECT COUNT(*) FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(?)",
            PROBE_TABLE,
        )
        if pcur.fetchone()[0]:
            raise SystemExit(f"{PROBE_TABLE} is CDC-enabled; use a separate non-CDC table")

        table_deadline = time.monotonic() + args.timeout
        if not wait_for_probe_table(scur, table_deadline, args.interval):
            raise SystemExit("probe table did not redo to the secondary before timeout")

        baseline = fetch_pos(scur)
        if baseline["dc"] is None:
            raise SystemExit("secondary last_commit_lsn is NULL")

        marker = str(uuid.uuid4())
        pcur.execute(f"INSERT INTO {PROBE_TABLE}(id) VALUES (?)", marker)
        commit_started = time.monotonic()
        deadline = commit_started + args.timeout
        target = None
        target_seen_at = None
        final_pos = baseline
        marker_visible = False
        while time.monotonic() < deadline:
            scur.execute(f"SELECT COUNT(*) FROM {PROBE_TABLE} WHERE id = ?", marker)
            marker_visible = scur.fetchone()[0] == 1
            final_pos = fetch_pos(scur)
            if marker_visible and final_pos["dc"] is not None and final_pos["dc"] > baseline["dc"]:
                target = final_pos["dc"]
                target_seen_at = time.monotonic()
                break
            time.sleep(args.interval)

        if target is None:
            result = {
                "status": "target_not_observed_within_timeout",
                "marker": marker,
                "marker_visible": marker_visible,
                "baseline": baseline,
                "final": final_pos,
                "primary": primary,
                "secondary": secondary,
                "timeout_seconds": args.timeout,
            }
            print(json.dumps(result, indent=2))
            return 2

        reached = final_pos["l"] is not None and final_pos["l"] >= target
        while not reached and time.monotonic() < deadline:
            time.sleep(args.interval)
            final_pos = fetch_pos(scur)
            reached = final_pos["l"] is not None and final_pos["l"] >= target

        result = {
            "status": "cdc_reached_target" if reached else "cdc_did_not_reach_target",
            "marker": marker,
            "baseline": baseline,
            "target_dmv_last_commit_lsn": target,
            "target_observed_after_seconds": target_seen_at - commit_started,
            "cdc_reached_after_seconds": (
                time.monotonic() - target_seen_at if reached else None
            ),
            "final": final_pos,
            "primary": primary,
            "secondary": secondary,
            "timeout_seconds": args.timeout,
        }
        print(json.dumps(result, indent=2))
        return 0 if reached else 2
    except pyodbc.Error as exc:
        raise SystemExit(f"SQL Server probe failed: {exc}") from exc
    finally:
        pconn.close()
        sconn.close()


if __name__ == "__main__":
    sys.exit(main())
