#!/usr/bin/env python3
"""
Loads observer_log.jsonl (produced by harness.py) and, for each observation,
looks up the ground-truth LSN that the CDC capture job actually assigned to
the row `max_id_snapshot` (read from cdc.dbo_orders_CT on the PRIMARY).

The property under test:

    For every observation, max_lsn (read via sys.fn_cdc_get_max_lsn() inside
    the SAME snapshot transaction as the base-table read) must be >= the true
    LSN of the row that transaction's snapshot considered the newest.

LSNs are fixed-length (10-byte) binary values, so comparing their hex strings
lexicographically gives the same ordering as comparing them numerically.

Any row where this fails is logged as a VIOLATION -- exactly the failure mode
that would break a watermark-based incremental snapshot algorithm ported from
Debezium's MySQL implementation.
"""

import argparse
import json

import pyodbc

DEFAULT_PRIMARY_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,14331;"
    "DATABASE=testdb;UID=sa;PWD=YourStrong!Passw0rd;TrustServerCertificate=yes"
)


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def fetch_ground_truth(conn_str):
    """Returns {row_id: lsn_hex} for every insert captured by CDC."""
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, __$start_lsn FROM cdc.dbo_orders_CT WHERE __$operation = 2"
    )
    ground_truth = {}
    for row_id, lsn_bin in cur.fetchall():
        ground_truth[row_id] = lsn_bin.hex()
    conn.close()
    return ground_truth


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--observer-log", default="observer_log.jsonl")
    ap.add_argument("--primary-conn", default=DEFAULT_PRIMARY_CONN)
    args = ap.parse_args()

    print("Fetching ground-truth LSNs from cdc.dbo_orders_CT on the primary...")
    ground_truth = fetch_ground_truth(args.primary_conn)
    print(f"  {len(ground_truth)} captured inserts found.")

    observations = load_jsonl(args.observer_log)
    print(f"Loaded {len(observations)} observations from {args.observer_log}.")

    violations = []
    checked = 0
    max_id_seen_without_capture = 0

    for obs in observations:
        max_id = obs.get("max_id_snapshot")
        max_lsn = obs.get("max_lsn")
        if max_id is None:
            continue  # no rows visible yet

        true_lsn = ground_truth.get(max_id)
        if true_lsn is None:
            # The row is visible in the base-table snapshot, but the capture
            # job hasn't produced a CDC row for it AT ALL yet (on the primary,
            # as of when we ran fetch_ground_truth). That's expected latency,
            # not itself a violation -- but it does mean max_lsn can't have
            # "seen" this row's change yet either. Track it separately.
            max_id_seen_without_capture += 1
            continue

        checked += 1
        if max_lsn is None or max_lsn < true_lsn:
            violations.append(
                {
                    "t": obs["t"],
                    "max_id_snapshot": max_id,
                    "observed_max_lsn": max_lsn,
                    "true_lsn_for_max_id": true_lsn,
                }
            )

    print(f"\nChecked {checked} observations against ground truth.")
    print(
        f"{max_id_seen_without_capture} observations referenced a row not yet "
        f"captured by CDC on the primary at analysis time (expected latency, "
        f"not counted as a violation -- re-run analysis after the capture job "
        f"has fully caught up if this number is large)."
    )

    if violations:
        print(f"\n*** {len(violations)} ORDERING VIOLATIONS FOUND ***\n")
        for v in violations[:20]:
            print(
                f"  t={v['t']:.3f}  max_id_snapshot={v['max_id_snapshot']}  "
                f"observed_max_lsn={v['observed_max_lsn']}  "
                f"true_lsn_for_that_row={v['true_lsn_for_max_id']}"
            )
        if len(violations) > 20:
            print(f"  ... and {len(violations) - 20} more.")
    else:
        print("\nNo violations found: fn_cdc_get_max_lsn(), read inside the same "
              "snapshot transaction, was always >= the true LSN of the newest "
              "row visible to that transaction's snapshot.")


if __name__ == "__main__":
    main()
