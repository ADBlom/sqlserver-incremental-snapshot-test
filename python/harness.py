#!/usr/bin/env python3
"""
Runs two concurrent loops:

  - writer_loop:   inserts rows into dbo.orders on the PRIMARY, one commit per row.
  - observer_loop: on the SECONDARY, opens a SNAPSHOT-isolation transaction, reads
                    sys.fn_cdc_get_max_lsn() and MAX(id) from dbo.orders within
                    that same transaction, then commits.

Both loops log (monotonic_timestamp, ...) events to JSON-lines files so you can
correlate them afterward with analyze.py.

Each thread opens its OWN pyodbc connection -- never share a connection/cursor
across threads.
"""

import argparse
import json
import threading
import time

import pyodbc

DEFAULT_PRIMARY_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,14331;"
    "DATABASE=testdb;UID=sa;PWD=YourStrong!Passw0rd;TrustServerCertificate=yes"
)
DEFAULT_SECONDARY_CONN = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,14332;"
    "DATABASE=testdb;UID=sa;PWD=YourStrong!Passw0rd;TrustServerCertificate=yes"
)

stop_event = threading.Event()


def writer_loop(conn_str, n_rows, out_path, delay_s):
    conn = pyodbc.connect(conn_str, autocommit=False)
    cur = conn.cursor()
    with open(out_path, "w") as f:
        for i in range(n_rows):
            cur.execute(
                "INSERT INTO dbo.orders(payload) OUTPUT INSERTED.id VALUES (?)",
                f"row{i}",
            )
            row_id = cur.fetchone()[0]
            conn.commit()
            f.write(json.dumps({"t": time.monotonic(), "row_id": row_id}) + "\n")
            f.flush()
            if delay_s:
                time.sleep(delay_s)
    conn.close()
    print(f"[writer] finished inserting {n_rows} rows -> {out_path}")


def observer_loop(conn_str, out_path):
    conn = pyodbc.connect(conn_str, autocommit=False)
    cur = conn.cursor()
    count = 0
    with open(out_path, "w") as f:
        while not stop_event.is_set():
            cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            cur.execute("BEGIN TRANSACTION")
            cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
            max_lsn_raw = cur.fetchval()
            max_lsn = max_lsn_raw.hex() if max_lsn_raw is not None else None
            cur.execute("SELECT MAX(id) FROM dbo.orders")
            max_id = cur.fetchval()
            cur.execute("COMMIT")
            f.write(
                json.dumps(
                    {"t": time.monotonic(), "max_id_snapshot": max_id, "max_lsn": max_lsn}
                )
                + "\n"
            )
            count += 1
    conn.close()
    print(f"[observer] recorded {count} observations -> {out_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=2000, help="number of rows to insert")
    ap.add_argument(
        "--delay", type=float, default=0.0, help="seconds to sleep between inserts (0 = as fast as possible)"
    )
    ap.add_argument("--primary-conn", default=DEFAULT_PRIMARY_CONN)
    ap.add_argument("--secondary-conn", default=DEFAULT_SECONDARY_CONN)
    ap.add_argument("--out-writer", default="writer_log.jsonl")
    ap.add_argument("--out-observer", default="observer_log.jsonl")
    ap.add_argument(
        "--drain-seconds",
        type=float,
        default=3.0,
        help="how long to keep observing after the writer finishes, to catch trailing redo",
    )
    args = ap.parse_args()

    observer_thread = threading.Thread(
        target=observer_loop, args=(args.secondary_conn, args.out_observer)
    )
    writer_thread = threading.Thread(
        target=writer_loop,
        args=(args.primary_conn, args.rows, args.out_writer, args.delay),
    )

    observer_thread.start()
    writer_thread.start()

    writer_thread.join()
    time.sleep(args.drain_seconds)
    stop_event.set()
    observer_thread.join()

    print("Done. Now run analyze.py against the two log files plus ground-truth LSNs.")


if __name__ == "__main__":
    main()
