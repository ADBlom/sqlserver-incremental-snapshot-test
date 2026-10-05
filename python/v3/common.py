"""Shared connection helpers for the v2 harness and analyzer."""
import os

import pyodbc

from analysis_core import lsn_numeric_to_hex

SA_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")


def add_conn_args(ap):
    ap.add_argument("--host", default="127.0.0.1", help="use 127.0.0.1, not localhost (IPv6 issues on macOS)")
    ap.add_argument("--primary-port", type=int, default=14331)
    ap.add_argument("--secondary-port", type=int, default=14332)
    ap.add_argument("--database", default="testdb")
    ap.add_argument("--encrypt", default="yes", choices=["yes", "no"],
                    help="set to 'no' if TLS handshakes time out under emulation")
    ap.add_argument("--login-timeout", type=int, default=60)


def conn_str(args, port, database=None):
    return (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={args.host},{port};DATABASE={database or args.database};"
        f"UID=sa;PWD={SA_PASSWORD};Encrypt={args.encrypt};TrustServerCertificate=yes"
    )


def connect(args, port, database=None, autocommit=True):
    """autocommit=True: transactions are managed with explicit BEGIN/COMMIT statements."""
    return pyodbc.connect(conn_str(args, port, database), autocommit=autocommit,
                          timeout=args.login_timeout)


def fetch_lsn(cur):
    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    v = cur.fetchone()[0]
    return v.hex() if v is not None else None


POS_SQL = (
    "SELECT sys.fn_cdc_get_max_lsn(), drs.last_commit_lsn, drs.last_redone_lsn "
    "FROM sys.dm_hadr_database_replica_states AS drs "
    "WHERE drs.database_id = DB_ID() AND drs.is_local = 1"
)


def fetch_pos(cur):
    """One statement, three positions, all as binary(10)-style hex:
       l  = CDC max LSN (fn_cdc_get_max_lsn)
       dc = DMV last_commit_lsn  (on a secondary: last commit record redone)
       dr = DMV last_redone_lsn  (last log record redone)"""
    cur.execute(POS_SQL)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("no local row in sys.dm_hadr_database_replica_states for this database")
    l = row[0].hex() if row[0] is not None else None
    return {"l": l, "dc": lsn_numeric_to_hex(row[1]), "dr": lsn_numeric_to_hex(row[2])}
