"""Shared connection helpers for the v2 harness and analyzer."""
import os

import pyodbc

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
