#!/usr/bin/env python3
"""
v5: which read-only LSN marks can bracket a chunk read (low before, high after)?

  python exp.py setup
  python exp.py run --duration 120          # writers on primary; chunk readers on primary AND secondary
  python exp.py analyze runs/v5/<dir>

Every chunk read records candidate marks before (low) and after (high) the read. Ground truth
(commit LSN of every row version) comes from the CDC change table after the run.

For a chunk R and a candidate mark pair (L, H) per key k:
  LOW violation : chunk older than a change committed at LSN <= L         (permanent regression)
  HIGH violation: chunk newer than the state as of H, i.e. it contains a  (transient regression /
                  commit with LSN > H                                       duplicate later)
Reachability: how long until the CDC stream position (fn_cdc_get_max_lsn on the same node) >= H.
Debezium can only close the window once the stream reaches H.
"""
import argparse
import json
import os
import random
import sys
import threading
import time
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path

import pyodbc

PW = os.environ.get("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")


MIN_USER = None   # set by --min-user: readers run as this low-privilege login
MIN_PW = os.environ.get("MIN_PASSWORD", "Ro!Passw0rd12")


def connect(port, db="testdb", host="127.0.0.1", minimal=False):
    uid, pw = ("sa", PW) if not (minimal and MIN_USER) else (MIN_USER, MIN_PW)
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};DATABASE={db};UID={uid};PWD={pw};Encrypt=yes;TrustServerCertificate=yes",
        autocommit=True, timeout=60)


def num_to_hex(n):
    """numeric(25,0) LSN (DMVs) -> 20-char hex (binary(10) form)."""
    if n is None:
        return None
    n = int(n)
    seq, rest = divmod(n, 10**15)
    off, slot = divmod(rest, 10**5)
    return f"{seq:08x}{off:08x}{slot:04x}"


def str_to_hex(s):
    """'00000070:0000D1D0:0001' (hex parts, dm_db_log_stats / fn_dblog) -> 20-char hex."""
    if s is None:
        return None
    a, b, c = s.strip("\x00").split(":")
    return (a + b + c).lower()


# ----------------------------------------------------------------------------- marks

PRIMARY_MARKS = """
SET NOCOUNT ON;
SELECT sys.fn_cdc_get_max_lsn(), (SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID())),
  (SELECT last_commit_lsn FROM sys.dm_hadr_database_replica_states WHERE database_id = DB_ID() AND is_local = 1),
  (SELECT end_of_log_lsn FROM sys.dm_hadr_database_replica_states WHERE database_id = DB_ID() AND is_local = 1),
  (SELECT MIN(database_transaction_begin_lsn) FROM sys.dm_tran_database_transactions
     WHERE database_id = DB_ID() AND database_transaction_begin_lsn IS NOT NULL),
  (SELECT MAX(database_transaction_last_lsn) FROM sys.dm_tran_database_transactions
     WHERE database_id = DB_ID() AND database_transaction_last_lsn IS NOT NULL);
"""
PRIMARY_NAMES = ["cdc_max", "log_end", "hadr_commit", "hadr_eol", "tran_min_begin", "tran_max_last"]

TRACKED_SQL = """
SET NOCOUNT ON;
DECLARE @s varchar(40) = ?;
SELECT MAX(c.[Current LSN]), COUNT(DISTINCT c.[Transaction ID])
FROM fn_dblog(@s, NULL) AS c
WHERE c.Operation = 'LOP_COMMIT_XACT' AND c.[Transaction ID] IN (
  SELECT [Transaction ID] FROM fn_dblog(@s, NULL)
  WHERE Operation IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS','LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS')
    AND Context IN ('LCX_CLUSTERED','LCX_HEAP') AND AllocUnitName LIKE 'dbo.wm.%');
"""
EXACT_SQL = """
SET NOCOUNT ON;
DECLARE @s varchar(40) = ?;
SELECT [Transaction ID],
  MAX(CASE WHEN Operation = 'LOP_BEGIN_XACT' THEN [Transaction Name] END),
  MAX(CASE WHEN Operation = 'LOP_COMMIT_XACT' THEN [Current LSN] END),
  MAX(CASE WHEN Operation IN ('LOP_INSERT_ROWS','LOP_DELETE_ROWS','LOP_MODIFY_ROW','LOP_MODIFY_COLUMNS')
            AND Context IN ('LCX_CLUSTERED','LCX_HEAP') AND AllocUnitName LIKE 'dbo.wm.%' THEN 1 ELSE 0 END),
  MIN(CASE WHEN Operation <> 'LOP_BEGIN_XACT' THEN [Previous LSN] END)
FROM fn_dblog(@s, NULL) WHERE [Transaction ID] <> '0000:00000000' GROUP BY [Transaction ID];
"""
USER_TX = {"user_transaction", "INSERT", "UPDATE", "DELETE", "MERGE", "implicit_transaction"}


def exact_tracked_mark(cur, start_hex, max_rounds=6):
    """Max commit LSN of user transactions that modified dbo.wm after start_hex. Transactions that
    began before the scan start (committed in range, no BEGIN seen) are chased back via [Previous LSN]
    until their BEGIN is found. Returns (lsn_or_None, rounds)."""
    start = start_hex
    for rnd in range(1, max_rounds + 1):
        try:
            cur.execute("EXEC dbo.dbz_tracked_commits ?" if MIN_USER else EXACT_SQL, hex_to_dblog(start))
            rows = cur.fetchall()
        except pyodbc.Error as e:
            return "ERR", str(e)[:80]
        best, back_to = None, None
        for _tx, name, clsn, touches, prev in rows:
            if clsn is None or str_to_hex(clsn) <= start_hex:
                continue       # at/below the stream position: already delivered
            if name is None:                       # began before the scan window
                p = str_to_hex(prev)
                if p and p != "0" * 20 and (back_to is None or p < back_to):
                    back_to = p
                continue
            if touches and name in USER_TX:
                c = str_to_hex(clsn)
                best = c if best is None or c > best else best
        if back_to is None or back_to >= start:
            return best, rnd
        start = back_to
    return "ERR", "no convergence"


SCAN_TOKEN = ("SELECT session_id, end_time, empty_scan_count FROM sys.dm_cdc_log_scan_sessions "
              "WHERE session_id = (SELECT MAX(session_id) FROM sys.dm_cdc_log_scan_sessions)")


def hex_to_dblog(h):
    return f"0x{h[:8]}:{h[8:16]}:{h[16:]}"


def back(h, nbytes):
    """LSN nbytes earlier inside the same VLF (clamped), for lookback variants."""
    off = int(h[8:16], 16)
    if off - nbytes < 0x2000:
        return h  # near a VLF start: stay on the known-valid record boundary
    return h[:8] + f"{off - nbytes:08x}" + "0001"


def tracked_mark(cur, start_hex):
    """Max commit LSN of a transaction that modified dbo.wm, from the log at/after start_hex."""
    try:
        cur.execute(TRACKED_SQL, hex_to_dblog(start_hex))
        r = cur.fetchone()
        return str_to_hex(r[0]) if r[0] else None, r[1]
    except pyodbc.Error as e:
        return None, str(e)[:80]


SECONDARY_MARKS = """
SET NOCOUNT ON;
SELECT sys.fn_cdc_get_max_lsn(), drs.last_commit_lsn, drs.last_redone_lsn, drs.last_hardened_lsn,
       drs.end_of_log_lsn
FROM sys.dm_hadr_database_replica_states AS drs WHERE drs.database_id = DB_ID() AND drs.is_local = 1;
"""
SECONDARY_NAMES = ["cdc_max", "dmv_commit", "dmv_redone", "dmv_hardened", "dmv_eol"]


def get_marks(cur, side):
    cur.execute(PRIMARY_MARKS if side == "primary" else SECONDARY_MARKS)
    r = cur.fetchone()
    names = PRIMARY_NAMES if side == "primary" else SECONDARY_NAMES
    out = {}
    for n, v in zip(names, r):
        if v is None:
            out[n] = None
        elif isinstance(v, (bytes, bytearray)):
            out[n] = bytes(v).hex()
        elif isinstance(v, str):
            out[n] = str_to_hex(v)
        else:
            out[n] = num_to_hex(v)
    return out


# ----------------------------------------------------------------------------- setup

def cmd_setup(a):
    c = connect(a.primary_port).cursor()
    c.execute("SELECT OBJECT_ID('dbo.wm')")
    if c.fetchone()[0] is not None:
        c.execute("EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'wm', @capture_instance=N'dbo_wm'")
        c.execute("DROP TABLE dbo.wm")
    c.execute("IF OBJECT_ID('dbo.noise') IS NULL CREATE TABLE dbo.noise (id BIGINT IDENTITY PRIMARY KEY, x INT NOT NULL)")
    c.execute("CREATE TABLE dbo.wm (id INT NOT NULL PRIMARY KEY, version BIGINT NOT NULL, pad VARCHAR(400) NOT NULL)")
    c.execute("EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'wm', @capture_instance=N'dbo_wm', "
              "@role_name=NULL, @captured_column_list=N'id, version', @supports_net_changes=0")
    c.execute("EXEC sys.sp_cdc_change_job @job_type=N'capture', @maxtrans=500, @maxscans=10, @pollinginterval=1")
    c.execute("BEGIN TRAN")
    total = a.hot + a.pool
    c.executemany("INSERT INTO dbo.wm VALUES (?, 0, REPLICATE('x', 300))", [(i,) for i in range(1, total + 1)])
    c.execute("COMMIT")
    print(f"seeded {total} rows (hot 1..{a.hot}, delete-pool {a.hot+1}..{total}); new keys start at {total+1000}")
    s = connect(a.secondary_port).cursor()
    for _ in range(90):
        try:
            s.execute("SELECT COUNT(*) FROM dbo.wm")
            if s.fetchone()[0] == total:
                print("secondary has the table"); return 0
        except pyodbc.Error:
            pass
        time.sleep(2)
    return 1


# ----------------------------------------------------------------------------- workload

class Shared:
    phase = "load"          # load | idle | noise
    stop = False


def retry(fn):
    for _ in range(5):
        try:
            return fn()
        except pyodbc.Error as e:
            if "deadlock" in str(e).lower() or "1205" in str(e):
                continue
            raise


def w_update(a, kind, seed):
    rnd = random.Random(seed)
    cur = connect(a.primary_port).cursor()
    while not Shared.stop:
        if Shared.phase != "load":
            time.sleep(0.05); continue
        ids = sorted(rnd.sample(range(1, a.hot + 1), rnd.choice([1, 1, 2, 5])))

        def txn():
            cur.execute("BEGIN TRAN")
            try:
                for i in ids:
                    if kind == "reinsert":
                        cur.execute("DELETE FROM dbo.wm OUTPUT DELETED.version WHERE id=?", i)
                        v = cur.fetchone()[0] + 1
                        cur.execute("INSERT INTO dbo.wm VALUES (?, ?, REPLICATE('y', 300))", i, v)
                    else:
                        cur.execute("UPDATE dbo.wm SET version=version+1 WHERE id=?", i)
                cur.execute("COMMIT")
            except Exception:
                cur.execute("IF @@TRANCOUNT>0 ROLLBACK"); raise
        try:
            retry(txn)
        except pyodbc.Error:
            pass


def w_straggler(a, seed):
    rnd = random.Random(seed)
    cur = connect(a.primary_port).cursor()
    while not Shared.stop:
        if Shared.phase != "load":
            time.sleep(0.05); continue
        i, j = sorted(rnd.sample(range(1, a.hot + 1), 2))
        try:
            cur.execute("BEGIN TRAN")
            cur.execute("UPDATE dbo.wm SET version=version+1 WHERE id=?", i)
            time.sleep(rnd.uniform(0.3, a.straggler_max))
            cur.execute("UPDATE dbo.wm SET version=version+1 WHERE id=?", j)
            cur.execute("COMMIT")
        except pyodbc.Error:
            cur.execute("IF @@TRANCOUNT>0 ROLLBACK")


def w_insert_delete(a, seed):
    rnd = random.Random(seed)
    cur = connect(a.primary_port).cursor()
    nxt = a.hot + a.pool + 1000 + seed * 100000
    pool = list(range(a.hot + 1, a.hot + a.pool + 1))
    rnd.shuffle(pool)
    while not Shared.stop:
        if Shared.phase != "load":
            time.sleep(0.05); continue
        try:
            cur.execute("INSERT INTO dbo.wm VALUES (?, 1, REPLICATE('n', 300))", nxt); nxt += 1
            if seed == 0 and pool and rnd.random() < 0.3:
                cur.execute("DELETE FROM dbo.wm WHERE id=?", pool.pop())
        except pyodbc.Error:
            pass
        time.sleep(0.01)


def w_sparse(a, seed):
    rnd = random.Random(seed)
    cur = connect(a.primary_port).cursor()
    while not Shared.stop:
        if Shared.phase != "sparse":
            time.sleep(0.05); continue
        cur.execute("UPDATE dbo.wm SET version=version+1 WHERE id=?", rnd.randint(1, a.hot))
        time.sleep(rnd.uniform(0.1, 3.0))


def w_noise(a):
    cur = connect(a.primary_port).cursor()
    while not Shared.stop:
        if Shared.phase in ("load", "noise"):
            cur.execute("INSERT INTO dbo.noise(x) VALUES (1)")
            time.sleep(0.005)
        else:
            time.sleep(0.05)


# ----------------------------------------------------------------------------- readers

def scan_token(cur):
    cur.execute(SCAN_TOKEN)
    r = cur.fetchone()
    return tuple(r) if r else None


def min_marks(cur):
    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    return {"cdc_max": cur.fetchone()[0].hex()}


SCAN_STATE = ("SELECT TOP 1 session_id, empty_scan_count, tran_count, scan_phase FROM sys.dm_cdc_log_scan_sessions "
              "ORDER BY session_id DESC")


def scan_barrier(pc, timeout=15.0):
    """Wait until the primary's capture job completed an EMPTY scan that started after now.
    Returns cdc_max read on the primary afterwards (all tracked commits visible to a read that
    finished before the call are then in the stream), or None on timeout."""
    pc.execute(SCAN_STATE); s0 = pc.fetchone()
    if s0 is None or s0[1] == 0:
        return None            # last scan was not empty: capture is busy, barrier cannot help
    need = 1 if s0[3] == "Done" else 2     # a scan running at s0 may have started before the read
    deadline = time.time() + timeout
    while time.time() < deadline and not Shared.stop:
        time.sleep(0.1)
        pc.execute(SCAN_STATE); s = pc.fetchone()
        if s is None:
            continue
        if (s[0] == s0[0] and s[1] >= s0[1] + need) or (s[0] > s0[0] and s[1] >= need):
            pc.execute("SELECT sys.fn_cdc_get_max_lsn()")
            return pc.fetchone()[0].hex()
    return None


def reader(a, side, port, outdir, variant=""):
    cur = connect(port, minimal=True).cursor()
    pc = connect(a.primary_port, minimal=True).cursor() if a.scanbar else None
    if side == "primary" and a.iso == "snapshot":
        cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
    name = side + variant
    f = open(outdir / f"chunks_{name}.jsonl", "w")
    rnd = random.Random(name)
    while not Shared.stop:
        phase = Shared.phase
        lo = min_marks(cur) if MIN_USER else get_marks(cur, side)
        t0 = time.time()
        if side == "primary" and a.iso == "snapshot":
            cur.execute("BEGIN TRAN")
        cur.execute("SELECT id, version FROM dbo.wm")
        rows = []
        while True:
            b = cur.fetchmany(a.fetch)
            if not b:
                break
            rows.extend((r[0], r[1]) for r in b)
            if a.jitter:
                time.sleep(rnd.uniform(0, a.jitter))
        if side == "primary" and a.iso == "snapshot":
            cur.execute("COMMIT")
        t1 = time.time()
        hi = min_marks(cur) if MIN_USER else get_marks(cur, side)
        t2 = time.time()
        cost = {}
        cur.execute("SELECT CONVERT(varchar(30), SYSDATETIME(), 126)")
        tT = cur.fetchone()[0]
        if pc is not None:
            ta = time.time()
            hi["scanbar"] = scan_barrier(pc)
            cost["scanbar"] = [time.time() - ta, 0]
        # tracked-commit marks from the log; start positions are derived from LOW-side values only
        c0 = lo["cdc_max"]
        for nm, start in (() if MIN_USER else (("trk_cdc", c0), ("trk_lb256k", back(c0, 0x40000)), ("trk_lb4m", back(c0, 0x400000)))):
            ta = time.time()
            hi[nm], n = tracked_mark(cur, start)
            cost[nm] = [time.time() - ta, n]
        if not a.no_trk:
            ta = time.time()
            hi["trk_exact"], n = exact_tracked_mark(cur, c0)
            cost["trk_exact"] = [time.time() - ta, n]
        if side == "primary" and not MIN_USER:
            mb = lo.get("tran_min_begin")
            start = min(c0, mb) if mb else c0
            ta = time.time()
            hi["trk_active"], n = tracked_mark(cur, start)
            cost["trk_active"] = [time.time() - ta, n]
        if variant == "_barrier":
            tok0 = scan_token(cur)
            changes, last, deadline = 0, tok0, time.time() + 40
            while changes < 2 and time.time() < deadline and not Shared.stop:
                time.sleep(0.05)
                tk = scan_token(cur)
                if tk != last:
                    changes += 1; last = tk
                    hi[f"barrier{changes}"] = get_marks(cur, side)["cdc_max"]
        f.write(json.dumps({"phase": phase, "t0": t0, "t1": t1, "t2": t2, "lo": lo, "hi": hi,
                            "cost": cost, "tT": tT, "rows": rows}, separators=(",", ":")) + "\n")
        time.sleep(a.gap)
    f.close()


def watcher(side, port, outdir):
    cur = connect(port, minimal=True).cursor()
    f = open(outdir / f"watch_{side}.jsonl", "w")
    while not Shared.stop:
        cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
        v = cur.fetchone()[0]
        f.write(json.dumps([time.time(), v.hex() if v else None]) + "\n")
        time.sleep(0.05)
    f.close()


def cmd_run(a):
    outdir = Path(a.out) / time.strftime("%Y%m%d-%H%M%S")
    outdir.mkdir(parents=True)
    json.dump(vars(a), open(outdir / "meta.json", "w"))
    th = []
    def add(fn, *args):
        t = threading.Thread(target=fn, args=args, daemon=True); t.start(); th.append(t)
    for n in range(a.updaters):
        add(w_update, a, "update", 100 + n)
    for n in range(a.reinserters):
        add(w_update, a, "reinsert", 200 + n)
    for n in range(a.stragglers):
        add(w_straggler, a, 300 + n)
    add(w_insert_delete, a, 0)
    add(w_noise, a)
    add(w_sparse, a, 900)
    add(w_sparse, a, 901)
    for side, port in (("primary", a.primary_port), ("secondary", a.secondary_port)):
        add(reader, a, side, port, outdir)
        add(watcher, side, port, outdir)
    if not MIN_USER:
        add(reader, a, "primary", a.primary_port, outdir, "_barrier")
    d = a.duration
    plan = [("load", d * 0.4), ("sparse", d * 0.3), ("idle", d * 0.15), ("noise", d * 0.15)]
    marks = []
    for ph, dur in plan:
        Shared.phase = ph
        print(f"phase {ph} for {dur:.0f}s", flush=True)
        marks.append((ph, time.time()))
        time.sleep(dur)
    Shared.phase = "idle"
    time.sleep(3)
    Shared.stop = True
    for t in th:
        t.join(timeout=30)
    print("run dir", outdir)
    return 0


# ----------------------------------------------------------------------------- analysis

def load_timeline(a):
    cur = connect(a.primary_port).cursor()
    # wait for CDC to drain
    cur.execute("SELECT MAX(log_end_lsn) FROM sys.dm_db_log_stats(DB_ID())")
    cur.execute("SELECT __$start_lsn, __$seqval, __$operation, id, version FROM cdc.dbo_wm_CT ORDER BY __$start_lsn, __$seqval")
    per = defaultdict(lambda: defaultdict(list))
    for lsn, seq, op, i, v in cur.fetchall():
        per[i][lsn.hex()].append((seq, op, v))
    tl = {}
    for i, groups in per.items():
        ev = []
        for lsn in sorted(groups):
            ops = sorted(groups[lsn])
            last_present = [x for x in ops if x[1] in (2, 4)]
            ev.append((lsn, last_present[-1][2] if last_present else None))
        tl[i] = ([e[0] for e in ev], [e[1] for e in ev])
    cur.execute("SELECT start_lsn, tran_end_time FROM cdc.lsn_time_mapping ORDER BY start_lsn")
    global MAPPING
    MAPPING = [(l.hex(), t) for l, t in cur.fetchall() if t.year > 1900]
    return tl


MAPPING = []


def time_high(tT, margin_ms):
    """first stream entry whose commit time is later than tT (+margin): its LSN is a high watermark."""
    from datetime import datetime, timedelta
    T = datetime.fromisoformat(tT) + timedelta(milliseconds=margin_ms)
    for l, t in MAPPING:
        if t > T:
            return l
    return None


def obs_idx(tl_i, ver):
    """position (number of events applied) the observed value corresponds to, or None."""
    if tl_i is None:
        return 0 if ver is None else None
    lsns, vers = tl_i
    if ver is None:
        return len(vers) if vers and vers[-1] is None else 0
    try:
        return vers.index(ver) + 1
    except ValueError:
        return None


def cmd_analyze(a):
    d = Path(a.dir)
    tl = load_timeline(a)
    max_tracked = max(l[-1] for l, _ in tl.values())
    print("timeline keys", len(tl), "max tracked commit lsn", max_tracked)
    res = {}
    for p in sorted(d.glob("chunks_*.jsonl")):
        name = p.stem[len("chunks_"):]
        side = name.split("_")[0]
        watch = [json.loads(x) for x in open(d / f"watch_{side}.jsonl")]
        wt = [w[0] for w in watch]; wl = [w[1] for w in watch]
        chunks = [json.loads(x) for x in open(p)]
        stats = defaultdict(lambda: defaultdict(int))
        delays = defaultdict(lambda: defaultdict(list))
        costs = defaultdict(list)
        for ch in chunks:
            rows = {i: v for i, v in ch["rows"]}
            ph = ch["phase"]
            for k, (sec, n) in ch.get("cost", {}).items():
                costs[k].append(sec)
                if ch["hi"].get(k) is not None:
                    costs[f"{k}[{ph}]"].append(sec)
            hi = dict(ch["hi"])
            for m in a.margins:
                hi[f"time_{m}ms"] = time_high(ch["tT"], m) if "tT" in ch else None
            for cand, H in hi.items():
                L = ch["lo"].get(cand) or ch["lo"]["cdc_max"]
                st = stats[cand]
                if H == "ERR":
                    st["err"] += 1; continue
                if H is None and cand.startswith("trk"):
                    H = L          # no tracked commit after the stream position
                if L is None or H is None:
                    st["null"] += 1; continue
                st["chunks"] += 1
                lowv = highv = 0
                for i, (lsns, vers) in tl.items():
                    o = obs_idx(tl[i], rows.get(i))
                    if o is None:
                        continue
                    if o < bisect_right(lsns, L):
                        lowv += 1
                    if o > bisect_right(lsns, H):
                        highv += 1
                st["low_viol_chunks"] += lowv > 0
                st["high_viol_chunks"] += highv > 0
                st["low_viol_rows"] += lowv
                st["high_viol_rows"] += highv
                k = None
                for j in range(len(wt)):
                    if wt[j] >= ch["t2"] and wl[j] is not None and wl[j] >= H:
                        k = j; break
                delays[cand][ph].append(None if k is None else wt[k] - ch["t2"])
        res[name] = (stats, delays)
        print(f"\n===== {name}: {len(chunks)} chunks =====")
        print(f"{'candidate':15} {'chunks':>6} {'null':>4} {'LOWv':>5} {'HIGHv':>6} | reach delay p50/p95/never(of n) per phase")
        for cand, st in stats.items():
            line = f"{cand:15} {st['chunks']:6d} {st['null']:4d} {st['low_viol_chunks']:5d} {st['high_viol_chunks']:6d} |"
            for ph in ("load", "sparse", "idle", "noise"):
                ds = delays[cand].get(ph, [])
                ok = sorted(x for x in ds if x is not None)
                nev = sum(1 for x in ds if x is None)
                if ds:
                    p50 = ok[len(ok) // 2] if ok else float("nan")
                    p95 = ok[int(len(ok) * .95)] if ok else float("nan")
                    line += f" {ph}:{p50:.2f}/{p95:.2f}/{nev}of{len(ds)}"
            print(line)
        print("  query cost (s, mean):", {k: round(sum(v) / len(v), 3) for k, v in costs.items()})
    json.dump({n: {c: dict(v) for c, v in st.items()} for n, (st, _) in res.items()},
              open(d / "report.json", "w"), indent=1)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["setup", "run", "analyze"])
    ap.add_argument("dir", nargs="?")
    ap.add_argument("--primary-port", type=int, default=14331)
    ap.add_argument("--secondary-port", type=int, default=14332)
    ap.add_argument("--hot", type=int, default=1500)
    ap.add_argument("--pool", type=int, default=300)
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--updaters", type=int, default=4)
    ap.add_argument("--reinserters", type=int, default=2)
    ap.add_argument("--stragglers", type=int, default=2)
    ap.add_argument("--straggler-max", type=float, default=2.0)
    ap.add_argument("--min-user", default=None, help="run readers as this low-privilege login (needs dbo.dbz_tracked_commits)")
    ap.add_argument("--scanbar", action="store_true", help="also evaluate the empty-scan barrier (needs VIEW DATABASE PERFORMANCE STATE on the primary)")
    ap.add_argument("--no-trk", action="store_true", help="skip the fn_dblog based candidate")
    ap.add_argument("--margins", type=lambda x: [int(v) for v in x.split(",")], default=[0, 50], help="analyze: clock-skew margins (ms) for the time-based high")
    ap.add_argument("--iso", default="rc", choices=["rc", "snapshot"])
    ap.add_argument("--fetch", type=int, default=200)
    ap.add_argument("--jitter", type=float, default=0.01)
    ap.add_argument("--gap", type=float, default=0.05)
    ap.add_argument("--out", default="../../runs/v5")
    a = ap.parse_args()
    global MIN_USER
    MIN_USER = a.min_user
    sys.exit({"setup": cmd_setup, "run": cmd_run, "analyze": cmd_analyze}[a.cmd](a))


if __name__ == "__main__":
    main()
