#!/usr/bin/env python3
"""
v2 harness.

  python harness.py setup            # (re)create dbo.hot, enable CDC on it, seed rows
  python harness.py run --duration 300 --chaos suspend_resume,capture_retune

Workload (primary): N writer threads doing UPDATEs / delete+reinsert on a small set of
hot rows. Every row carries a per-row `version` that only ever increases (the increment
happens under the row lock, so version order == commit order for that row). "Straggler"
threads hold a transaction open for seconds so commit order != begin order.

Observers (secondary), three flavours, each with its own connection:

  pre        L_low read in autocommit BEFORE the snapshot transaction begins.
             Safe by construction; a failure here means CDC rows can become visible
             on the secondary before the base-table changes they describe.
  in_first   BEGIN; L_low = fn_cdc_get_max_lsn(); read table.   (the realistic pattern)
  snap_first BEGIN; touch the table (fixes the snapshot); jitter; L_low = fn_cdc_get_max_lsn();
             read table.  Adversarial: fails if fn_cdc_get_max_lsn() reports the live
             redo position instead of the transaction's snapshot position.

Every observation also records L_in (second read inside the txn), L_high (read after
COMMIT) and a COUNT/SUM probe to detect statement-level (non-transaction-level) snapshots.

Chaos (optional): suspend_resume, capture_restart, capture_retune, docker_pause,
docker_restart_secondary.

All results go to runs/<timestamp>/ as JSON-lines; analyze.py does the checking.
"""

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import traceback
from collections import defaultdict
from pathlib import Path

import pyodbc

from common import add_conn_args, connect, fetch_lsn

OBSERVER_MODES = ("pre", "in_first", "snap_first")
CHAOS_EVENTS = ("suspend_resume", "capture_restart", "capture_retune",
                "docker_pause", "docker_restart_secondary")
STATS = defaultdict(int)  # each key is only written by its owning thread


def jl(f, obj):
    f.write(json.dumps(obj, separators=(",", ":")) + "\n")


# ----------------------------------------------------------------------------- setup

def cmd_setup(args):
    total = args.rows + args.stragglers
    conn = connect(args, args.primary_port)
    cur = conn.cursor()

    cur.execute("SELECT OBJECT_ID('dbo.hot')")
    if cur.fetchone()[0] is not None:
        cur.execute("SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance = N'dbo_hot'")
        if cur.fetchone()[0]:
            cur.execute("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', "
                        "@source_name = N'hot', @capture_instance = N'dbo_hot'")
        cur.execute("DROP TABLE dbo.hot")

    cur.execute("CREATE TABLE dbo.hot (id INT NOT NULL PRIMARY KEY, "
                "version BIGINT NOT NULL, pad VARCHAR(4000) NOT NULL)")
    # Only id+version are captured: keeps CT small; pad still inflates the transaction log.
    cur.execute("EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'hot', "
                "@capture_instance = N'dbo_hot', @role_name = NULL, "
                "@captured_column_list = N'id, version', @supports_net_changes = 0")
    cur.execute("EXEC sys.sp_cdc_change_job @job_type = N'capture', "
                "@maxtrans = 500, @maxscans = 10, @pollinginterval = 5")

    cur.execute("BEGIN TRANSACTION")
    cur.executemany("INSERT INTO dbo.hot(id, version, pad) VALUES (?, 0, REPLICATE('x', ?))",
                    [(i, args.pad_bytes) for i in range(1, total + 1)])
    cur.execute("COMMIT")
    print(f"Primary: dbo.hot created with {total} rows and CDC enabled (capture instance dbo_hot).")

    print("Waiting for the secondary to redo the table...")
    sconn = connect(args, args.secondary_port)
    scur = sconn.cursor()
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            scur.execute("SELECT COUNT(*) FROM dbo.hot")
            n = scur.fetchone()[0]
            if n == total:
                print(f"Secondary sees {n} rows. Setup complete.")
                return 0
        except pyodbc.Error:
            pass
        time.sleep(2)
    print("Timed out waiting for the secondary. Check AG health (sql/health_checks.sql).")
    return 1


# ----------------------------------------------------------------------------- writers

def run_txn(cur, ids, kind, pad_bytes):
    """One transaction; returns [[id, new_version], ...] for rows it committed."""
    cur.execute("BEGIN TRANSACTION")
    out = []
    try:
        for i in ids:
            if kind == "reinsert":
                cur.execute("DELETE FROM dbo.hot OUTPUT DELETED.version WHERE id = ?", i)
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"row {i} missing")
                new = row[0] + 1
                cur.execute("INSERT INTO dbo.hot(id, version, pad) "
                            "VALUES (?, ?, REPLICATE(CHAR(65 + CAST(? % 26 AS INT)), ?))",
                            i, new, new, pad_bytes)
            else:
                cur.execute("UPDATE dbo.hot SET version = version + 1, "
                            "pad = REPLICATE(CHAR(65 + CAST((version + 1) % 26 AS INT)), ?) "
                            "OUTPUT INSERTED.version WHERE id = ?", pad_bytes, i)
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"row {i} missing")
                new = row[0]
            out.append([i, new])
        cur.execute("COMMIT")
    except Exception:
        try:
            cur.execute("IF @@TRANCOUNT > 0 ROLLBACK")
        except Exception:
            pass
        raise
    return out


def writer_thread(idx, args, stop, outdir):
    key = f"writer_{idx}"
    rng = random.Random(args.seed * 1000 + idx)
    conn = connect(args, args.primary_port)
    cur = conn.cursor()
    with open(outdir / f"{key}.jsonl", "w") as f:
        while not stop.is_set():
            r = rng.random()
            if r < args.big_txn_prob:
                kind = "big"
                ids = sorted(rng.sample(range(1, args.rows + 1), min(args.big_txn_size, args.rows)))
            elif r < args.big_txn_prob + args.reinsert_prob:
                kind = "reinsert"
                ids = [rng.randint(1, args.rows)]
            else:
                kind = "multi"
                ids = sorted(rng.sample(range(1, args.rows + 1),
                                        rng.randint(1, min(args.max_rows_per_txn, args.rows))))
            try:
                committed = run_txn(cur, ids, kind, args.pad_bytes)
            except pyodbc.Error as e:
                STATS[key + "_errors"] += 1
                jl(f, {"t": time.monotonic(), "err": str(e)[:200]})
                time.sleep(0.2)
                continue
            jl(f, {"t": time.monotonic(), "w": idx, "k": kind, "c": committed})
            STATS[key] += 1
            if args.write_delay:
                time.sleep(args.write_delay)
    conn.close()


def straggler_thread(idx, row_id, args, stop, outdir):
    """Holds a transaction open (row lock on its own private row) so that commit order
    differs from begin order relative to the normal writers."""
    key = f"straggler_{idx}"
    rng = random.Random(args.seed * 1000 + 500 + idx)
    conn = connect(args, args.primary_port)
    cur = conn.cursor()
    upd = ("UPDATE dbo.hot SET version = version + 1, "
           "pad = REPLICATE(CHAR(65 + CAST((version + 1) % 26 AS INT)), ?) "
           "OUTPUT INSERTED.version WHERE id = ?")
    with open(outdir / f"{key}.jsonl", "w") as f:
        while not stop.is_set():
            try:
                cur.execute("BEGIN TRANSACTION")
                cur.execute(upd, args.pad_bytes, row_id)
                cur.fetchone()
                stop.wait(rng.uniform(0.5, 1.0) * args.straggler_hold)
                cur.execute(upd, args.pad_bytes, row_id)
                new = cur.fetchone()[0]
                cur.execute("COMMIT")
            except pyodbc.Error as e:
                try:
                    cur.execute("IF @@TRANCOUNT > 0 ROLLBACK")
                except Exception:
                    pass
                jl(f, {"t": time.monotonic(), "err": str(e)[:200]})
                time.sleep(0.5)
                continue
            jl(f, {"t": time.monotonic(), "w": f"s{idx}", "k": "straggler", "c": [[row_id, new]]})
            STATS[key] += 1
            stop.wait(rng.uniform(0.2, 1.0))
    conn.close()


# ----------------------------------------------------------------------------- observers

def observe_once(cur, mode, n_ids, rng, jitter):
    t = time.monotonic()
    if mode == "pre":
        l_low = fetch_lsn(cur)              # autocommit statement, before the txn exists
        cur.execute("BEGIN TRANSACTION")
    else:
        cur.execute("BEGIN TRANSACTION")
        if mode == "snap_first":
            cur.execute("SELECT TOP (1) id FROM dbo.hot")   # fixes the snapshot now
            cur.fetchall()
            time.sleep(rng.uniform(0, jitter))               # let redo move on
        l_low = fetch_lsn(cur)
    try:
        cur.execute("SELECT id, version FROM dbo.hot")
        rows = cur.fetchall()
        l_in = fetch_lsn(cur)
        cur.execute("SELECT COUNT(*), COALESCE(SUM(version), 0) FROM dbo.hot")
        cnt, sm = cur.fetchone()
    finally:
        cur.execute("IF @@TRANCOUNT > 0 COMMIT")
    l_high = fetch_lsn(cur)

    v = [-1] * n_ids
    for i, ver in rows:
        if 1 <= i <= n_ids:
            v[i - 1] = ver
    return {"t": t, "mode": mode, "l_low": l_low, "l_in": l_in, "l_high": l_high,
            "cnt": cnt, "sum": sm, "v": v}


def observer_thread(idx, mode, n_ids, args, stop, outdir):
    key = f"observer_{mode}_{idx}"
    rng = random.Random(args.seed * 1000 + 100 + idx)
    conn = None
    cur = None
    with open(outdir / f"{key}.jsonl", "w") as f:
        while not stop.is_set():
            try:
                if conn is None:
                    conn = connect(args, args.secondary_port)
                    cur = conn.cursor()
                    cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
                jl(f, observe_once(cur, mode, n_ids, rng, args.snap_jitter))
                STATS[key] += 1
            except pyodbc.Error as e:
                STATS[key + "_errors"] += 1
                jl(f, {"t": time.monotonic(), "mode": mode, "err": str(e)[:300]})
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
                stop.wait(1.0)
    if conn is not None:
        conn.close()


# ----------------------------------------------------------------------------- chaos

def restart_capture(cur):
    try:
        cur.execute("EXEC sys.sp_cdc_stop_job @job_type = N'capture'")
    except pyodbc.Error:
        pass
    time.sleep(1.0)
    for _ in range(10):
        try:
            cur.execute("EXEC sys.sp_cdc_start_job @job_type = N'capture'")
            return True
        except pyodbc.Error:
            time.sleep(1.0)
    return False


def chaos_suspend_resume(args, rng):
    dur = rng.uniform(2, args.max_suspend)
    c = connect(args, args.secondary_port, "master")
    cur = c.cursor()
    cur.execute(f"ALTER DATABASE [{args.database}] SET HADR SUSPEND")
    try:
        time.sleep(dur)
    finally:
        cur.execute(f"ALTER DATABASE [{args.database}] SET HADR RESUME")
        c.close()
    return {"duration": dur}


def chaos_capture_restart(args, rng):
    c = connect(args, args.primary_port)
    cur = c.cursor()
    down = rng.uniform(1, args.max_capture_down)
    try:
        cur.execute("EXEC sys.sp_cdc_stop_job @job_type = N'capture'")
    except pyodbc.Error:
        pass
    time.sleep(down)
    ok = restart_capture(cur)
    c.close()
    return {"down": down, "restarted": ok}


def chaos_capture_retune(args, rng):
    maxtrans = rng.choice([1, 5, 50, 500])
    maxscans = rng.choice([1, 3, 10])
    poll = rng.choice([0, 1, 5])
    c = connect(args, args.primary_port)
    cur = c.cursor()
    cur.execute(f"EXEC sys.sp_cdc_change_job @job_type = N'capture', @maxtrans = {maxtrans}, "
                f"@maxscans = {maxscans}, @pollinginterval = {poll}")
    ok = restart_capture(cur)
    c.close()
    return {"maxtrans": maxtrans, "maxscans": maxscans, "pollinginterval": poll, "restarted": ok}


def chaos_docker_pause(args, rng):
    dur = rng.uniform(1, min(5.0, args.max_suspend))  # keep below the 10 s AG session timeout
    subprocess.run(["docker", "pause", args.secondary_container], check=True, capture_output=True)
    try:
        time.sleep(dur)
    finally:
        subprocess.run(["docker", "unpause", args.secondary_container], capture_output=True)
    return {"duration": dur}


def chaos_docker_restart_secondary(args, rng):
    subprocess.run(["docker", "restart", args.secondary_container], check=True, capture_output=True)
    return {}


CHAOS = {
    "suspend_resume": chaos_suspend_resume,
    "capture_restart": chaos_capture_restart,
    "capture_retune": chaos_capture_retune,
    "docker_pause": chaos_docker_pause,
    "docker_restart_secondary": chaos_docker_restart_secondary,
}


def chaos_thread(events, args, stop, outdir):
    rng = random.Random(args.seed + 777)
    with open(outdir / "chaos.jsonl", "w") as f:
        while not stop.wait(rng.uniform(0.5, 1.5) * args.chaos_interval):
            name = rng.choice(events)
            t0 = time.monotonic()
            try:
                detail = CHAOS[name](args, rng) or {}
            except Exception as e:  # keep the run going; record what failed
                detail = {"error": str(e)[:300]}
            jl(f, {"t0": t0, "t1": time.monotonic(), "event": name, **detail})
            f.flush()
            STATS["chaos_events"] += 1


def cleanup(args, events):
    """Undo anything chaos may have left behind."""
    try:
        c = connect(args, args.secondary_port, "master")
        c.cursor().execute(f"ALTER DATABASE [{args.database}] SET HADR RESUME")
        c.close()
    except Exception as e:
        print(f"[cleanup] HADR RESUME: {str(e)[:120]}")
    if "docker_pause" in events:
        subprocess.run(["docker", "unpause", args.secondary_container], capture_output=True)
    try:
        c = connect(args, args.primary_port)
        cur = c.cursor()
        cur.execute("EXEC sys.sp_cdc_change_job @job_type = N'capture', "
                    "@maxtrans = 500, @maxscans = 10, @pollinginterval = 5")
        restart_capture(cur)
        c.close()
    except Exception as e:
        print(f"[cleanup] capture job reset: {str(e)[:120]}")


# ----------------------------------------------------------------------------- run

def preflight(args):
    for label, port, want in (("primary", args.primary_port, "READ_WRITE"),
                              ("secondary", args.secondary_port, "READ_ONLY")):
        c = connect(args, port)
        cur = c.cursor()
        cur.execute("SELECT CAST(DATABASEPROPERTYEX(?, 'Updateability') AS VARCHAR(20))", args.database)
        got = cur.fetchone()[0]
        if got != want:
            raise SystemExit(f"{label} on port {port} reports Updateability={got}, expected {want}. "
                             "Are the ports swapped, or is the AG unhealthy?")
        cur.execute("SELECT COUNT(*) FROM dbo.hot")
        print(f"[preflight] {label}: {got}, dbo.hot rows = {cur.fetchone()[0]}")
        if label == "secondary":
            cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            cur.execute("BEGIN TRANSACTION")
            cur.execute("SELECT COUNT(*) FROM dbo.hot")
            cur.fetchone()
            print(f"[preflight] secondary max LSN: {fetch_lsn(cur)}")
            cur.execute("COMMIT")
        c.close()


def cmd_run(args):
    events = [e for e in args.chaos.split(",") if e]
    for e in events:
        if e not in CHAOS:
            raise SystemExit(f"unknown chaos event {e!r}; choose from {CHAOS_EVENTS}")
    modes = [m for m in args.observers.split(",") if m]
    for m in modes:
        if m not in OBSERVER_MODES:
            raise SystemExit(f"unknown observer mode {m!r}; choose from {OBSERVER_MODES}")

    preflight(args)

    outdir = Path(args.out) / time.strftime("%Y%m%d-%H%M%S")
    outdir.mkdir(parents=True)
    n_ids = args.rows + args.stragglers
    meta = {"args": vars(args), "n_ids": n_ids, "start_wall": time.time(),
            "start_mono": time.monotonic()}
    (outdir / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"[run] output directory: {outdir}")

    stop_writers, stop_chaos, stop_obs = threading.Event(), threading.Event(), threading.Event()
    threads_w, threads_o, threads_c = [], [], []

    def guarded(fn, *a):
        try:
            fn(*a)
        except Exception:
            traceback.print_exc()
            print("[run] a worker thread died; results from this run are incomplete", file=sys.stderr)

    for m in modes:
        for k in range(args.observer_copies):
            threads_o.append(threading.Thread(
                target=guarded, args=(observer_thread, k, m, n_ids, args, stop_obs, outdir)))
    for k in range(args.writers):
        threads_w.append(threading.Thread(target=guarded, args=(writer_thread, k, args, stop_writers, outdir)))
    for k in range(args.stragglers):
        threads_w.append(threading.Thread(
            target=guarded, args=(straggler_thread, k, args.rows + 1 + k, args, stop_writers, outdir)))
    if events:
        threads_c.append(threading.Thread(target=guarded, args=(chaos_thread, events, args, stop_chaos, outdir)))

    for t in threads_o + threads_c + threads_w:
        t.start()

    t_end = time.monotonic() + args.duration
    try:
        while time.monotonic() < t_end:
            time.sleep(min(10, max(0.0, t_end - time.monotonic())))
            commits = sum(v for k, v in STATS.items() if k.startswith(("writer_", "straggler_")) and not k.endswith("_errors"))
            obs = sum(v for k, v in STATS.items() if k.startswith("observer_") and not k.endswith("_errors"))
            errs = sum(v for k, v in STATS.items() if k.endswith("_errors"))
            print(f"[run] {int(args.duration - (t_end - time.monotonic()))}s  commits={commits} "
                  f"observations={obs} errors={errs} chaos_events={STATS['chaos_events']}")
    except KeyboardInterrupt:
        print("[run] interrupted; shutting down cleanly")

    stop_writers.set()
    stop_chaos.set()
    for t in threads_w + threads_c:
        t.join()
    cleanup(args, events)
    print(f"[run] writers stopped; observing the secondary catch up for {args.drain}s")
    time.sleep(args.drain)
    stop_obs.set()
    for t in threads_o:
        t.join()

    meta["end_mono"] = time.monotonic()
    meta["stats"] = dict(STATS)
    (outdir / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"[run] done. Analyze with:  python analyze.py {outdir}")
    return 0


# ----------------------------------------------------------------------------- CLI

def main():
    common = argparse.ArgumentParser(add_help=False)
    add_conn_args(common)
    common.add_argument("--rows", type=int, default=200, help="hot rows used by normal writers")
    common.add_argument("--stragglers", type=int, default=2,
                        help="extra rows, each owned by a long-transaction thread")
    common.add_argument("--pad-bytes", type=int, default=1000,
                        help="width of the non-captured pad column; inflates log volume")

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", parents=[common], help="(re)create dbo.hot, enable CDC, seed rows")

    r = sub.add_parser("run", parents=[common], help="run the workload + observers (+ chaos)")
    r.add_argument("--duration", type=float, default=60, help="seconds of write load")
    r.add_argument("--drain", type=float, default=5, help="seconds to keep observing after writers stop")
    r.add_argument("--writers", type=int, default=4)
    r.add_argument("--max-rows-per-txn", type=int, default=3)
    r.add_argument("--big-txn-prob", type=float, default=0.02)
    r.add_argument("--big-txn-size", type=int, default=50)
    r.add_argument("--reinsert-prob", type=float, default=0.10, help="delete+reinsert in one txn")
    r.add_argument("--write-delay", type=float, default=0.0, help="sleep between txns per writer")
    r.add_argument("--straggler-hold", type=float, default=3.0, help="seconds a straggler txn stays open")
    r.add_argument("--observers", default=",".join(OBSERVER_MODES))
    r.add_argument("--observer-copies", type=int, default=1)
    r.add_argument("--snap-jitter", type=float, default=0.02, help="max sleep (s) in snap_first mode")
    r.add_argument("--chaos", default="", help=f"comma list from: {','.join(CHAOS_EVENTS)}")
    r.add_argument("--chaos-interval", type=float, default=15.0, help="mean seconds between chaos events")
    r.add_argument("--max-suspend", type=float, default=8.0)
    r.add_argument("--max-capture-down", type=float, default=8.0)
    r.add_argument("--secondary-container", default="mssql-node2")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--out", default="runs")

    args = ap.parse_args()
    sys.exit(cmd_setup(args) if args.cmd == "setup" else cmd_run(args))


if __name__ == "__main__":
    main()
