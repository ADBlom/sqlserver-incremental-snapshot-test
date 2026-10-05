#!/usr/bin/env python3
"""
Analyze a v2 run directory.

  python analyze.py runs/20260928-101500        (or `python analyze.py` for the newest run)

1. Waits for the CDC capture job on the primary to catch up with everything the writers
   committed, then loads the ground truth (every insert/update-after image with its
   commit LSN) from cdc.dbo_hot_CT.
2. Completeness check: every committed (row, version) must appear in the CT table.
3. Lower-bound check, per observer mode (THE safety property):
       every change committed at LSN <= L_low must be visible in the snapshot read
       that was taken together with L_low.
4. Coverage / power statistics, and characterization of the upper side (L_high).
5. Correlates any violations with the chaos events that were running.
"""

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from analysis_core import build_index, check_observation, pct
from common import add_conn_args, connect


def load_jsonl(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def wait_for_capture(cur, committed_max, timeout):
    deadline = time.time() + timeout
    while True:
        cur.execute("SELECT id, MAX(version) FROM cdc.dbo_hot_CT "
                    "WHERE __$operation IN (2, 4) GROUP BY id")
        ct_max = {r[0]: r[1] for r in cur.fetchall()}
        behind = {i: (m, ct_max.get(i, -1)) for i, m in committed_max.items() if ct_max.get(i, -1) < m}
        if not behind:
            return True, {}
        if time.time() > deadline:
            return False, behind
        print(f"  capture job still behind on {len(behind)} rows; waiting...")
        time.sleep(3)


def fetch_ct(cur):
    cur.execute("SELECT id, version, [__$start_lsn] FROM cdc.dbo_hot_CT WHERE [__$operation] IN (2, 4)")
    rows = []
    while True:
        batch = cur.fetchmany(50000)
        if not batch:
            break
        rows.extend((r[0], r[1], r[2].hex()) for r in batch)
    return rows


def chaos_near(events, t, before=2.0, after=6.0):
    return [e for e in events if e["t0"] - before <= t <= e["t1"] + after]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir", nargs="?")
    ap.add_argument("--wait", type=int, default=180, help="max seconds to wait for capture to catch up")
    ap.add_argument("--max-print", type=int, default=15)
    add_conn_args(ap)
    args = ap.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else sorted(Path("runs").iterdir())[-1]
    meta = json.loads((run_dir / "meta.json").read_text())
    n_ids = meta["n_ids"]
    print(f"Analyzing {run_dir}  (n_ids={n_ids}, duration={meta['args']['duration']}s, "
          f"chaos='{meta['args']['chaos']}')")

    # --- writers -> what was committed
    committed_max, committed_pairs, n_txn = {}, set(), 0
    for p in sorted(run_dir.glob("writer_*.jsonl")) + sorted(run_dir.glob("straggler_*.jsonl")):
        for rec in load_jsonl(p):
            if "c" not in rec:
                continue
            n_txn += 1
            for i, ver in rec["c"]:
                committed_pairs.add((i, ver))
                if ver > committed_max.get(i, -1):
                    committed_max[i] = ver
    print(f"Writers committed {n_txn} transactions touching {len(committed_pairs)} (row, version) pairs.")

    # --- ground truth from the primary
    conn = connect(args, args.primary_port)
    cur = conn.cursor()
    print("Waiting for the CDC capture job on the primary to catch up...")
    caught_up, behind = wait_for_capture(cur, committed_max, args.wait)
    ct_rows = fetch_ct(cur)
    conn.close()
    ct_pairs = {(i, v) for i, v, _ in ct_rows}
    missing = sorted(p for p in committed_pairs if p not in ct_pairs)
    print(f"CT rows loaded: {len(ct_rows)}.  caught_up={caught_up}")
    completeness_ok = not missing
    if missing:
        print(f"*** COMPLETENESS: {len(missing)} committed (row, version) pairs never appeared in CDC "
              f"(first few: {missing[:5]}) -- either the capture job was still behind (see --wait) "
              f"or CDC lost changes.")
    idx = build_index(ct_rows)

    chaos = load_jsonl(run_dir / "chaos.jsonl") if (run_dir / "chaos.jsonl").exists() else []
    if chaos:
        print("Chaos events:", dict(Counter(e["event"] for e in chaos)),
              "| failed:", sum(1 for e in chaos if "error" in e))

    # --- observers
    by_mode = defaultdict(list)
    for p in sorted(run_dir.glob("observer_*.jsonl")):
        mode = p.stem[len("observer_"):].rsplit("_", 1)[0]   # observer_<mode>_<copy>.jsonl
        by_mode[mode].append(p)

    all_violations = []
    report = {}
    for mode, paths in sorted(by_mode.items()):
        c = Counter()
        min_slacks = []
        for p in paths:
            prev_low = None
            for obs in load_jsonl(p):
                if "err" in obs:
                    c["errors"] += 1
                    prev_low = None
                    continue
                c["obs"] += 1
                res = check_observation(obs, idx, n_ids)
                if res["null_lsn"]:
                    c["null_lsn"] += 1
                    continue
                if res["viol"]:
                    c["obs_with_violation"] += 1
                    c["violating_rows"] += len(res["viol"])
                    for (i, s, e) in res["viol"]:
                        all_violations.append({"mode": mode, "t": obs["t"], "row": i, "snapshot": s,
                                               "expected_min": e, "l_low": obs["l_low"],
                                               "l_in": obs["l_in"], "l_high": obs["l_high"]})
                c["rows_checked"] += res["rows_checked"]
                c["rows_exact"] += res["rows_exact"]
                c["rows_ahead_low"] += res["rows_ahead_low"]
                c["obs_ahead_low"] += 1 if res["rows_ahead_low"] else 0
                c["rows_ahead_high"] += res["rows_ahead_high"]
                c["obs_ahead_high"] += 1 if res["rows_ahead_high"] else 0
                c["max_ahead_high"] = max(c["max_ahead_high"], res["max_ahead_high"])
                if res["min_slack"] is not None:
                    min_slacks.append(res["min_slack"])
                if obs["l_in"] != obs["l_low"]:
                    c["l_in_ne_l_low"] += 1
                if obs["l_high"] is not None and obs["l_low"] is not None and obs["l_high"] < obs["l_low"]:
                    c["l_high_lt_l_low"] += 1
                if obs["l_high"] == obs["l_low"]:
                    c["l_high_eq_l_low"] += 1
                if prev_low is not None:
                    if obs["l_low"] < prev_low:
                        c["l_low_regressed"] += 1
                    elif obs["l_low"] != prev_low:
                        c["l_low_advanced"] += 1
                prev_low = obs["l_low"]
                present = [x for x in obs["v"] if x >= 0]
                if obs["cnt"] != len(present) or obs["sum"] != sum(present):
                    c["intra_txn_drift"] += 1
        min_slacks.sort()
        report[mode] = {"counters": dict(c),
                        "min_slack_p50": pct(min_slacks, 50), "min_slack_p95": pct(min_slacks, 95),
                        "min_slack_max": min_slacks[-1] if min_slacks else None,
                        "min_slack_zero_frac": (sum(1 for s in min_slacks if s == 0) / len(min_slacks)
                                                if min_slacks else None)}

    # --- report
    print("\n" + "=" * 78)
    for mode, r in report.items():
        c = r["counters"]
        n = max(1, c.get("obs", 0) - c.get("errors", 0))
        print(f"\n[{mode}]  observations={c.get('obs', 0)}  connection_errors={c.get('errors', 0)}  "
              f"null_lsn={c.get('null_lsn', 0)}")
        print(f"  LOWER-BOUND VIOLATIONS: {c.get('obs_with_violation', 0)} observations "
              f"({c.get('violating_rows', 0)} rows)   <-- the safety property")
        print(f"  coverage: rows checked={c.get('rows_checked', 0)}, "
              f"exactly at boundary={c.get('rows_exact', 0)}, snapshot ahead of L_low={c.get('rows_ahead_low', 0)}")
        print(f"            observations where snapshot was ahead of L_low: {c.get('obs_ahead_low', 0)}/{n}")
        print(f"            per-observation min slack (snapshot_ver - expected_ver): "
              f"p50={r['min_slack_p50']} p95={r['min_slack_p95']} max={r['min_slack_max']} "
              f"frac==0: {r['min_slack_zero_frac'] if r['min_slack_zero_frac'] is None else round(r['min_slack_zero_frac'], 3)}")
        print(f"            L_low advanced between consecutive observations: {c.get('l_low_advanced', 0)} times, "
              f"regressed: {c.get('l_low_regressed', 0)}")
        print(f"  L_in != L_low (LSN changed inside one txn): {c.get('l_in_ne_l_low', 0)}   "
              f"L_high < L_low: {c.get('l_high_lt_l_low', 0)}   L_high == L_low: {c.get('l_high_eq_l_low', 0)}")
        print(f"  intra-txn drift (COUNT/SUM disagrees with the rows read -> not a txn-level snapshot): "
              f"{c.get('intra_txn_drift', 0)}")
        print(f"  upper side: snapshot rows newer than CDC at L_high: {c.get('rows_ahead_high', 0)} rows in "
              f"{c.get('obs_ahead_high', 0)} observations, max {c.get('max_ahead_high', 0)} versions ahead "
              f"(expected: base data is visible before its CDC row -- informational, not a violation)")

    total_viol = len(all_violations)
    print("\n" + "=" * 78)
    if total_viol:
        print(f"*** {total_viol} violating (observation,row) pairs. First {args.max_print}:")
        for v in sorted(all_violations, key=lambda x: x["t"])[:args.max_print]:
            near = chaos_near(chaos, v["t"])
            print(f"  [{v['mode']}] t={v['t']:.3f} row={v['row']} snapshot_ver={v['snapshot']} "
                  f"expected>={v['expected_min']} l_low={v['l_low']} l_in={v['l_in']} l_high={v['l_high']}"
                  + (f"  near chaos: {[e['event'] for e in near]}" if near else ""))
    else:
        print("No lower-bound violations in any observer mode.")
        print("Read the coverage lines above before trusting that: it is only meaningful if snapshots were "
              "frequently ahead of L_low and min-slack sat near 0 (the check had resolution), the capture "
              "job was actually lagging at times, and chaos events ran.")
    if not completeness_ok:
        print("NOTE: completeness check failed (see above); treat this run as suspect.")

    (run_dir / "report.json").write_text(json.dumps(
        {"modes": report, "violations": all_violations[:500], "completeness_ok": completeness_ok,
         "missing_pairs": missing[:50], "chaos": chaos}, indent=2))
    print(f"\nWrote {run_dir / 'report.json'}")
    return 1 if (total_viol or not completeness_ok) else 0


if __name__ == "__main__":
    sys.exit(main())
