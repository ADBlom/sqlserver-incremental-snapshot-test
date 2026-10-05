"""Pure-Python (no DB) logic for checking observations against CDC ground truth.

LSNs are fixed-width (10-byte) binaries, passed around as 20-char lowercase hex strings;
lexicographic comparison of such strings equals SQL Server's binary comparison.
"""
from bisect import bisect_right
from collections import defaultdict


def build_index(ct_rows):
    """ct_rows: iterable of (id, version, lsn_hex) for insert/update-after images.
    Returns {id: (sorted_lsns, running_max_version)} so that
    expected_at(id, L) == highest version of `id` committed at LSN <= L."""
    per = defaultdict(list)
    for i, ver, lsn in ct_rows:
        per[i].append((lsn, ver))
    idx = {}
    for i, lst in per.items():
        lst.sort()
        lsns, runmax, m = [], [], -1
        for lsn, ver in lst:
            m = max(m, ver)
            lsns.append(lsn)
            runmax.append(m)
        idx[i] = (lsns, runmax)
    return idx


def expected_at(idx, row_id, lsn):
    e = idx.get(row_id)
    if e is None or lsn is None:
        return None
    k = bisect_right(e[0], lsn)
    return e[1][k - 1] if k else None


def check_observation(obs, idx, n_ids):
    """Lower-bound check: every change committed at LSN <= L_low must be visible in the
    snapshot (snapshot_version >= expected_version). Also gathers coverage metrics."""
    v, l_low, l_high = obs["v"], obs.get("l_low"), obs.get("l_high")
    res = {"viol": [], "min_slack": None, "rows_checked": 0, "rows_exact": 0,
           "rows_ahead_low": 0, "rows_ahead_high": 0, "max_ahead_high": 0, "null_lsn": False}
    if l_low is None:
        res["null_lsn"] = True
        return res
    for i in range(1, n_ids + 1):
        s = v[i - 1]
        e = expected_at(idx, i, l_low)
        if e is not None:
            slack = s - e
            res["rows_checked"] += 1
            if slack < 0:
                res["viol"].append((i, s, e))
            elif slack == 0:
                res["rows_exact"] += 1
            else:
                res["rows_ahead_low"] += 1
            ms = res["min_slack"]
            if ms is None or slack < ms:
                res["min_slack"] = slack
        if l_high is not None:
            eh = expected_at(idx, i, l_high)
            if eh is not None and s > eh:
                res["rows_ahead_high"] += 1
                res["max_ahead_high"] = max(res["max_ahead_high"], s - eh)
    return res


def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(p / 100.0 * len(sorted_vals)))]
