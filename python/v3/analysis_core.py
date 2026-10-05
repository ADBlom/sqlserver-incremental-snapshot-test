"""Pure-Python (no DB) logic for checking observations against CDC ground truth.

LSNs are fixed-width (10-byte) binaries, passed around as 20-char lowercase hex strings;
lexicographic comparison of such strings equals SQL Server's binary comparison.
"""
from bisect import bisect_left, bisect_right
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


def lsn_numeric_to_hex(n):
    """Convert the numeric(25,0) LSN form used by sys.dm_hadr_database_replica_states
    (VLF seq * 10^15 + log block offset * 10^5 + slot) to the 20-char hex form of the
    binary(10) LSN used by CDC:  4-byte VLF | 4-byte block offset | 2-byte slot."""
    if n is None:
        return None
    n = int(n)
    return f"{n // 10**15:08x}{(n // 10**5) % 10**10:08x}{n % 10**5:04x}"


def build_lsn_of(ct_rows):
    """(row id, version) -> commit LSN of the transaction that produced that version."""
    return {(i, ver): lsn for i, ver, lsn in ct_rows}


def check_lower(v, idx, lsn):
    """Everything committed at LSN <= `lsn` must be visible in snapshot `v`."""
    viol, min_slack, checked = [], None, 0
    for i in range(1, len(v) + 1):
        e = expected_at(idx, i, lsn)
        if e is None:
            continue
        slack = v[i - 1] - e
        checked += 1
        if slack < 0:
            viol.append((i, v[i - 1], e))
        if min_slack is None or slack < min_slack:
            min_slack = slack
    return {"viol": viol, "min_slack": min_slack, "checked": checked}


def check_upper(v, lsn_of, d):
    """No version visible in snapshot `v` may have been committed after `d` (if d is given).
    Also returns the commit LSN of the newest visible version."""
    viol, newest, unmapped = [], None, 0
    for i, s in enumerate(v, start=1):
        if s < 0:
            continue
        lsn = lsn_of.get((i, s))
        if lsn is None:
            unmapped += 1
            continue
        if newest is None or lsn > newest:
            newest = lsn
        if d is not None and lsn > d:
            viol.append((i, s, lsn))
    return {"viol": viol, "newest": newest, "unmapped": unmapped}


def rank(commit_lsns, lsn):
    """Number of distinct committed-transaction LSNs <= lsn (commit_lsns sorted, unique)."""
    return bisect_right(commit_lsns, lsn)


def build_series(samples):
    """samples: dicts with 't' and 'l' (CDC max LSN hex). Returns (times, running_max_lsn)."""
    ts, ls, m = [], [], ""
    for smp in sorted(samples, key=lambda x: x["t"]):
        if smp.get("l") is None:
            continue
        m = max(m, smp["l"])
        ts.append(smp["t"])
        ls.append(m)
    return ts, ls


def delay_until(series, d, t_end):
    """Seconds after t_end until CDC's max LSN first reached >= d (0 if already there),
    or None if it never did within the recorded series."""
    ts, ls = series
    k = bisect_left(ls, d)
    if k >= len(ls):
        return None
    return max(0.0, ts[k] - t_end)
