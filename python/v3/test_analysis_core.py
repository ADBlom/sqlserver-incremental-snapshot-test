"""Sanity tests for analysis_core on synthetic data. Run: python test_analysis_core.py"""
from analysis_core import build_index, check_observation, expected_at

L = lambda n: f"{n:020x}"
ct = [(1, 1, L(10)), (1, 2, L(30)), (1, 2, L(30)), (2, 1, L(20)), (2, 3, L(50))]
idx = build_index(ct)

assert expected_at(idx, 1, L(5)) is None
assert expected_at(idx, 1, L(10)) == 1
assert expected_at(idx, 1, L(29)) == 1
assert expected_at(idx, 1, L(30)) == 2
assert expected_at(idx, 2, L(49)) == 1 and expected_at(idx, 2, L(50)) == 3

# clean observation: snapshot at/ahead of L_low
r = check_observation({"v": [2, 3], "l_low": L(30), "l_high": L(30)}, idx, 2)
assert not r["viol"] and r["rows_ahead_low"] == 1 and r["rows_exact"] == 1
assert r["rows_ahead_high"] == 1 and r["max_ahead_high"] == 2   # row 2 is ahead of CDC at L_high

# planted violation: snapshot stale relative to L_low
r = check_observation({"v": [1, 1], "l_low": L(30), "l_high": L(30)}, idx, 2)
assert r["viol"] == [(1, 1, 2)], r["viol"]

# torn/missing row is a violation once CDC says it must exist
r = check_observation({"v": [2, -1], "l_low": L(30)}, idx, 2)
assert r["viol"] == [(2, -1, 1)]

# rows with no CDC history are ignored; NULL L_low is flagged, not checked
assert check_observation({"v": [0, 0], "l_low": L(1)}, idx, 2)["rows_checked"] == 0
assert check_observation({"v": [0, 0], "l_low": None}, idx, 2)["null_lsn"]
print("analysis_core tests passed")

# ---------------- v3: DMV helpers ----------------
from analysis_core import lsn_numeric_to_hex, build_lsn_of, check_lower, check_upper, build_series, delay_until

# lsn_numeric_to_hex round-trips against the same VLF/offset/slot values CDC would encode
n = 12 * 10**15 + 34 * 10**5 + 56
assert lsn_numeric_to_hex(n) == f"{12:08x}{34:08x}{56:04x}"
assert lsn_numeric_to_hex(None) is None

lsn_of = build_lsn_of(ct)   # reuse `ct` from above: [(1,1,L(10)), (1,2,L(30)), (1,2,L(30)), (2,1,L(20)), (2,3,L(50))]
assert lsn_of[(1, 1)] == L(10) and lsn_of[(2, 3)] == L(50)

# check_lower: d_pre read before snapshot must be a safe low watermark
r = check_lower([2, 1], idx, L(30))   # row1=v2 (committed at L30), row2=v1 (committed at L20)
assert not r["viol"] and r["checked"] == 2
r = check_lower([1, 1], idx, L(30))   # row1 stale (v1 committed at L10, but L30 says v2 should be visible)
assert r["viol"] == [(1, 1, 2)]

# check_upper: d_post read after commit must never under-count what the snapshot shows
r = check_upper([2, 1], lsn_of, L(30))   # row1=v2 truly committed at L30 -- exactly at the watermark, OK
assert not r["viol"] and r["newest"] == L(30)
r = check_upper([2, 3], lsn_of, L(30))   # row2=v3 truly committed at L50 > watermark L30 -- VIOLATION
assert r["viol"] == [(2, 3, L(50))]
r = check_upper([1, 1], lsn_of, L(5))    # both rows' versions were committed after watermark L5 -- VIOLATIONS
assert r["viol"] == [(1, 1, L(10)), (2, 1, L(20))]
r = check_upper([0, -1], lsn_of, L(1))   # version 0 was never committed (no CT row) -> unmapped, not a violation
assert not r["viol"] and r["unmapped"] == 1   # id=1,ver=0 unmapped; id=2 is -1 (absent) and skipped entirely

# build_series / delay_until: CDC catching up to a DMV position over time
samples = [{"t": 1.0, "l": L(10)}, {"t": 2.0, "l": L(20)}, {"t": 3.0, "l": L(30)}, {"t": 5.0, "l": L(30)}]
series = build_series(samples)
assert delay_until(series, L(20), t_end=1.5) == 0.5          # CDC reaches L20 at t=2.0
assert delay_until(series, L(10), t_end=1.5) == 0.0           # already there
assert delay_until(series, L(999), t_end=0.0) is None         # never reached in this series

print("v3 analysis_core tests passed")
