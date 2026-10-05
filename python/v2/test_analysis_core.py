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
