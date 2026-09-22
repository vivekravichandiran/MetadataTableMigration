#!/usr/bin/env python3
"""Test 1: first-time load into an empty target."""
from harness import q, scalar, iscalar, run_job, SRC, TGT

print("== Test 1: first-time load ==")
q(f"TRUNCATE TABLE {TGT}")
src = iscalar(f"SELECT COUNT(*) FROM {SRC}")
tgt0 = iscalar(f"SELECT COUNT(*) FROM {TGT}")
expected = iscalar(f"SELECT COUNT(DISTINCT target_table_name) FROM {SRC}")
print(f"source rows={src}, target rows (pre)={tgt0}, expected after load={expected}")
assert tgt0 == 0, "target should start empty"

result, out = run_job({
    "source_table": SRC, "target_table": TGT,
    "merge_keys": "target_table_name", "exclusion_csv_path": "",
    "dry_run": "false", "monotonic_guard": "true", "dedup_source": "true",
})
print(f"job result={result}")
print(f"notebook output: {out[:600]}")
assert result == "SUCCESS", f"job did not succeed: {result}"

tgt1 = iscalar(f"SELECT COUNT(*) FROM {TGT}")
print(f"target rows (post)={tgt1}")
assert tgt1 == expected, f"expected {expected} rows, got {tgt1}"

# Duplicated target must keep the latest source row (by last_updated_ts).
dup = "ai_27.e2e_bronze.postgres_incr_product_id"
src_latest = scalar(
    f"SELECT checkpoint_column_value FROM {SRC} WHERE target_table_name='{dup}' "
    f"ORDER BY last_updated_ts DESC LIMIT 1")
tgt_val = scalar(
    f"SELECT checkpoint_column_value FROM {TGT} WHERE target_table_name='{dup}'")
print(f"dup target latest src ckpt={src_latest}, tgt ckpt={tgt_val}")
assert str(src_latest) == str(tgt_val), "dedup did not keep the latest row"

print("\nTEST 1 PASSED")
