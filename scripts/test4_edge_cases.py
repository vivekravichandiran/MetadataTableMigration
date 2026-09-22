#!/usr/bin/env python3
"""Test 4: idempotency, dry-run safety, dynamic multi-key merge, malformed CSV."""
import ast
import os
import subprocess
from harness import q, scalar, iscalar, run_job, SRC, TGT, PROFILE

EXCL = "/Volumes/main/metadata_migration/config/exclusion_list.csv"
BAD_LOCAL = "/tmp/exclusion_bad.csv"
BAD_VOL = "dbfs:/Volumes/main/metadata_migration/config/exclusion_bad.csv"
BAD_PATH = "/Volumes/main/metadata_migration/config/exclusion_bad.csv"


def metrics(out):
    return ast.literal_eval(out).get("operation_metrics", {})


# ---------------------------------------------------------------- 4a idempotency
print("== Test 4a: idempotency (re-run, no source change) ==")
before = iscalar(f"SELECT COUNT(*) FROM {TGT}")
result, out = run_job({
    "source_table": SRC, "target_table": TGT, "merge_keys": "target_table_name",
    "exclusion_csv_path": EXCL, "dry_run": "false",
    "monotonic_guard": "true", "dedup_source": "true"})
assert result == "SUCCESS", f"4a job failed: {result}"
after = iscalar(f"SELECT COUNT(*) FROM {TGT}")
m = metrics(out)
print(f"target before={before}, after={after}, inserted={m.get('numTargetRowsInserted')}, "
      f"updated={m.get('numTargetRowsUpdated')}")
assert after == before, "idempotency: row count changed"
assert m.get("numTargetRowsInserted") == "0" and m.get("numTargetRowsUpdated") == "0", \
    "idempotency: unexpected inserts/updates"
print("4a PASSED\n")

# ------------------------------------------------------------------- 4b dry-run
print("== Test 4b: dry-run does not mutate target ==")
# Make a change that WOULD merge, then run dry_run and confirm no mutation.
q(f"UPDATE {SRC} SET last_updated_ts = TIMESTAMP'2099-12-31 00:00:00', "
  f"checkpoint_column_value = TIMESTAMP'2099-12-31 00:00:00' "
  f"WHERE target_table_name = 'ai_27.e2e_bronze.postgres_products_incr'")
b4 = iscalar(f"SELECT COUNT(*) FROM {TGT}")
val_before = scalar(f"SELECT checkpoint_column_value FROM {TGT} "
                    f"WHERE target_table_name='ai_27.e2e_bronze.postgres_products_incr'")
result, out = run_job({
    "source_table": SRC, "target_table": TGT, "merge_keys": "target_table_name",
    "exclusion_csv_path": EXCL, "dry_run": "true",
    "monotonic_guard": "true", "dedup_source": "true"})
assert result == "SUCCESS", f"4b job failed: {result}"
af = iscalar(f"SELECT COUNT(*) FROM {TGT}")
val_after = scalar(f"SELECT checkpoint_column_value FROM {TGT} "
                   f"WHERE target_table_name='ai_27.e2e_bronze.postgres_products_incr'")
summary = ast.literal_eval(out)
print(f"target rows {b4}->{af}; ckpt {val_before}->{val_after}; dry_run flag={summary.get('dry_run')}")
assert af == b4 and str(val_before) == str(val_after), "dry-run mutated the target!"
assert summary.get("dry_run") is True, "dry_run not reported"
print("4b PASSED\n")

# --------------------------------------------------------------- 4c multi-key
print("== Test 4c: dynamic multi-key merge (run_id,target_table_name) ==")
q(f"TRUNCATE TABLE {TGT}")
expected = iscalar(f"""
  SELECT COUNT(*) FROM (SELECT DISTINCT run_id, target_table_name FROM {SRC}
    WHERE NOT (
        (split(target_table_name,'\\\\.')[0]='ril_bulk_02' AND split(target_table_name,'\\\\.')[1]='iot')
     OR (target_table_name='ril_bulk_02.finance.dim_finance_01')))""")
result, out = run_job({
    "source_table": SRC, "target_table": TGT,
    "merge_keys": "run_id,target_table_name", "exclusion_csv_path": EXCL,
    "dry_run": "false", "monotonic_guard": "true", "dedup_source": "true"})
assert result == "SUCCESS", f"4c job failed: {result}"
tot = iscalar(f"SELECT COUNT(*) FROM {TGT}")
# With composite key, duplicated target_table_name rows are kept separately.
dup_rows = iscalar(f"SELECT COUNT(*) FROM {TGT} "
                   f"WHERE target_table_name='ai_27.e2e_bronze.postgres_incr_product_id'")
print(f"expected(distinct run_id+target, non-excluded)={expected}; target total={tot}; "
      f"rows for duplicated target={dup_rows}")
assert tot == expected, f"multi-key expected {expected}, got {tot}"
assert dup_rows == 2, "composite key should retain both rows of a duplicated target"
print("4c PASSED\n")

# --------------------------------------------------------- 4d malformed CSV
print("== Test 4d: malformed exclusion CSV fails fast ==")
with open(BAD_LOCAL, "w") as f:
    f.write("exclude_type,catalog,schema,table\n")
    f.write("table,ril_bulk_02,finance,\n")   # table type but blank table -> invalid
subprocess.run(["databricks", "fs", "cp", BAD_LOCAL, BAD_VOL, "-p", PROFILE,
                "--overwrite"], capture_output=True, text=True)
result, out = run_job({
    "source_table": SRC, "target_table": TGT, "merge_keys": "target_table_name",
    "exclusion_csv_path": BAD_PATH, "dry_run": "true",
    "monotonic_guard": "true", "dedup_source": "true"})
print(f"job result={result}; output snippet={out[:200]}")
assert result == "FAILED", "malformed CSV should fail the job"
assert "Malformed" in out or "malformed" in out, "expected a malformed-rule error message"
print("4d PASSED\n")

print("ALL TEST 4 EDGE CASES PASSED")
