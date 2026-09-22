#!/usr/bin/env python3
"""Test 2: changes in source (update + insert) reflect in target after merge."""
from harness import q, scalar, iscalar, run_job, SRC, TGT

print("== Test 2: source change reflects in target ==")

T_UPD = "ai_27.e2e_bronze.postgres_products_incr"        # existing -> update path
T_NEW = "ai_27.e2e_bronze.newly_added_table_t2"          # new -> insert path
NEW_CKPT = "2099-06-15 12:00:00"

tgt_before = iscalar(f"SELECT COUNT(*) FROM {TGT}")
old_val = scalar(f"SELECT checkpoint_column_value FROM {TGT} WHERE target_table_name='{T_UPD}'")
print(f"target rows before={tgt_before}; {T_UPD} ckpt before={old_val}")
assert tgt_before == 10, "expected 10 rows from Test 1"

# (a) UPDATE an existing source row: newer checkpoint + newer last_updated_ts
q(f"""UPDATE {SRC}
       SET checkpoint_column_value = TIMESTAMP'{NEW_CKPT}',
           last_updated_ts        = TIMESTAMP'{NEW_CKPT}'
     WHERE target_table_name = '{T_UPD}'""")

# (b) INSERT a brand-new source row with a new target_table_name
q(f"""INSERT INTO {SRC} (run_id, job_id, source_table_name, target_table_name,
        checkpoint_column_name, checkpoint_column_type, checkpoint_column_value,
        prev_checkpoint_column_value, is_delta_cdf_enabled, insert_ts, last_updated_ts)
     VALUES ('t2newrun00000000000000000000test','ingest_t2_new','ai27_test_db.new_src_t2',
        '{T_NEW}','last_updated_at','timestamp', TIMESTAMP'2099-06-16 00:00:00',
        NULL, false, TIMESTAMP'2099-06-16 00:00:00', TIMESTAMP'2099-06-16 00:00:00')""")

expected = iscalar(f"SELECT COUNT(DISTINCT target_table_name) FROM {SRC}")
print(f"expected target rows after merge={expected}")

result, out = run_job({
    "source_table": SRC, "target_table": TGT,
    "merge_keys": "target_table_name", "exclusion_csv_path": "",
    "dry_run": "false", "monotonic_guard": "true", "dedup_source": "true",
})
print(f"job result={result}")
print(f"notebook output: {out[:400]}")
assert result == "SUCCESS", f"job failed: {result}"

tgt_after = iscalar(f"SELECT COUNT(*) FROM {TGT}")
upd_val = scalar(f"SELECT checkpoint_column_value FROM {TGT} WHERE target_table_name='{T_UPD}'")
new_exists = iscalar(f"SELECT COUNT(*) FROM {TGT} WHERE target_table_name='{T_NEW}'")
print(f"target rows after={tgt_after}; {T_UPD} ckpt after={upd_val}; new row present={new_exists}")

assert tgt_after == expected == 11, f"expected 11 rows, got {tgt_after}"
assert str(upd_val).startswith("2099-06-15"), f"update not reflected: {upd_val}"
assert new_exists == 1, "new source row was not inserted into target"

print("\nTEST 2 PASSED")
