#!/usr/bin/env python3
"""Test 3: exclusion list drops schema-level and table-level matches; control passes."""
from harness import q, scalar, iscalar, run_job, SRC, TGT

print("== Test 3: exclusion list ==")
EXCL = "/Volumes/main/metadata_migration/config/exclusion_list.csv"

IOT1 = "ril_bulk_02.iot.device_events"          # schema-excluded
IOT2 = "ril_bulk_02.iot.device_metrics"         # schema-excluded
FIN1 = "ril_bulk_02.finance.dim_finance_01"     # table-excluded
FIN2 = "ril_bulk_02.finance.dim_finance_02"     # NOT excluded (control)
ALL = [IOT1, IOT2, FIN1, FIN2]

# Clean any prior test rows, then insert the 4 probe rows.
q(f"DELETE FROM {SRC} WHERE target_table_name IN ({','.join(repr(x) for x in ALL)})")
vals = []
for i, t in enumerate(ALL):
    vals.append(
        f"('t3run{i:028d}','ingest_t3','ril_bulk_02.src_{i}','{t}',"
        f"'last_updated_at','timestamp',TIMESTAMP'2099-07-0{i+1} 00:00:00',"
        f"NULL,false,TIMESTAMP'2099-07-0{i+1} 00:00:00',TIMESTAMP'2099-07-0{i+1} 00:00:00')")
q(f"""INSERT INTO {SRC} (run_id, job_id, source_table_name, target_table_name,
        checkpoint_column_name, checkpoint_column_type, checkpoint_column_value,
        prev_checkpoint_column_value, is_delta_cdf_enabled, insert_ts, last_updated_ts)
     VALUES {','.join(vals)}""")

q(f"TRUNCATE TABLE {TGT}")

# Expected: distinct targets not matched by exclusion rules.
expected = iscalar(f"""
  SELECT COUNT(DISTINCT target_table_name) FROM {SRC}
  WHERE NOT (
      (split(target_table_name,'\\\\.')[0]='ril_bulk_02' AND split(target_table_name,'\\\\.')[1]='iot')
   OR (target_table_name='ril_bulk_02.finance.dim_finance_01')
  )""")
print(f"expected target rows after exclusion={expected}")

result, out = run_job({
    "source_table": SRC, "target_table": TGT,
    "merge_keys": "target_table_name", "exclusion_csv_path": EXCL,
    "dry_run": "false", "monotonic_guard": "true", "dedup_source": "true",
})
print(f"job result={result}")
print(f"notebook output: {out[:500]}")
assert result == "SUCCESS", f"job failed: {result}"

tgt_total = iscalar(f"SELECT COUNT(*) FROM {TGT}")
iot_n = iscalar(f"SELECT COUNT(*) FROM {TGT} WHERE target_table_name IN ('{IOT1}','{IOT2}')")
fin1_n = iscalar(f"SELECT COUNT(*) FROM {TGT} WHERE target_table_name='{FIN1}'")
fin2_n = iscalar(f"SELECT COUNT(*) FROM {TGT} WHERE target_table_name='{FIN2}'")
print(f"target total={tgt_total}; iot rows={iot_n}; dim_finance_01={fin1_n}; dim_finance_02(control)={fin2_n}")

assert iot_n == 0, "schema-level exclusion failed (iot rows present)"
assert fin1_n == 0, "table-level exclusion failed (dim_finance_01 present)"
assert fin2_n == 1, "control row wrongly excluded (dim_finance_02 missing)"
assert tgt_total == expected, f"expected {expected} rows, got {tgt_total}"

print("\nTEST 3 PASSED")
