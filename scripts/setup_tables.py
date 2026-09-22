#!/usr/bin/env python3
"""Create source/target cdf_metadata tables on serverless and load sample data.

Runs all SQL via the Databricks Statement Execution API using the `uc_target`
CLI profile (so no tokens are handled here). The source CSV is cleaned of the
known corrupted row (stray "NN|" line-number prefix) before loading.
"""
import csv
import json
import subprocess
import sys
import time

PROFILE = "e2_demo_west"
WAREHOUSE_ID = "75fd8278393d07eb"  # Shared Endpoint (serverless)
CATALOG = "main"
SCHEMA = "metadata_migration"
SRC = f"{CATALOG}.{SCHEMA}.cdf_metadata_src"
TGT = f"{CATALOG}.{SCHEMA}.cdf_metadata_tgt"
CSV_PATH = "/Users/vivek.ravichandiran/MetadataTableMigration/cdf_metadata_table_sample.csv"

COLUMNS = [
    ("run_id", "STRING"),
    ("job_id", "STRING"),
    ("source_table_name", "STRING"),
    ("target_table_name", "STRING"),
    ("checkpoint_column_name", "STRING"),
    ("checkpoint_column_type", "STRING"),
    ("checkpoint_column_value", "TIMESTAMP"),
    ("prev_checkpoint_column_value", "TIMESTAMP"),
    ("is_delta_cdf_enabled", "BOOLEAN"),
    ("insert_ts", "TIMESTAMP"),
    ("last_updated_ts", "TIMESTAMP"),
]


def run_sql(statement: str, label: str) -> dict:
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s",
        "on_wait_timeout": "CANCEL",
    }
    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        print(f"[{label}] CLI error:\n{proc.stderr}")
        sys.exit(1)
    resp = json.loads(proc.stdout)
    state = resp.get("status", {}).get("state")
    stmt_id = resp.get("statement_id")

    # Poll if still running.
    while state in ("PENDING", "RUNNING"):
        time.sleep(2)
        p = subprocess.run(
            ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}",
             "-p", PROFILE],
            capture_output=True, text=True,
        )
        resp = json.loads(p.stdout)
        state = resp.get("status", {}).get("state")

    if state != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {})
        print(f"[{label}] FAILED ({state}): {err.get('message', resp)}")
        sys.exit(1)
    print(f"[{label}] OK")
    return resp


def clean_rows():
    """Parse CSV, repairing rows that carry a stray 'NN|' line-number prefix."""
    rows = []
    with open(CSV_PATH, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        for raw in reader:
            if not raw or all(c.strip() == "" for c in raw):
                continue
            # Repair corrupted first cell like "    10|8b4963...".
            first = raw[0]
            if "|" in first:
                first = first.split("|", 1)[1]
            raw = [first.strip()] + [c for c in raw[1:]]
            rows.append(raw)
    return header, rows


def sql_literal(col_type: str, value: str) -> str:
    v = (value or "").strip()
    if v == "" or v.lower() == "null":
        return "NULL"
    if col_type == "BOOLEAN":
        return "true" if v.lower() == "true" else "false"
    if col_type == "TIMESTAMP":
        return f"CAST('{v}' AS TIMESTAMP)"
    # STRING — escape single quotes.
    return "'" + v.replace("'", "''") + "'"


def build_insert(rows) -> str:
    col_names = ", ".join(c for c, _ in COLUMNS)
    values_rows = []
    for r in rows:
        cells = [sql_literal(t, r[i]) for i, (_, t) in enumerate(COLUMNS)]
        values_rows.append("(" + ", ".join(cells) + ")")
    return f"INSERT INTO {SRC} ({col_names}) VALUES\n" + ",\n".join(values_rows)


def main():
    header, rows = clean_rows()
    print(f"Parsed {len(rows)} clean data row(s) from CSV.")

    col_ddl = ",\n  ".join(f"{c} {t}" for c, t in COLUMNS)

    run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}", "create schema")
    run_sql(f"CREATE OR REPLACE TABLE {SRC} (\n  {col_ddl}\n) USING DELTA", "create source")
    run_sql(f"CREATE OR REPLACE TABLE {TGT} (\n  {col_ddl}\n) USING DELTA", "create target")
    run_sql(build_insert(rows), "load source data")

    res = run_sql(f"SELECT COUNT(*) FROM {SRC}", "count source")
    cnt = res.get("result", {}).get("data_array", [["?"]])[0][0]
    print(f"\nSource row count: {cnt}")
    print(f"Source: {SRC}")
    print(f"Target: {TGT} (empty)")


if __name__ == "__main__":
    main()
