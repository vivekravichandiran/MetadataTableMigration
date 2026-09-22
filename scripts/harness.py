#!/usr/bin/env python3
"""Test harness: serverless SQL for setup/verify, classic job for the merge."""
import json
import subprocess
import sys
import time

PROFILE = "e2_demo_west"
WAREHOUSE_ID = "75fd8278393d07eb"
JOB_ID = "758446040115874"  # serverless test job (classic job is 650943183729628)
SRC = "main.metadata_migration.cdf_metadata_src"
TGT = "main.metadata_migration.cdf_metadata_tgt"


def q(sql: str):
    """Run SQL on serverless; return list-of-rows (data_array)."""
    payload = {"warehouse_id": WAREHOUSE_ID, "statement": sql,
               "wait_timeout": "50s", "on_wait_timeout": "CANCEL"}
    p = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", PROFILE, "--json", json.dumps(payload)],
        capture_output=True, text=True)
    r = json.loads(p.stdout)
    st = r.get("status", {}).get("state")
    sid = r.get("statement_id")
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        g = subprocess.run(["databricks", "api", "get",
                            f"/api/2.0/sql/statements/{sid}", "-p", PROFILE],
                           capture_output=True, text=True)
        r = json.loads(g.stdout); st = r.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {r.get('status',{}).get('error',{})}\nSQL: {sql}")
    return r.get("result", {}).get("data_array", []) or []


def scalar(sql: str):
    rows = q(sql)
    return rows[0][0] if rows else None


def iscalar(sql: str):
    """Scalar as int (the SQL API returns all values as strings)."""
    v = scalar(sql)
    return int(v) if v is not None else None


def run_job(params: dict, timeout_s: int = 1200):
    """Trigger the deployed job with notebook_params; wait for terminal state.
    Returns (result_state, notebook_exit_output)."""
    print(f"  -> run_job params={params}")
    body = json.dumps({"job_id": int(JOB_ID), "notebook_params": params})
    p = subprocess.run(
        ["databricks", "jobs", "run-now",
         "--json", body, "--no-wait", "-p", PROFILE, "-o", "json"],
        capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"run-now failed: {p.stderr}\n{p.stdout}")
    run_id = json.loads(p.stdout)["run_id"]
    print(f"  -> run_id={run_id}  (polling)")
    deadline = time.time() + timeout_s
    life = ""
    while time.time() < deadline:
        g = subprocess.run(["databricks", "jobs", "get-run", str(run_id),
                            "-p", PROFILE, "-o", "json"],
                           capture_output=True, text=True)
        run = json.loads(g.stdout)
        state = run.get("state", {})
        new_life = state.get("life_cycle_state")
        if new_life != life:
            life = new_life; print(f"     state={life}")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = state.get("result_state")
            task_run_id = run.get("tasks", [{}])[0].get("run_id")
            out = ""
            try:
                o = subprocess.run(["databricks", "jobs", "get-run-output",
                                    str(task_run_id), "-p", PROFILE, "-o", "json"],
                                   capture_output=True, text=True)
                od = json.loads(o.stdout)
                out = od.get("notebook_output", {}).get("result", "") or \
                      od.get("error", "") or od.get("error_trace", "")
            except Exception as e:
                out = f"(no output: {e})"
            return result, out
        time.sleep(15)
    raise TimeoutError(f"run {run_id} did not finish within {timeout_s}s")


if __name__ == "__main__":
    # quick self-check
    print("src count:", scalar(f"SELECT COUNT(*) FROM {SRC}"))
    print("tgt count:", scalar(f"SELECT COUNT(*) FROM {TGT}"))
