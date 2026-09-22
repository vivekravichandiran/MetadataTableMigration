#!/usr/bin/env python3
"""Probe candidate node types for capacity; print the first that provisions."""
import json, subprocess, sys, time

PROFILE = "uc_target"
USER = "vivek.ravichandiran@databricks.com"
CANDIDATES = [
    "Standard_D4ads_v6", "Standard_D4as_v5",
    "Standard_D4pds_v6", "Standard_D8ads_v6", "Standard_D4ds_v6",
]


def api(method, path, body=None):
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(p.stdout)
    except Exception:
        return {"_raw": p.stdout, "_err": p.stderr}


def probe(node_type):
    body = {
        "cluster_name": f"probe-{node_type}",
        "spark_version": "17.3.x-scala2.13",
        "node_type_id": node_type,
        "num_workers": 0,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": USER,
        "spark_conf": {"spark.databricks.cluster.profile": "singleNode",
                       "spark.master": "local[*, 4]"},
        "custom_tags": {"ResourceClass": "SingleNode"},
        "autotermination_minutes": 10,
    }
    r = api("post", "/api/2.0/clusters/create", body)
    cid = r.get("cluster_id")
    if not cid:
        print(f"  {node_type}: create failed: {r}")
        return None
    verdict = "timeout"
    for _ in range(30):  # ~90s
        time.sleep(3)
        ev = api("post", "/api/2.0/clusters/events",
                 {"cluster_id": cid, "limit": 5}).get("events", [])
        codes = [e.get("details", {}).get("reason", {}).get("code", "") for e in ev]
        types = [e.get("type") for e in ev]
        if "CLOUD_PROVIDER_RESOURCE_STOCKOUT" in codes or "ADD_NODES_FAILED" in types:
            verdict = "STOCKOUT"; break
        st = api("get", f"/api/2.0/clusters/get?cluster_id={cid}").get("state")
        if st == "RUNNING":
            verdict = "RUNNING"; break
        if st in ("TERMINATED", "ERROR"):
            verdict = f"TERMINATED:{st}"; break
    api("post", "/api/2.0/clusters/permanent-delete", {"cluster_id": cid})
    return verdict


for nt in CANDIDATES:
    v = probe(nt)
    print(f"{nt}: {v}")
    if v == "RUNNING":
        print(f"\nAVAILABLE_SKU={nt}")
        sys.exit(0)
print("\nNo candidate reached RUNNING within probe window.")
sys.exit(1)
