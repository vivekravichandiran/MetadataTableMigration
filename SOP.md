# SOP: Metadata Table Merge Utility

**Purpose:** Synchronize one `cdf_metadata_table` (**source**) into another (**target**)
using a dynamically-built Delta `MERGE`, while excluding tables listed in a CSV
exclusion config. This document is the step-by-step operating procedure.

- **Notebook:** `bundle/notebooks/merge_metadata.py`
- **Bundle:** `bundle/databricks.yml` (Databricks Asset Bundle / DAB)
- **Jobs:** `merge_metadata_job` (classic DBR 17.3 LTS job cluster) and
  `merge_metadata_job_serverless` (serverless variant for constrained workspaces)

---

## 1. What it does (in order)

1. Reads the **source** metadata table.
2. Loads and validates the **exclusion CSV**.
3. Drops source rows whose **`target_table_name`** matches an exclusion rule
   (**skip-only** — never deletes from the target):
   - `exclude_type = schema` → drops all rows under that `catalog.schema`.
   - `exclude_type = table`  → drops the exact `catalog.schema.table`.
4. (Optional) Deduplicates the source on the merge keys, keeping the latest
   row by `last_updated_ts`.
5. Builds the `MERGE ... ON` condition **dynamically** from `merge_keys`.
6. Runs an **upsert**: `WHEN MATCHED UPDATE *` + `WHEN NOT MATCHED INSERT *`,
   optionally guarded by `src.last_updated_ts > tgt.last_updated_ts`.

---

## 2. Parameters (single source of truth)

| Parameter | Required | Example | Description |
|-----------|----------|---------|-------------|
| `source_table` | Yes | `main.metadata_migration.cdf_metadata_src` | Fully-qualified source table |
| `target_table` | Yes | `main.metadata_migration.cdf_metadata_tgt` | Fully-qualified target table |
| `merge_keys` | Yes | `target_table_name` or `run_id,target_table_name` | Comma-separated key columns; the MERGE `ON` clause is built from these |
| `exclusion_csv_path` | No | `/Volumes/main/metadata_migration/config/exclusion_list.csv` | Path to exclusion CSV. Leave **empty** to apply no exclusions |
| `dry_run` | No | `true` / `false` | `true` computes counts but does **not** MERGE. Default `false` |
| `monotonic_guard` | No | `true` / `false` | Only update when the source row is newer (`last_updated_ts`). Default `true` |
| `dedup_source` | No | `true` / `false` | Dedup source on merge keys before merging. Default `true` |

> **Values are strings.** Booleans are the literal strings `"true"` / `"false"`.

### Exclusion CSV format

```csv
exclude_type,catalog,schema,table
schema,ril_bulk_02,iot,
table,ril_bulk_02,finance,dim_finance_01
```

- `exclude_type = schema` → leave `table` **blank**.
- `exclude_type = table`  → `table` is **required**.
- Malformed rows (e.g. `table` type with a blank table) cause the job to **fail fast**.

---

## 3. Prerequisites (one-time)

1. **Databricks CLI** installed and authenticated to the target workspace:
   ```bash
   databricks configure --token --profile <PROFILE> --host https://<workspace-host>/
   databricks current-user me -p <PROFILE>   # verify
   ```
2. **Unity Catalog objects** exist:
   - Source and target tables (identical 11-column schema).
   - A UC Volume for the exclusion CSV, e.g.
     `main.metadata_migration.config`.
3. **Upload the exclusion CSV** to the volume:
   ```bash
   databricks fs cp exclusion_list_example.csv \
     dbfs:/Volumes/main/metadata_migration/config/exclusion_list.csv \
     -p <PROFILE> --overwrite
   ```
4. **Compute:** the classic job needs a workspace where you can create a
   DBR 17.3 LTS job cluster. If you lack cluster-create permission, use the
   `merge_metadata_job_serverless` job instead (identical logic, serverless).

---

## 4. Deploy the bundle

From the `bundle/` directory:

```bash
cd bundle
databricks bundle validate -t dev -p <PROFILE>
databricks bundle deploy   -t dev -p <PROFILE>
```

- Edit the per-target `workspace.host` and the `variables` defaults in
  `databricks.yml` for your environment.
- `dev` deploys with development naming; `prod` is available via `-t prod`.
- If you switch workspaces, clear stale local state first: `rm -rf bundle/.databricks`.

---

## 5. Run via the deployed job (recommended)

**Option A — run with the deployed defaults:**
```bash
databricks bundle run merge_metadata_job -t dev -p <PROFILE>
```

**Option B — override parameters per run** (no redeploy) with `run-now`:
```bash
databricks jobs run-now --profile <PROFILE> --json '{
  "job_id": <JOB_ID>,
  "notebook_params": {
    "source_table": "main.metadata_migration.cdf_metadata_src",
    "target_table": "main.metadata_migration.cdf_metadata_tgt",
    "merge_keys": "target_table_name",
    "exclusion_csv_path": "/Volumes/main/metadata_migration/config/exclusion_list.csv",
    "dry_run": "false",
    "monotonic_guard": "true",
    "dedup_source": "true"
  }
}'
```

Find `<JOB_ID>`:
```bash
databricks jobs list -p <PROFILE> -o json | \
  python3 -c "import sys,json;[print(j['job_id'],j['settings']['name']) for j in json.load(sys.stdin)]"
```

**Recommended first run:** set `dry_run=true`, review the printed
source / excluded / after-exclusion counts, then re-run with `dry_run=false`.

---

## 6. Run as a STANDALONE notebook (passing parameters)

You can run `merge_metadata.py` directly in the workspace, outside the job.

### 6a. Interactive (widgets)

1. Import `bundle/notebooks/merge_metadata.py` into your workspace
   (or open it from the deployed bundle files path).
2. Attach it to a cluster running **DBR 17.3 LTS** (must have Unity Catalog access;
   use `Single User` access mode).
3. Run the first cell — it creates the input **widgets** at the top of the notebook.
4. Fill in the widget fields:
   - `source_table`, `target_table`, `merge_keys`, `exclusion_csv_path`
   - `dry_run`, `monotonic_guard`, `dedup_source` (dropdowns)
5. **Run All**. Read the run summary printed by the final cell.

> To apply **no exclusions**, leave `exclusion_csv_path` empty.
> Start with `dry_run = true` to preview, then switch to `false`.

### 6b. Programmatically from another notebook (`dbutils.notebook.run`)

```python
result = dbutils.notebook.run(
    "/Workspace/Path/To/merge_metadata",   # path to the imported notebook
    timeout_seconds=3600,
    arguments={
        "source_table": "main.metadata_migration.cdf_metadata_src",
        "target_table": "main.metadata_migration.cdf_metadata_tgt",
        "merge_keys": "run_id,target_table_name",
        "exclusion_csv_path": "/Volumes/main/metadata_migration/config/exclusion_list.csv",
        "dry_run": "false",
        "monotonic_guard": "true",
        "dedup_source": "true",
    },
)
print(result)   # the notebook returns its run-summary dict as a string
```

### 6c. Set widget values from code (e.g. in a scratch cell)

```python
dbutils.widgets.text("source_table", "main.metadata_migration.cdf_metadata_src")
dbutils.widgets.text("target_table", "main.metadata_migration.cdf_metadata_tgt")
dbutils.widgets.text("merge_keys", "target_table_name")
dbutils.widgets.text("exclusion_csv_path",
                     "/Volumes/main/metadata_migration/config/exclusion_list.csv")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
dbutils.widgets.dropdown("monotonic_guard", "true", ["true", "false"])
dbutils.widgets.dropdown("dedup_source", "true", ["true", "false"])
# then Run All
```

> **Note:** the notebook uses `dbutils.notebook.exit(...)` at the end. When run
> interactively this simply ends the run and prints the summary; it is required
> so `dbutils.notebook.run` callers receive the result.

---

## 7. Verify results

```sql
-- Row counts
SELECT COUNT(*) FROM main.metadata_migration.cdf_metadata_src;
SELECT COUNT(*) FROM main.metadata_migration.cdf_metadata_tgt;

-- Confirm an excluded target is absent (example)
SELECT COUNT(*) FROM main.metadata_migration.cdf_metadata_tgt
WHERE target_table_name = 'ril_bulk_02.finance.dim_finance_01';   -- expect 0

-- Inspect the last MERGE metrics
DESCRIBE HISTORY main.metadata_migration.cdf_metadata_tgt LIMIT 1;
```

The notebook also prints a **run summary**: `source_rows`, `excluded_rows`,
`rows_after_exclusion`, `rows_after_dedup`, and the Delta `operation_metrics`
(`numTargetRowsInserted`, `numTargetRowsUpdated`, ...).

---

## 8. Choosing merge keys

- Use a **unique** key set so a target row matches at most one source row.
- `target_table_name` alone is **not unique** if multiple runs write the same
  target — dedup keeps only the latest. For row-per-run granularity use a
  composite key, e.g. `run_id,target_table_name`.
- If the source has duplicate keys and `dedup_source=false`, Delta MERGE will
  error on multiple matches — enable dedup or pick a more specific key.

---

## 9. Safety & idempotency

- **Dry-run first** (`dry_run=true`) to preview counts without mutating the target.
- **Idempotent:** re-running with `monotonic_guard=true` and no source change
  makes **zero** inserts/updates.
- **Skip-only exclusions:** excluded rows are never merged and existing target
  rows are never deleted by this utility.
- Each MERGE is a **single Delta transaction** — a failure leaves the target
  unchanged.

---

## 10. Troubleshooting

| Symptom | Cause / Fix |
|---------|-------------|
| `not authorized to create clusters` | No cluster-create permission → use `merge_metadata_job_serverless` or an admin-provided cluster |
| `CLOUD_PROVIDER_RESOURCE_STOCKOUT` / `SkuNotAvailable` | Node type unavailable in region → change `node_type_id` in `merge_metadata_job.yml` |
| `Malformed exclusion rule(s)` | Fix the exclusion CSV (`table` type needs a table; `schema` type must leave table blank) |
| `merge_keys not present in both source and target` | Correct the key column names |
| `Source has columns absent from target` | Align schemas — source must be a subset of target |
| Multiple-match MERGE error | Enable `dedup_source=true` or use a more unique `merge_keys` |
| Bundle deploy references an old job id | `rm -rf bundle/.databricks` then redeploy |

---

## 11. Quick reference (copy/paste)

```bash
# Deploy
cd bundle && databricks bundle deploy -t dev -p <PROFILE>

# Dry-run via job
databricks jobs run-now -p <PROFILE> --json '{"job_id": <JOB_ID>,
  "notebook_params": {"source_table":"main.metadata_migration.cdf_metadata_src",
  "target_table":"main.metadata_migration.cdf_metadata_tgt",
  "merge_keys":"target_table_name","exclusion_csv_path":"",
  "dry_run":"true","monotonic_guard":"true","dedup_source":"true"}}'

# Live run: set "dry_run":"false"
```
