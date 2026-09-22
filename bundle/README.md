# Metadata Table Migration & Merge Utility

A parameterized Databricks notebook, packaged as a Databricks Asset Bundle (DAB),
that synchronizes one `cdf_metadata_table` (**source**) into another (**target**)
using a dynamically-built Delta `MERGE`, while honoring a CSV-driven **exclusion list**.

## Layout

```
bundle/
├── databricks.yml                    # Bundle + variables + targets (dev/prod)
├── notebooks/
│   └── merge_metadata.py             # The parameterized merge notebook
├── resources/
│   └── merge_metadata_job.yml        # Job definition (notebook_task + params)
└── README.md
```

## What it does

1. Reads the **source** metadata table.
2. Loads and validates the **exclusion CSV**.
3. Drops source rows whose **`target_table_name`** matches an exclusion rule
   (skip-only — no deletes against the target):
   - `exclude_type = schema` → drop all rows under that `catalog.schema`.
   - `exclude_type = table`  → drop the exact `catalog.schema.table`.
4. Optionally deduplicates the source on the merge keys (keeps latest `last_updated_ts`).
5. Builds the `MERGE ... ON` condition dynamically from `merge_keys` (null-safe `<=>`).
6. Executes an **upsert** (`WHEN MATCHED UPDATE *` + `WHEN NOT MATCHED INSERT *`),
   optionally guarded by `src.last_updated_ts > tgt.last_updated_ts` for idempotency.

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_table` | `ops.meta.cdf_metadata_src` | Source metadata table (FQN) |
| `target_table` | `ops.meta.cdf_metadata_tgt` | Target metadata table (FQN) |
| `merge_keys` | `target_table_name` | Comma-separated merge key columns |
| `exclusion_csv_path` | `/Volumes/ops/meta/config/exclusion_list.csv` | Exclusion CSV path |
| `dry_run` | `true` (dev) / `false` (prod) | If true, compute counts but skip MERGE |
| `monotonic_guard` | `true` | Only update when source row is newer |
| `dedup_source` | `true` | Dedup source on merge keys before merging |

## Exclusion CSV format

```csv
exclude_type,catalog,schema,table
schema,ril_bulk_02,iot,
table,ril_bulk_02,finance,dim_finance_01
```

- `exclude_type = schema` → `table` column is blank.
- `exclude_type = table`  → `table` column is required.

## Deploy & run

```bash
# From the bundle/ directory
databricks bundle validate -t dev
databricks bundle deploy -t dev

# Dry run first (dev target defaults dry_run=true)
databricks bundle run merge_metadata_job -t dev

# Override any variable at run time
databricks bundle run merge_metadata_job -t dev \
  --var source_table=ops.meta.cdf_metadata_src \
  --var target_table=ops.meta.cdf_metadata_tgt \
  --var merge_keys=run_id \
  --var dry_run=false

# Production
databricks bundle deploy -t prod
databricks bundle run merge_metadata_job -t prod
```

## Testing guidance

- Start with `dry_run=true` to review source/excluded/after-exclusion counts.
- For the first functional test, use `merge_keys=target_table_name`.
- Then flip `dry_run=false` and re-run; the operation is idempotent, so it is safe
  to re-run with the monotonic guard enabled.

## Notes / assumptions (v1)

- **Upsert only**, no deletes (matches skip-only exclusion semantics).
- Source schema must be a subset of the target schema (validated; fails fast otherwise).
- Literal `"null"` strings in `prev_checkpoint_column_value` are preserved as-is.
- Before running, set the `workspace.host` for each target in `databricks.yml`.
