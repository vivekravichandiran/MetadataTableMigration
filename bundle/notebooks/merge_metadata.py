# Databricks notebook source
# MAGIC %md
# MAGIC # Metadata Table Merge Utility
# MAGIC
# MAGIC Synchronizes one `cdf_metadata_table` (**source**) into another (**target**) via a
# MAGIC dynamically-built Delta `MERGE`, honoring a CSV-driven **exclusion list**.
# MAGIC
# MAGIC The exclusion list is matched against each source row's **`target_table_name`**
# MAGIC (`catalog.schema.table`). Excluded rows are dropped from the source set *before* the
# MAGIC merge (**skip-only** semantics — no deletes are issued against the target).
# MAGIC
# MAGIC All behavior is driven by widget parameters, which are supplied by the DAB job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters (widgets)

# COMMAND ----------

dbutils.widgets.text("source_table", "", "Source metadata table (catalog.schema.table)")
dbutils.widgets.text("target_table", "", "Target metadata table (catalog.schema.table)")
dbutils.widgets.text("merge_keys", "target_table_name", "Comma-separated merge key columns")
dbutils.widgets.text("exclusion_csv_path", "", "Path to exclusion list CSV (UC Volume path)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no MERGE executed)")
dbutils.widgets.dropdown(
    "monotonic_guard", "true", ["true", "false"],
    "Only update when src.last_updated_ts > tgt.last_updated_ts",
)
dbutils.widgets.dropdown(
    "dedup_source", "true", ["true", "false"],
    "Deduplicate source on merge keys (keep latest last_updated_ts)",
)

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Column used both for the monotonic guard and for dedup tie-breaking.
ORDER_COL = "last_updated_ts"
# Column in the metadata rows that the exclusion list is matched against.
EXCLUSION_MATCH_COL = "target_table_name"


def _get(name: str) -> str:
    return dbutils.widgets.get(name).strip()


def _get_bool(name: str) -> bool:
    return _get(name).lower() == "true"


source_table = _get("source_table")
target_table = _get("target_table")
exclusion_csv_path = _get("exclusion_csv_path")
merge_keys = [k.strip() for k in _get("merge_keys").split(",") if k.strip()]
dry_run = _get_bool("dry_run")
monotonic_guard = _get_bool("monotonic_guard")
dedup_source = _get_bool("dedup_source")

print("Resolved parameters:")
print(f"  source_table        = {source_table}")
print(f"  target_table        = {target_table}")
print(f"  merge_keys          = {merge_keys}")
print(f"  exclusion_csv_path  = {exclusion_csv_path or '(none)'}")
print(f"  dry_run             = {dry_run}")
print(f"  monotonic_guard     = {monotonic_guard}")
print(f"  dedup_source        = {dedup_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate parameters & tables (fail fast)

# COMMAND ----------


def _table_exists(fqn: str) -> bool:
    try:
        return spark.catalog.tableExists(fqn)
    except Exception:
        return False


errors = []

if not source_table:
    errors.append("`source_table` is required.")
if not target_table:
    errors.append("`target_table` is required.")
if not merge_keys:
    errors.append("`merge_keys` must contain at least one column.")

if source_table and not _table_exists(source_table):
    errors.append(f"Source table not found: {source_table}")
if target_table and not _table_exists(target_table):
    errors.append(f"Target table not found: {target_table}")

if errors:
    raise ValueError("Parameter validation failed:\n  - " + "\n  - ".join(errors))

source_df = spark.read.table(source_table)
target_cols = set(spark.read.table(target_table).columns)
source_cols = set(source_df.columns)

# Merge keys must exist on both sides.
missing_keys = [k for k in merge_keys if k not in source_cols or k not in target_cols]
if missing_keys:
    raise ValueError(
        f"merge_keys not present in both source and target: {missing_keys}. "
        f"source cols={sorted(source_cols)} target cols={sorted(target_cols)}"
    )

# Source schema must be a subset of target so INSERT */UPDATE * align.
extra_in_source = source_cols - target_cols
if extra_in_source:
    raise ValueError(
        f"Source has columns absent from target (cannot INSERT */UPDATE *): "
        f"{sorted(extra_in_source)}"
    )

if EXCLUSION_MATCH_COL not in source_cols:
    raise ValueError(
        f"Source is missing the exclusion match column `{EXCLUSION_MATCH_COL}`."
    )

print("Parameter & schema validation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load & validate the exclusion list

# COMMAND ----------

EXCLUSION_SCHEMA_COLS = ["exclude_type", "catalog", "schema", "table"]


def load_exclusion_rules(path: str):
    """Return (schema_rules_df, table_rules_df) as normalized (catalog, schema[, table])
    DataFrames, or (None, None) when no path is provided."""
    if not path:
        print("No exclusion_csv_path provided — no exclusions will be applied.")
        return None, None

    raw = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )

    missing_cols = [c for c in EXCLUSION_SCHEMA_COLS if c not in raw.columns]
    if missing_cols:
        raise ValueError(
            f"Exclusion CSV missing required columns {missing_cols}. "
            f"Found: {raw.columns}"
        )

    rules = (
        raw.select(
            F.lower(F.trim(F.col("exclude_type"))).alias("exclude_type"),
            F.lower(F.trim(F.col("catalog"))).alias("catalog"),
            F.lower(F.trim(F.col("schema"))).alias("schema"),
            F.lower(F.trim(F.col("table"))).alias("table"),
        )
        # Drop fully blank / corrupt lines.
        .where(F.col("exclude_type").isNotNull() & (F.col("exclude_type") != ""))
    )

    # Treat literal 'null' / '' table values as absent.
    rules = rules.withColumn(
        "table",
        F.when(F.col("table").isin("", "null"), F.lit(None)).otherwise(F.col("table")),
    )

    # Validate rule integrity.
    bad = rules.where(
        (~F.col("exclude_type").isin("schema", "table"))
        | F.col("catalog").isNull() | (F.col("catalog") == "")
        | F.col("schema").isNull() | (F.col("schema") == "")
        | ((F.col("exclude_type") == "table") & F.col("table").isNull())
        | ((F.col("exclude_type") == "schema") & F.col("table").isNotNull())
    )
    bad_rows = bad.collect()
    if bad_rows:
        raise ValueError(
            "Malformed exclusion rule(s):\n  - "
            + "\n  - ".join(str(r.asDict()) for r in bad_rows)
        )

    schema_rules = (
        rules.where(F.col("exclude_type") == "schema")
        .select("catalog", "schema")
        .distinct()
    )
    table_rules = (
        rules.where(F.col("exclude_type") == "table")
        .select("catalog", "schema", "table")
        .distinct()
    )

    print(
        f"Loaded exclusion rules: {schema_rules.count()} schema-level, "
        f"{table_rules.count()} table-level."
    )
    return schema_rules, table_rules


schema_rules, table_rules = load_exclusion_rules(exclusion_csv_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply exclusions (skip-only)

# COMMAND ----------


def apply_exclusions(df: DataFrame, schema_rules, table_rules) -> DataFrame:
    """Drop rows whose `target_table_name` matches any exclusion rule."""
    if schema_rules is None and table_rules is None:
        return df

    # Split catalog.schema.table (max 3 parts; table may itself be simple).
    parts = F.split(F.col(EXCLUSION_MATCH_COL), "\\.")
    enriched = (
        df.withColumn("_ex_catalog", F.lower(parts.getItem(0)))
        .withColumn("_ex_schema", F.lower(parts.getItem(1)))
        .withColumn("_ex_table", F.lower(parts.getItem(2)))
    )

    filtered = enriched

    if schema_rules is not None and schema_rules.take(1):
        filtered = filtered.join(
            F.broadcast(schema_rules),
            (filtered["_ex_catalog"] == schema_rules["catalog"])
            & (filtered["_ex_schema"] == schema_rules["schema"]),
            "left_anti",
        )

    if table_rules is not None and table_rules.take(1):
        filtered = filtered.join(
            F.broadcast(table_rules),
            (filtered["_ex_catalog"] == table_rules["catalog"])
            & (filtered["_ex_schema"] == table_rules["schema"])
            & (filtered["_ex_table"] == table_rules["table"]),
            "left_anti",
        )

    return filtered.drop("_ex_catalog", "_ex_schema", "_ex_table")


source_count = source_df.count()
filtered_df = apply_exclusions(source_df, schema_rules, table_rules)
filtered_count = filtered_df.count()
excluded_count = source_count - filtered_count

print(f"Source rows            : {source_count}")
print(f"Excluded rows          : {excluded_count}")
print(f"Rows after exclusion   : {filtered_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deduplicate source on merge keys
# MAGIC Delta `MERGE` errors when a single target row matches multiple source rows.
# MAGIC When enabled, keep the latest row per key set (by `last_updated_ts`).

# COMMAND ----------

if dedup_source:
    order_expr = (
        F.col(ORDER_COL).desc_nulls_last()
        if ORDER_COL in filtered_df.columns
        else F.lit(1)
    )
    w = Window.partitionBy(*merge_keys).orderBy(order_expr)
    deduped_df = (
        filtered_df.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    deduped_count = deduped_df.count()
    print(f"Rows after dedup       : {deduped_count} "
          f"(removed {filtered_count - deduped_count} duplicate-key rows)")
else:
    deduped_df = filtered_df
    deduped_count = filtered_count

merge_source = deduped_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build & execute the dynamic MERGE

# COMMAND ----------

# Null-safe equality so nullable keys match correctly.
merge_condition = " AND ".join([f"tgt.`{k}` <=> src.`{k}`" for k in merge_keys])
print(f"MERGE ON: {merge_condition}")

if monotonic_guard and ORDER_COL in target_cols and ORDER_COL in source_cols:
    update_condition = f"src.`{ORDER_COL}` > tgt.`{ORDER_COL}`"
    print(f"WHEN MATCHED update guarded by: {update_condition}")
else:
    update_condition = None
    if monotonic_guard:
        print(f"monotonic_guard requested but `{ORDER_COL}` not on both sides — disabled.")

if dry_run:
    print("\nDRY RUN — MERGE will NOT be executed.")
    print(f"Would merge {deduped_count} source row(s) into {target_table}.")
else:
    tgt = DeltaTable.forName(spark, target_table)
    builder = tgt.alias("tgt").merge(merge_source.alias("src"), merge_condition)
    if update_condition:
        builder = builder.whenMatchedUpdateAll(condition=update_condition)
    else:
        builder = builder.whenMatchedUpdateAll()
    builder = builder.whenNotMatchedInsertAll()
    builder.execute()
    print("MERGE executed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run summary & metrics

# COMMAND ----------

summary = {
    "source_table": source_table,
    "target_table": target_table,
    "merge_keys": merge_keys,
    "dry_run": dry_run,
    "source_rows": source_count,
    "excluded_rows": excluded_count,
    "rows_after_exclusion": filtered_count,
    "rows_after_dedup": deduped_count,
}

if not dry_run:
    try:
        op_metrics = (
            spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
            .select("operation", "operationMetrics")
            .first()
        )
        summary["last_operation"] = op_metrics["operation"]
        summary["operation_metrics"] = op_metrics["operationMetrics"]
    except Exception as e:  # pragma: no cover
        summary["operation_metrics_error"] = str(e)

print("Run summary:")
for k, v in summary.items():
    print(f"  {k}: {v}")

dbutils.notebook.exit(str(summary))
