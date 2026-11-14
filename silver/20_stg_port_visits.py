# Databricks notebook source
# Member 3 - Silver port visits
# Reads Bronze port_visit JSONL, transforms (silver/port_visits_logic.py),
# validates, deduplicates and MERGEs. Set dry_run=true to skip all writes.

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import sys
import uuid

# COMMAND ----------

# 1. Parameters and configuration

dbutils.widgets.text("dry_run", "true")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("expected_unique_ids", "296199")  # blank = skip this check

DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
RUN_ID = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
EXPECTED_UNIQUE = dbutils.widgets.get("expected_unique_ids").strip()

REPO_PATH = (
    "/Workspace/Users/urwaaa.04@gmail.com/"
    "Global-Maritime-Traffic-Vessel-Activity-Intelligence-Warehouse"
)
if REPO_PATH not in sys.path:
    sys.path.append(REPO_PATH)

from schemas.silver_schemas import STG_PORT_VISITS_SCHEMA
from silver.transform_utils import add_audit_columns
from silver.port_visits_logic import transform_port_visits

BRONZE_PATH = (
    "abfss://bronze@stmaritimeks.dfs.core.windows.net/"
    "gfw/events/port_visit"
)
TARGET_TABLE = "project_staging.stg_port_visits"
REJECT_TABLE = "project_staging.stg_rejects"

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Run ID:", RUN_ID)
print("Dry run:", DRY_RUN)
print("Bronze path:", BRONZE_PATH)

# COMMAND ----------

# 2. Read Bronze JSONL as text (explicit schema is applied in the logic module).
# _metadata works on Serverless; input_file_name() does not.
# pathGlobFilter keeps stray .part files out of the read.

raw = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.jsonl")
    .text(BRONZE_PATH)
    .select(
        F.col("value").alias("_raw_json"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_bronze_ingest_ts"),
    )
)

# COMMAND ----------

# 3. Transform, validate, deduplicate (pure logic, tested locally).

valid, rejects, counts = transform_port_visits(raw)

print("Counts from transform:")
for k, v in counts.items():
    print(f"  {k}: {v}")

assert counts["reconciled"], (
    "DQ-R01 reconciliation failed: unique ids != valid + rejects"
)
if EXPECTED_UNIQUE:
    assert counts["rows_after_dedupe"] == int(EXPECTED_UNIQUE), (
        f"Expected {EXPECTED_UNIQUE} unique ids, got {counts['rows_after_dedupe']}"
    )

# COMMAND ----------

# 4. Add audit columns and select the exact target schema order.
# NOTE: check add_audit_columns does not clobber _bronze_ingest_ts set above.

silver = add_audit_columns(valid, run_id=RUN_ID).select(
    *[field.name for field in STG_PORT_VISITS_SCHEMA]
)

rejects_for_table = add_audit_columns(rejects, run_id=RUN_ID).select(
    "reject_id",
    "entity",
    "reject_reason",
    "raw_json",
    "_source_file",
    "_bronze_ingest_ts",
    "_silver_load_ts",
    "_run_id",
)

# COMMAND ----------

# 5. MERGE Silver rows by event_id. Only overwrite when the incoming row is
# at least as new, so re-running an old window cannot replace newer data.

MIN_TS = "timestamp'1900-01-01 00:00:00'"
NEWER = (
    f"coalesce(s._bronze_ingest_ts, {MIN_TS}) >= "
    f"coalesce(t._bronze_ingest_ts, {MIN_TS})"
)

if DRY_RUN:
    print("DRY RUN: skipping MERGE into", TARGET_TABLE)
else:
    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(silver.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll(condition=NEWER)
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("MERGE completed:", TARGET_TABLE)

# COMMAND ----------

# 6. MERGE rejected records by deterministic reject_id.

if DRY_RUN:
    print("DRY RUN: skipping MERGE into", REJECT_TABLE)
else:
    (
        DeltaTable.forName(spark, REJECT_TABLE).alias("t")
        .merge(rejects_for_table.alias("s"), "t.reject_id = s.reject_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Reject MERGE completed:", REJECT_TABLE)

# COMMAND ----------

# 7. Run summary for Member 4 (swap print for common/audit.py when it exists).

summary = {
    "run_id": RUN_ID,
    "dry_run": DRY_RUN,
    "rows_in": counts["rows_in"],
    "duplicates_removed": counts["duplicates_removed"],
    "rows_after_dedupe": counts["rows_after_dedupe"],
    "rejects": counts["rejects"],
    "rows_out": counts["valid"],
    "reconciled": counts["reconciled"],
}
print("PORT VISIT RUN SUMMARY")
for k, v in summary.items():
    print(f"{k}: {v}")

if not DRY_RUN:
    print("Target table count:", spark.table(TARGET_TABLE).count())
    print("Reject table count:", spark.table(REJECT_TABLE).count())