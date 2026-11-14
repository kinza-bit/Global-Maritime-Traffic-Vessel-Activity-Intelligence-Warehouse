
# Databricks notebook source
# Member 3 - Silver vessel events
# Reads Bronze fishing, loitering and encounter JSONL, transforms,
# validates, deduplicates and MERGEs. Set dry_run=true to skip all writes.

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import sys
import uuid

# COMMAND ----------

# 1. Parameters and configuration

dbutils.widgets.text("dry_run", "true")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("expected_unique_ids", "")  # blank = skip this check

DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() == "true"
RUN_ID = dbutils.widgets.get("run_id").strip() or str(uuid.uuid4())
EXPECTED_UNIQUE = dbutils.widgets.get("expected_unique_ids").strip()

REPO_PATH = (
    "/Workspace/Users/urwaaa.04@gmail.com/"
    "Global-Maritime-Traffic-Vessel-Activity-Intelligence-Warehouse"
)
if REPO_PATH not in sys.path:
    sys.path.append(REPO_PATH)

from schemas.silver_schemas import STG_VESSEL_EVENTS_SCHEMA
from silver.transform_utils import add_audit_columns
from silver.vessel_events_logic import transform_vessel_events

BRONZE_BASE = (
    "abfss://bronze@stmaritimeks.dfs.core.windows.net/"
    "gfw/events"
)

FISHING_PATH = f"{BRONZE_BASE}/fishing"
LOITERING_PATH = f"{BRONZE_BASE}/loitering"
ENCOUNTER_PATH = f"{BRONZE_BASE}/encounter"

TARGET_TABLE = "project_staging.stg_vessel_events"
REJECT_TABLE = "project_staging.stg_rejects"

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Run ID:", RUN_ID)
print("Dry run:", DRY_RUN)
print("Fishing path:", FISHING_PATH)
print("Loitering path:", LOITERING_PATH)
print("Encounter path:", ENCOUNTER_PATH)

# COMMAND ----------

# 2. Read each Bronze JSONL folder as text.
# _metadata works on Serverless; input_file_name() does not.

def read_event_folder(path):
    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl")
        .text(path)
        .select(
            F.col("value").alias("_raw_json"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias(
                "_bronze_ingest_ts"
            ),
        )
    )


fishing_raw = read_event_folder(FISHING_PATH)
loitering_raw = read_event_folder(LOITERING_PATH)
encounter_raw = read_event_folder(ENCOUNTER_PATH)

raw = (
    fishing_raw
    .unionByName(loitering_raw)
    .unionByName(encounter_raw)
)

print("Bronze input rows:", raw.count())

# COMMAND ----------

# 3. Transform, validate and deduplicate.

valid, rejects, counts = transform_vessel_events(raw)

print("Counts from transform:")
for k, v in counts.items():
    print(f"  {k}: {v}")

assert counts["reconciled"], (
    "DQ-R01 reconciliation failed: unique ids != valid + rejects"
)

if EXPECTED_UNIQUE:
    assert counts["rows_after_dedupe"] == int(EXPECTED_UNIQUE), (
        f"Expected {EXPECTED_UNIQUE} unique ids, "
        f"got {counts['rows_after_dedupe']}"
    )

# COMMAND ----------

# 4. Add audit columns and select exact target schema order.

silver = add_audit_columns(valid, run_id=RUN_ID).select(
    *[field.name for field in STG_VESSEL_EVENTS_SCHEMA]
)

rejects_for_table = add_audit_columns(
    rejects, run_id=RUN_ID
).select(
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

# 5. MERGE Silver rows by event_id.

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

# 7. Run summary for Member 4.

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

print("VESSEL EVENTS RUN SUMMARY")
for k, v in summary.items():
    print(f"{k}: {v}")

if not DRY_RUN:
    print("Target table count:", spark.table(TARGET_TABLE).count())
    print("Reject table count:", spark.table(REJECT_TABLE).count())
