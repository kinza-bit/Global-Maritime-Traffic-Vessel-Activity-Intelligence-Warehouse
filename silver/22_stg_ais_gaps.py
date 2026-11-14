# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# 22_stg_ais_gaps.py  --  Bronze (gap events) -> Silver project_staging.stg_ais_gaps
#
# Self-contained (PySpark only, no imports from silver/transform_utils.py) and
# follows docs/silver_data_dictionary.md section 8 + the global rules / reject
# rules. Safe to re-run: Delta MERGE on event_id (gaps) and reject_id (rejects).
#
# FIRST RUN: keep dry_run = true (default). It reads Bronze, transforms, prints
# the counts and a sample, and writes nothing. Then set dry_run = false.
#
# Needs: project_staging.stg_ais_gaps and project_staging.stg_rejects to exist
# (run sql/10_stg_tables.sql first) and Bronze files under
#   abfss://bronze@<storage>.dfs.core.windows.net/gfw/events/gap/load_type=*/ingest_date=*/*.jsonl

# COMMAND ----------

import uuid
from pyspark.sql import functions as F, Window
from pyspark.sql import types as T

# Explicit Bronze read schema (no inferSchema). Scalars are read as STRING, then cast.
_S = T.StringType()
_pos = T.StructType([T.StructField("lat", _S), T.StructField("lon", _S)])
BRONZE_SCHEMA = T.StructType([
    T.StructField("id", _S),
    T.StructField("type", _S),
    T.StructField("start", _S),
    T.StructField("end", _S),
    T.StructField("vessel", T.StructType([
        T.StructField("id", _S), T.StructField("name", _S), T.StructField("ssvid", _S),
        T.StructField("flag", _S), T.StructField("type", _S)])),
    T.StructField("regions", T.StructType([
        T.StructField("highSeas", T.ArrayType(_S)),
        T.StructField("mpa", T.ArrayType(_S)),
        T.StructField("mpaNoTake", T.ArrayType(_S))])),
    T.StructField("gap", T.StructType([
        T.StructField("durationHours", _S), T.StructField("distanceKm", _S),
        T.StructField("impliedSpeedKnots", _S), T.StructField("intentionalDisabling", _S),
        T.StructField("positionsPerDaySatReception", _S),
        T.StructField("offPosition", _pos), T.StructField("onPosition", _pos)])),
])


def clean_text(c):
    """Trim, collapse repeated spaces, empty string -> NULL."""
    t = F.trim(F.regexp_replace(c.cast("string"), r"\s+", " "))
    return F.when(t == "", F.lit(None)).otherwise(t)


def try_num(colname, precision_scale):
    """Unparseable -> NULL (never an error). A real 0 stays 0."""
    return F.expr(f"try_cast({colname} as decimal({precision_scale}))")


def to_bool(colname):
    v = F.lower(F.trim(F.col(colname)))
    return (F.when(v.isin("true", "1"), F.lit(True))
             .when(v.isin("false", "0"), F.lit(False)))


def read_bronze(spark, path):
    return (spark.read.text(path)
            .select(F.col("value").alias("raw_json"),
                    F.col("_metadata.file_path").alias("_source_file")))


def flatten_and_clean(lines):
    """Parse JSON with the explicit schema, flatten, convert types, clean text."""
    p = lines.withColumn("j", F.from_json("raw_json", BRONZE_SCHEMA))
    raw = p.select(
        "raw_json", "_source_file",
        F.col("j.id").alias("id_raw"),
        F.col("j.type").alias("type_raw"),
        F.col("j.start").alias("start_raw"), F.col("j.end").alias("end_raw"),
        F.col("j.vessel.id").alias("vessel_id_raw"),
        F.col("j.vessel.name").alias("vessel_name_raw"),
        F.col("j.vessel.ssvid").alias("ssvid_raw"),
        F.col("j.vessel.flag").alias("flag_raw"),
        F.col("j.vessel.type").alias("vtype_raw"),
        F.col("j.gap.durationHours").alias("duration_raw"),
        F.col("j.gap.distanceKm").alias("distance_raw"),
        F.col("j.gap.impliedSpeedKnots").alias("speed_raw"),
        F.col("j.gap.intentionalDisabling").alias("intent_raw"),
        F.col("j.gap.positionsPerDaySatReception").alias("posday_raw"),
        F.col("j.gap.offPosition.lat").alias("off_lat_raw"),
        F.col("j.gap.offPosition.lon").alias("off_lon_raw"),
        F.col("j.gap.onPosition.lat").alias("on_lat_raw"),
        F.col("j.gap.onPosition.lon").alias("on_lon_raw"),
        (F.coalesce(F.size("j.regions.highSeas") > 0, F.lit(False))).alias("high_seas"),
        (F.coalesce(F.size("j.regions.mpa") > 0, F.lit(False))).alias("mpa"),
        (F.coalesce(F.size("j.regions.mpaNoTake") > 0, F.lit(False))).alias("mpa_no_take"),
    )
    return raw.select(
        "raw_json", "_source_file", "high_seas", "mpa", "mpa_no_take",
        clean_text(F.col("id_raw")).alias("event_id"),
        F.upper(clean_text(F.col("type_raw"))).alias("event_type"),
        clean_text(F.col("vessel_id_raw")).alias("vessel_id"),
        F.coalesce(F.upper(clean_text(F.col("vessel_name_raw"))), F.lit("Unknown")).alias("vessel_name"),
        F.col("vessel_name_raw").isNull().alias("_name_was_missing"),
        clean_text(F.col("ssvid_raw")).alias("ssvid"),
        F.upper(clean_text(F.col("flag_raw"))).alias("vessel_flag"),
        F.upper(clean_text(F.col("vtype_raw"))).alias("vessel_type"),
        F.expr("try_cast(start_raw as timestamp)").alias("start_ts"),
        F.expr("try_cast(end_raw as timestamp)").alias("end_ts"),
        try_num("duration_raw", "10,2").alias("duration_hours"),
        try_num("distance_raw", "10,2").alias("distance_km"),
        try_num("speed_raw", "10,2").alias("implied_speed_knots"),
        to_bool("intent_raw").alias("intentional_disabling_raw"),
        try_num("off_lat_raw", "10,6").alias("off_lat"),
        try_num("off_lon_raw", "10,6").alias("off_lon"),
        try_num("on_lat_raw", "10,6").alias("on_lat"),
        try_num("on_lon_raw", "10,6").alias("on_lon"),
        try_num("posday_raw", "10,2").alias("positions_per_day_sat_reception"),
        # file-name metadata used for de-duplication
        F.regexp_extract("_source_file", r"_(\d{4}-\d{2})\.jsonl", 1).alias("_window_month"),
        F.when(F.regexp_extract("_source_file", r"load_type=([a-z]+)", 1) == "incremental", 1)
         .otherwise(0).alias("_load_rank"),
        F.to_timestamp(F.regexp_extract("_source_file", r"ingest_date=(\d{4}-\d{2}-\d{2})", 1))
         .alias("_bronze_ingest_ts"),
    ).withColumn(
        "intentional_disabling", F.coalesce(F.col("intentional_disabling_raw"), F.lit(False))
    )


def dedupe(df):
    """One row per event_id. Keep latest window month, then incremental over initial,
    then file path as the final tie-break (docs/silver_data_dictionary.md section 1)."""
    key = F.coalesce(F.col("event_id"),
                     F.concat(F.lit("__null__"), F.monotonically_increasing_id().cast("string")))
    w = Window.partitionBy(key).orderBy(
        F.col("_window_month").desc(), F.col("_load_rank").desc(), F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_reject_reason(df):
    def bad_lat(c): return c.isNotNull() & ((c < -90) | (c > 90))
    def bad_lon(c): return c.isNotNull() & ((c < -180) | (c > 180))
    coords_bad = (bad_lat(F.col("off_lat")) | bad_lat(F.col("on_lat")) |
                  bad_lon(F.col("off_lon")) | bad_lon(F.col("on_lon")))
    reason = (F.when(F.col("event_id").isNull(), "DQ-C01")
               .when(F.col("vessel_id").isNull(), "DQ-C02")
               .when(F.col("start_ts").isNull() | F.col("end_ts").isNull(), "DQ-C03")
               .when(F.col("duration_hours") < 0, "DQ-V01")
               .when(F.col("end_ts") < F.col("start_ts"), "DQ-V02")
               .when(coords_bad, "DQ-V03")
               .when(F.col("event_type").isNotNull() & (F.col("event_type") != "GAP"), "DQ-V04"))
    return df.withColumn("reject_reason", reason)


def build_silver(valid, run_id):
    return valid.select(
        F.col("event_id"), F.col("vessel_id"), F.col("vessel_name"), F.col("ssvid"),
        F.col("vessel_flag"), F.col("vessel_type"),
        F.col("start_ts"), F.col("end_ts"), F.to_date("start_ts").alias("start_date"),
        F.col("duration_hours"), F.col("distance_km"), F.col("implied_speed_knots"),
        F.col("intentional_disabling"),
        F.col("off_lat"), F.col("off_lon"), F.col("on_lat"), F.col("on_lon"),
        F.col("positions_per_day_sat_reception"),
        F.col("high_seas"), F.col("mpa"), F.col("mpa_no_take"),
        F.col("_source_file"), F.col("_bronze_ingest_ts"),
        F.current_timestamp().alias("_silver_load_ts"),
        F.lit(run_id).alias("_run_id"))


def build_rejects(rejected, run_id):
    rid = F.sha2(F.concat_ws("|", F.lit("gap"), F.coalesce(F.col("event_id"), F.lit("")),
                             F.col("reject_reason"), F.col("_source_file"), F.col("raw_json")), 256)
    return rejected.select(
        rid.alias("reject_id"), F.lit("gap").alias("entity"), F.col("reject_reason"),
        F.col("raw_json"), F.col("_source_file"), F.col("_bronze_ingest_ts"),
        F.current_timestamp().alias("_silver_load_ts"), F.lit(run_id).alias("_run_id"))

# COMMAND ----------

def main():
    from delta.tables import DeltaTable

    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("storage", "stmaritimeks")
    dbutils.widgets.dropdown("dry_run", "true", ["true", "false"])
    run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
    storage = dbutils.widgets.get("storage")
    dry_run = dbutils.widgets.get("dry_run") == "false"

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    path = f"abfss://bronze@{storage}.dfs.core.windows.net/gfw/events/gap/*/*/*.jsonl"

    lines = read_bronze(spark, path)
    rows_in = lines.count()
    if rows_in == 0:
        raise Exception("No Bronze gap files found at " + path +
                        ". Ask Member 2 to run Bronze ingestion for GAP events first.")

    cleaned = flatten_and_clean(lines)
    deduped = dedupe(cleaned).cache()
    unique_ids = deduped.count()
    dup_removed = rows_in - unique_ids

    flagged = add_reject_reason(deduped)
    valid = flagged.filter("reject_reason IS NULL")
    rejected = flagged.filter("reject_reason IS NOT NULL")
    silver = build_silver(valid, run_id).cache()
    rejects = build_rejects(rejected, run_id)
    n_silver, n_rej = silver.count(), rejects.count()

    stats = {
        "run_id": run_id, "rows_in_bronze": rows_in, "duplicates_removed": dup_removed,
        "unique_after_dedupe": unique_ids, "rejects": n_rej, "rows_out_silver": n_silver,
        "reconciled (unique ids = silver + rejects)": unique_ids == n_silver + n_rej,
        "vessel_name set to Unknown": deduped.filter("_name_was_missing").count(),
        "missing intentional_disabling -> FALSE": deduped.filter("intentional_disabling_raw IS NULL").count(),
        "missing vessel_flag (kept NULL)": silver.filter("vessel_flag IS NULL").count(),
    }
    print("=== stg_ais_gaps run summary ===")
    for k, v in stats.items():
        print(f"{k}: {v}")

    if dry_run:
        print("\nDRY RUN: nothing written. Sample rows:")
        display(silver.limit(10))
        return stats

    (DeltaTable.forName(spark, "project_staging.stg_ais_gaps").alias("t")
        .merge(silver.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    if n_rej > 0:
        (DeltaTable.forName(spark, "project_staging.stg_rejects").alias("t")
            .merge(rejects.alias("s"), "t.reject_id = s.reject_id")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    final = spark.table("project_staging.stg_ais_gaps").count()
    print(f"\nWRITTEN. project_staging.stg_ais_gaps now has {final} rows.")
    return stats


if "dbutils" in globals():      # runs in Databricks; skipped when imported for tests
    main()