"""Member 3 - Silver port visits: pure transform logic.

No dbutils, no abfss paths, no MERGE. Runs locally under pytest and is
called by silver/20_stg_port_visits.py in Databricks.
"""
from pyspark.sql import functions as F

from schemas.silver_schemas import BRONZE_PORT_VISIT_SCHEMA
from silver.transform_utils import (
    clean_text,
    clean_vessel_name,
    to_timestamp_utc,
    to_decimal,
    to_bool,
    array_flag,
    dedupe_latest,
    split_valid_rejects,
    is_unparseable,
)


def _rule(name, condition):
    """A NULL condition (e.g. duration < 0 when duration is NULL) counts as 'not failed'."""
    return (name, F.coalesce(condition, F.lit(False)))


def flatten_port_visits(raw):
    """raw needs _raw_json and _source_file; _bronze_ingest_ts is optional."""
    if "_bronze_ingest_ts" not in raw.columns:
        raw = raw.withColumn("_bronze_ingest_ts", F.lit(None).cast("timestamp"))

    parsed = raw.withColumn(
        "_record", F.from_json(F.col("_raw_json"), BRONZE_PORT_VISIT_SCHEMA)
    )

    flat = parsed.select(
        F.col("_source_file"),
        F.col("_raw_json"),
        F.col("_bronze_ingest_ts"),
        F.col("_record"),  # kept so we can detect malformed JSON
        clean_text("_record.id").alias("event_id"),
        clean_text("_record.port_visit.visitId").alias("visit_id"),
        clean_text("_record.vessel.id").alias("vessel_id"),
        clean_vessel_name("_record.vessel.name").alias("vessel_name"),
        clean_text("_record.vessel.ssvid").alias("ssvid"),
        clean_text("_record.vessel.flag", "upper").alias("vessel_flag"),
        clean_text("_record.vessel.type", "upper").alias("vessel_type"),
        to_timestamp_utc("_record.start").alias("start_ts"),
        to_timestamp_utc("_record.end").alias("end_ts"),
        to_decimal("_record.port_visit.durationHrs").alias("duration_hrs"),
        F.expr("try_cast(_record.port_visit.confidence AS INT)").alias("confidence"),
        to_decimal("_record.position.lat", 10, 6).alias("lat"),
        to_decimal("_record.position.lon", 10, 6).alias("lon"),
        clean_text("_record.port_visit.startAnchorage.name", "title").alias("start_anchorage_name"),
        clean_text("_record.port_visit.endAnchorage.name", "title").alias("end_anchorage_name"),
        clean_text("_record.port_visit.endAnchorage.flag", "upper").alias("end_anchorage_flag"),
        to_decimal("_record.port_visit.endAnchorage.lat", 10, 6).alias("end_anchorage_lat"),
        to_decimal("_record.port_visit.endAnchorage.lon", 10, 6).alias("end_anchorage_lon"),
        to_bool("_record.port_visit.endAnchorage.atDock", default=False).alias("end_at_dock"),
        to_decimal("_record.distances.startDistanceFromPortKm").alias("start_dist_port_km"),
        to_decimal("_record.distances.endDistanceFromPortKm").alias("end_dist_port_km"),
        to_decimal("_record.distances.startDistanceFromShoreKm").alias("start_dist_shore_km"),
        to_decimal("_record.distances.endDistanceFromShoreKm").alias("end_dist_shore_km"),
        array_flag("_record.regions.highSeas").alias("high_seas"),
    )
    return flat.withColumn("start_date", F.to_date("start_ts"))


def build_rules():
    return [
        _rule("MALFORMED_JSON",F.get_json_object(F.col("_raw_json"), "$").isNull() | F.col("_record").isNull(),),  # must stay first,  # must stay first
        _rule("DQ-C01 missing event_id", F.col("event_id").isNull()),
        _rule("DQ-C02 missing vessel_id", F.col("vessel_id").isNull()),
        _rule("DQ-C03 missing start_ts", F.col("start_ts").isNull()),
        _rule("Missing duration_hrs", F.col("duration_hrs").isNull()),
        _rule("Missing end_ts (target NOT NULL)", F.col("end_ts").isNull()),
        _rule("DQ-V01 negative duration", F.col("duration_hrs") < 0),
        _rule("DQ-V02 end_ts before start_ts", F.col("end_ts") < F.col("start_ts")),
        _rule(
            "DQ-V03 latitude outside [-90, 90]",
            F.col("lat").isNotNull() & ((F.col("lat") < -90) | (F.col("lat") > 90)),
        ),
        _rule(
            "DQ-V03 longitude outside [-180, 180]",
            F.col("lon").isNotNull() & ((F.col("lon") < -180) | (F.col("lon") > 180)),
        ),
    ]


def transform_port_visits(raw):
    """Returns (valid, rejects, counts).

    Rows with a NULL event_id are NOT deduplicated (they would collapse into
    one row and silently break reconciliation). They go straight to the
    reject split and are kept as separate rows.
    """
    flat = flatten_port_visits(raw)

    quality = flat.agg(
        F.count(F.lit(1)).alias("rows_in"),
        F.countDistinct("event_id").alias("unique_keyed"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("unkeyed"),
        F.sum((F.col("vessel_name") == "Unknown").cast("int")).alias("unknown_vessel_names"),
        F.sum(F.col("vessel_flag").isNull().cast("int")).alias("missing_vessel_flags"),
        F.sum(F.col("end_anchorage_name").isNull().cast("int")).alias("missing_end_anchorage_names"),
        F.sum(F.col("duration_hrs").isNull().cast("int")).alias("missing_durations"),
        F.sum(
            is_unparseable("_record.port_visit.durationHrs", "duration_hrs").cast("int")
        ).alias("unparseable_durations"),
    ).first().asDict()

    keyed = flat.filter(F.col("event_id").isNotNull())
    unkeyed = flat.filter(F.col("event_id").isNull())

    deduped_keyed = dedupe_latest(keyed, key_cols=["event_id"], source_col="_source_file")
    deduped = deduped_keyed.unionByName(unkeyed, allowMissingColumns=True)

    valid, rejects = split_valid_rejects(
        deduped,
        rules=build_rules(),
        entity="port_visit",
        raw_json_col="_raw_json",
        source_col="_source_file",
    )

    counts = dict(quality)
    counts["unkeyed"] = counts["unkeyed"] or 0
    counts["valid"] = valid.count()
    counts["rejects"] = rejects.count()
    counts["rows_after_dedupe"] = counts["unique_keyed"] + counts["unkeyed"]
    counts["duplicates_removed"] = (
        counts["rows_in"] - counts["unkeyed"] - counts["unique_keyed"]
    )
    counts["reconciled"] = counts["rows_after_dedupe"] == counts["valid"] + counts["rejects"]
    return valid, rejects, counts