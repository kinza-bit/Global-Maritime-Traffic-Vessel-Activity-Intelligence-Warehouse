"""Pure Silver transformation logic for fishing, loitering and encounter events.

No Databricks utilities, storage paths, widgets, or Delta MERGE operations.
Follows the helper contract in silver/transform_utils.py and the target
STG_VESSEL_EVENTS_SCHEMA in schemas/silver_schemas.py.

Business-rule defaults:
* Keep encounter .1 and .2 as separate rows; event_id is the key.
* Retain valid pre-2025 records; no date cutoff is applied.
* Fishing duration uses elapsed timestamps because the observed source field
  averageDurationHours is NULL. If a valid source duration is supplied later,
  it is preferred.
* Loitering prefers totalTimeHours, falling back to elapsed timestamps.
* Encounter duration is elapsed timestamps.
* max_duration_hours is optional until the team supplies its configured DQ
  threshold. No arbitrary maximum is hard-coded.

Input DataFrame:
* _raw_json: one Bronze JSON object per row
* _source_file: Bronze source path
* _bronze_ingest_ts: optional; if absent it is added as NULL
"""
from pyspark.sql import functions as F

from schemas.silver_schemas import BRONZE_VESSEL_EVENT_SCHEMA
from silver.transform_utils import (
    array_flag,
    clean_text,
    clean_vessel_name,
    dedupe_latest,
    event_base_id as make_event_base_id,
    hours_between,
    is_unparseable,
    split_valid_rejects,
    to_bool,
    to_decimal,
    to_timestamp_utc,
)


_EVENT_TYPES = ("FISHING", "LOITERING", "ENCOUNTER")


def _rule(name, condition):
    """A NULL rule result is treated as not-failed, matching port-visits logic."""
    return name, F.coalesce(condition, F.lit(False))


def flatten_vessel_events(raw):
    """Parse and flatten the three Bronze event types into Silver columns."""
    required = {"_raw_json", "_source_file"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(
            "raw DataFrame missing required columns: " + ", ".join(sorted(missing))
        )

    if "_bronze_ingest_ts" not in raw.columns:
        raw = raw.withColumn("_bronze_ingest_ts", F.lit(None).cast("timestamp"))

    parsed = raw.withColumn(
        "_record", F.from_json(F.col("_raw_json"), BRONZE_VESSEL_EVENT_SCHEMA)
    )

    event_id = clean_text("_record.id")
    event_type = clean_text("_record.type", "upper")
    start_ts = to_timestamp_utc("_record.start")
    end_ts = to_timestamp_utc("_record.end")
    elapsed_duration = hours_between(start_ts, end_ts)

    fishing_source_duration = to_decimal("_record.fishing.averageDurationHours")
    loitering_source_duration = to_decimal("_record.loitering.totalTimeHours")

    flat = parsed.select(
        F.col("_source_file"),
        F.col("_raw_json"),
        F.col("_bronze_ingest_ts"),
        F.col("_record"),
        event_id.alias("event_id"),
        F.when(
            event_type == "ENCOUNTER", make_event_base_id(event_id)
        ).otherwise(event_id).alias("event_base_id"),
        event_type.alias("event_type"),
        clean_text("_record.vessel.id").alias("vessel_id"),
        clean_vessel_name("_record.vessel.name").alias("vessel_name"),
        clean_text("_record.vessel.ssvid").alias("ssvid"),
        clean_text("_record.vessel.flag", "upper").alias("vessel_flag"),
        clean_text("_record.vessel.type", "upper").alias("vessel_type"),
        start_ts.alias("start_ts"),
        end_ts.alias("end_ts"),
        F.to_date(start_ts).alias("start_date"),
        to_decimal("_record.position.lat", 10, 6).alias("lat"),
        to_decimal("_record.position.lon", 10, 6).alias("lon"),
        F.when(
            event_type == "FISHING",
            F.coalesce(fishing_source_duration, elapsed_duration),
        ).when(
            event_type == "LOITERING",
            F.coalesce(loitering_source_duration, elapsed_duration),
        ).when(
            event_type == "ENCOUNTER", elapsed_duration
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias("duration_hours"),
        F.when(
            event_type == "FISHING",
            to_decimal("_record.fishing.averageSpeedKnots"),
        ).when(
            event_type == "LOITERING",
            to_decimal("_record.loitering.averageSpeedKnots"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias("avg_speed_knots"),
        F.when(
            event_type == "FISHING",
            to_decimal("_record.fishing.totalDistanceKm"),
        ).when(
            event_type == "LOITERING",
            to_decimal("_record.loitering.totalDistanceKm"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias("total_distance_km"),
        F.when(
            event_type == "LOITERING",
            to_decimal("_record.loitering.averageDistanceFromShoreKm"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias("avg_dist_shore_km"),
        F.when(
            event_type == "FISHING",
            to_bool("_record.fishing.potentialRisk", default=None),
        ).when(
            event_type == "ENCOUNTER",
            to_bool("_record.encounter.potentialRisk", default=None),
        ).otherwise(F.lit(None).cast("boolean")).alias("potential_risk"),
        F.when(
            event_type == "ENCOUNTER",
            clean_text("_record.encounter.type", "upper"),
        ).otherwise(F.lit(None).cast("string")).alias("encounter_type"),
        F.when(
            event_type == "ENCOUNTER",
            to_decimal("_record.encounter.medianSpeedKnots"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias(
            "encounter_median_speed_knots"
        ),
        F.when(
            event_type == "ENCOUNTER",
            to_decimal("_record.encounter.medianDistanceKilometers"),
        ).otherwise(F.lit(None).cast("decimal(10,2)")).alias(
            "encounter_median_distance_km"
        ),
        F.when(
            event_type == "ENCOUNTER",
            clean_text("_record.encounter.vessel.id"),
        ).otherwise(F.lit(None).cast("string")).alias("encountered_vessel_id"),
        F.when(
            event_type == "ENCOUNTER",
            clean_vessel_name("_record.encounter.vessel.name"),
        ).otherwise(F.lit(None).cast("string")).alias("encountered_vessel_name"),
        F.when(
            event_type == "ENCOUNTER",
            clean_text("_record.encounter.vessel.flag", "upper"),
        ).otherwise(F.lit(None).cast("string")).alias("encountered_vessel_flag"),
        F.when(
            event_type == "ENCOUNTER",
            clean_text("_record.encounter.vessel.type", "upper"),
        ).otherwise(F.lit(None).cast("string")).alias("encountered_vessel_type"),
        to_decimal("_record.distances.startDistanceFromShoreKm").alias(
            "start_dist_shore_km"
        ),
        to_decimal("_record.distances.endDistanceFromShoreKm").alias(
            "end_dist_shore_km"
        ),
        to_decimal("_record.distances.startDistanceFromPortKm").alias(
            "start_dist_port_km"
        ),
        to_decimal("_record.distances.endDistanceFromPortKm").alias(
            "end_dist_port_km"
        ),
        array_flag("_record.regions.highSeas").alias("high_seas"),
        array_flag("_record.regions.mpa").alias("mpa"),
        array_flag("_record.regions.mpaNoTake").alias("mpa_no_take"),
    )
    return flat


def build_rules(max_duration_hours=None):
    """Build reject rules; optional maximum must come from agreed DQ config."""
    rules = [
        _rule(
            "MALFORMED_JSON",
            F.get_json_object(F.col("_raw_json"), "$").isNull()
            | F.col("_record").isNull(),
        ),
        _rule("DQ-C01 missing event_id", F.col("event_id").isNull()),
        _rule("DQ-C02 missing vessel_id", F.col("vessel_id").isNull()),
        _rule("DQ-C03 missing start_ts", F.col("start_ts").isNull()),
        _rule("DQ-C04 missing end_ts", F.col("end_ts").isNull()),
        _rule("DQ-V04 invalid event_type", F.col("event_type").isNull() | ~F.col("event_type").isin(*_EVENT_TYPES)),
        _rule("DQ-V01 missing duration", F.col("duration_hours").isNull()),
        _rule("DQ-V02 negative duration", F.col("duration_hours") < 0),
        _rule("DQ-V03 end_ts before start_ts", F.col("end_ts") < F.col("start_ts")),
        _rule(
            "DQ-V05 latitude outside [-90, 90]",
            F.col("lat").isNotNull()
            & ((F.col("lat") < -90) | (F.col("lat") > 90)),
        ),
        _rule(
            "DQ-V06 longitude outside [-180, 180]",
            F.col("lon").isNotNull()
            & ((F.col("lon") < -180) | (F.col("lon") > 180)),
        ),
    ]
    if max_duration_hours is not None:
        rules.append(
            _rule(
                f"DQ-V07 duration exceeds configured maximum ({max_duration_hours} hours)",
                F.col("duration_hours") > F.lit(max_duration_hours),
            )
        )
    return rules


def _reject_entity_from_path(rejects):
    """Classify rejects by event type inferred from their Bronze folder."""
    path = F.lower(F.coalesce(F.col("_source_file"), F.lit("")))
    return (
        rejects.withColumn(
            "entity",
            F.when(path.contains("/fishing/"), F.lit("fishing"))
            .when(path.contains("/loitering/"), F.lit("loitering"))
            .when(path.contains("/encounter/"), F.lit("encounter"))
            .otherwise(F.lit("unknown")),
        )
        # Recompute the deterministic ID using the final entity value.
        .withColumn(
            "reject_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("entity"),
                    F.col("reject_reason"),
                    F.col("raw_json"),
                ),
                256,
            ),
        )
    )


def transform_vessel_events(raw, max_duration_hours=None):
    """Return (valid, rejects, counts), consistent with transform_port_visits."""
    flat = flatten_vessel_events(raw)

    quality = flat.agg(
        F.count(F.lit(1)).alias("rows_in"),
        F.countDistinct("event_id").alias("unique_keyed"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("unkeyed"),
        F.sum((F.col("vessel_name") == "Unknown").cast("int")).alias(
            "unknown_vessel_names"
        ),
        F.sum(F.col("vessel_flag").isNull().cast("int")).alias(
            "missing_vessel_flags"
        ),
        F.sum(F.col("duration_hours").isNull().cast("int")).alias(
            "missing_durations"
        ),
        F.sum(
            F.when(
                F.col("event_type") == "FISHING",
                is_unparseable(
                    "_record.fishing.averageDurationHours",
                    to_decimal("_record.fishing.averageDurationHours"),
                ).cast("int"),
            )
            .when(
                F.col("event_type") == "LOITERING",
                is_unparseable(
                    "_record.loitering.totalTimeHours",
                    to_decimal("_record.loitering.totalTimeHours"),
                ).cast("int"),
            )
            .otherwise(F.lit(0))
        ).alias("unparseable_source_durations"),
        F.sum((F.col("event_type") == "FISHING").cast("int")).alias(
            "fishing_rows"
        ),
        F.sum((F.col("event_type") == "LOITERING").cast("int")).alias(
            "loitering_rows"
        ),
        F.sum((F.col("event_type") == "ENCOUNTER").cast("int")).alias(
            "encounter_rows"
        ),
        F.sum(
            (
                (F.col("event_type") == "ENCOUNTER")
                & F.col("event_id").rlike(r"\.[12]$")
            ).cast("int")
        ).alias("suffixed_encounter_rows"),
    ).first().asDict()

    keyed = flat.filter(F.col("event_id").isNotNull())
    unkeyed = flat.filter(F.col("event_id").isNull())
    deduped_keyed = dedupe_latest(
        keyed, key_cols=["event_id"], source_col="_source_file"
    )
    deduped = deduped_keyed.unionByName(unkeyed, allowMissingColumns=True)
    valid, rejects = split_valid_rejects(
        deduped,
        rules=build_rules(max_duration_hours=max_duration_hours),
        entity="vessel_event",
        raw_json_col="_raw_json",
        source_col="_source_file",
    )
    rejects = _reject_entity_from_path(rejects)

    target_columns = [
        "event_id", "event_base_id", "event_type", "vessel_id", "vessel_name",
        "ssvid", "vessel_flag", "vessel_type", "start_ts", "end_ts", "start_date",
        "lat", "lon", "duration_hours", "avg_speed_knots", "total_distance_km",
        "avg_dist_shore_km", "potential_risk", "encounter_type",
        "encounter_median_speed_knots", "encounter_median_distance_km",
        "encountered_vessel_id", "encountered_vessel_name",
        "encountered_vessel_flag", "encountered_vessel_type",
        "start_dist_shore_km", "end_dist_shore_km", "start_dist_port_km",
        "end_dist_port_km", "high_seas", "mpa", "mpa_no_take",
    ]
    valid = valid.select(
        *target_columns, "_source_file", "_bronze_ingest_ts"
    )

    counts = dict(quality)
    counts["unkeyed"] = counts.get("unkeyed") or 0
    counts["valid"] = valid.count()
    counts["rejects"] = rejects.count()
    counts["rows_after_dedupe"] = (
        (counts.get("unique_keyed") or 0) + counts["unkeyed"]
    )
    counts["duplicates_removed"] = (
        (counts.get("rows_in") or 0)
        - counts["unkeyed"]
        - (counts.get("unique_keyed") or 0)
    )
    counts["reconciled"] = (
        counts["rows_after_dedupe"] == counts["valid"] + counts["rejects"]
    )
    return valid, rejects, counts
