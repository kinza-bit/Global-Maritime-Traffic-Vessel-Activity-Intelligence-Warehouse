"""Explicit schemas for the Silver layer (Member 3).

Version 2: corrected against real Bronze data (2025-01..2025-03).
Rules:
  * Never use inferSchema anywhere in the repo.
  * Bronze read schemas read every scalar as STRING (some numerics arrive as
    JSON strings, e.g. port_visit.confidence). Casting happens in Silver via
    transform_utils. Fields not listed here are ignored by the JSON reader.
  * regions.* are ARRAYS of strings, e.g. highSeas = ["high_seas"] or [].
  * Silver types follow README 11.2.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    ArrayType,
    TimestampType,
    DateType,
    IntegerType,
    BooleanType,   
)

S = StringType()
D2 = DecimalType(10, 2)
D6 = DecimalType(10, 6)


def _f(name, dtype=S, nullable=True):
    return StructField(name, dtype, nullable)


# ---------------------------------------------------------------------------
# (a) BRONZE READ SCHEMAS
# ---------------------------------------------------------------------------
_VESSEL_REF = StructType([_f("id"), _f("name"), _f("ssvid"), _f("flag"), _f("type")])
_POSITION = StructType([_f("lat"), _f("lon")])
_REGIONS = StructType([
    _f("eez", ArrayType(S)),
    _f("highSeas", ArrayType(S)),
    _f("mpa", ArrayType(S)),
    _f("mpaNoTake", ArrayType(S)),
])
_DISTANCES = StructType([
    _f("startDistanceFromShoreKm"), _f("endDistanceFromShoreKm"),
    _f("startDistanceFromPortKm"), _f("endDistanceFromPortKm"),
])
_ANCHORAGE = StructType([
    _f("id"), _f("name"), _f("flag"), _f("lat"), _f("lon"),
    _f("atDock"), _f("distanceFromShoreKm"),
])

_COMMON_EVENT_FIELDS = [
    _f("id"), _f("type"), _f("start"), _f("end"),
    _f("vessel", _VESSEL_REF),
    _f("position", _POSITION),
    _f("regions", _REGIONS),
    _f("distances", _DISTANCES),
]

BRONZE_PORT_VISIT_SCHEMA = StructType(_COMMON_EVENT_FIELDS + [
    _f("port_visit", StructType([
        _f("visitId"), _f("confidence"), _f("durationHrs"),
        _f("startAnchorage", _ANCHORAGE),
        _f("endAnchorage", _ANCHORAGE),
    ])),
])

BRONZE_AIS_GAP_SCHEMA = StructType(_COMMON_EVENT_FIELDS + [
    _f("gap", StructType([
        _f("durationHours"),
        _f("distanceKm"),             # STRING in Bronze
        _f("impliedSpeedKnots"),      # STRING in Bronze
        _f("intentionalDisabling"),
        _f("offPosition", _POSITION),
        _f("onPosition", _POSITION),
        _f("positionsPerDaySatReception"),
    ])),
])
# One read schema for FISHING, LOITERING and ENCOUNTER; non-matching structs
# come back NULL.
BRONZE_VESSEL_EVENT_SCHEMA = StructType(_COMMON_EVENT_FIELDS + [
    _f("fishing", StructType([
        _f("totalDistanceKm"), _f("averageSpeedKnots"),
        _f("averageDurationHours"),          # always NULL in real data
        _f("potentialRisk"),
    ])),
    _f("loitering", StructType([
        _f("totalTimeHours"), _f("totalDistanceKm"),
        _f("averageSpeedKnots"), _f("averageDistanceFromShoreKm"),
    ])),
    _f("encounter", StructType([
        _f("type"), _f("medianDistanceKilometers"), _f("medianSpeedKnots"),
        _f("potentialRisk"),
        _f("vessel", _VESSEL_REF),
    ])),
])

# ---------------------------------------------------------------------------
# (b) SILVER TARGET SCHEMAS
# ---------------------------------------------------------------------------
AUDIT_COLUMNS = [
    _f("_source_file", S, False),
    _f("_bronze_ingest_ts", TimestampType()),
    _f("_silver_load_ts", TimestampType(), False),
    _f("_run_id", S, False),
]

STG_PORT_VISITS_SCHEMA = StructType([
    _f("event_id", S, False),
    _f("visit_id"),
    _f("vessel_id", S, False),
    _f("vessel_name"),                 # NULL/empty -> 'Unknown'
    _f("ssvid"),
    _f("vessel_flag"),                 # UPPER; stays NULL when missing
    _f("vessel_type"),                 # UPPER
    _f("start_ts", TimestampType(), False),
    _f("end_ts", TimestampType(), False),
    _f("start_date", DateType()),
    _f("duration_hrs", D2),
    _f("confidence", IntegerType()),   # categorical 2 | 3 | 4
    _f("lat", D6),
    _f("lon", D6),
    _f("start_anchorage_name"),
    _f("end_anchorage_name"),          # ~9% NULL in source (DQ-I02, W)
    _f("end_anchorage_flag"),
    _f("end_anchorage_lat", D6),
    _f("end_anchorage_lon", D6),
    _f("end_at_dock", BooleanType()),
    _f("start_dist_port_km", D2),
    _f("end_dist_port_km", D2),
    _f("start_dist_shore_km", D2),
    _f("end_dist_shore_km", D2),
    _f("high_seas", BooleanType()),    # size(regions.highSeas) > 0
] + AUDIT_COLUMNS)

STG_VESSEL_EVENTS_SCHEMA = StructType([
    _f("event_id", S, False),
    _f("event_base_id"),               # id without the trailing .1/.2 (encounters are stored twice)
    _f("event_type", S, False),        # FISHING | LOITERING | ENCOUNTER
    _f("vessel_id", S, False),
    _f("vessel_name"),
    _f("ssvid"),
    _f("vessel_flag"),
    _f("vessel_type"),
    _f("start_ts", TimestampType(), False),
    _f("end_ts", TimestampType(), False),
    _f("start_date", DateType()),
    _f("lat", D6),
    _f("lon", D6),
    # loitering: totalTimeHours; fishing/encounter: derived (end_ts - start_ts)
    _f("duration_hours", D2),
    _f("avg_speed_knots", D2),         # fishing, loitering only
    _f("total_distance_km", D2),       # fishing, loitering only
    _f("avg_dist_shore_km", D2),       # loitering only
    _f("potential_risk", BooleanType()),
    _f("encounter_type"),              # UPPER, encounters only
    _f("encounter_median_speed_knots", D2),
    _f("encounter_median_distance_km", D2),   # distance BETWEEN the two vessels
    _f("encountered_vessel_id"),
    _f("encountered_vessel_name"),
    _f("encountered_vessel_flag"),
    _f("encountered_vessel_type"),
    _f("start_dist_shore_km", D2),
    _f("end_dist_shore_km", D2),
    _f("start_dist_port_km", D2),
    _f("end_dist_port_km", D2),
    _f("high_seas", BooleanType()),    # size(regions.highSeas) > 0
    _f("mpa", BooleanType()),          # size(regions.mpa) > 0
    _f("mpa_no_take", BooleanType()),  # size(regions.mpaNoTake) > 0
] + AUDIT_COLUMNS)

STG_AIS_GAPS_SCHEMA = StructType([
    _f("event_id", S, False),
    _f("vessel_id", S, False),
    _f("vessel_name"),
    _f("ssvid"),
    _f("vessel_flag"),
    _f("vessel_type"),
    _f("start_ts", TimestampType(), False),
    _f("end_ts", TimestampType(), False),
    _f("start_date", DateType()),
    _f("duration_hours", D2),
    _f("distance_km", D2),
    _f("implied_speed_knots", D2),
    _f("intentional_disabling", BooleanType()),
    _f("off_lat", D6),
    _f("off_lon", D6),
    _f("on_lat", D6),
    _f("on_lon", D6),
    _f("positions_per_day_sat_reception", D2),
    _f("high_seas", BooleanType()),
    _f("mpa", BooleanType()),
    _f("mpa_no_take", BooleanType()),
] + AUDIT_COLUMNS)

STG_REJECTS_SCHEMA = StructType([
    _f("reject_id", S, False),
    _f("entity", S, False),            # port_visit | fishing | loitering | encounter
    _f("reject_reason", S, False),     # e.g. "DQ-V02 end_ts < start_ts"
    _f("raw_json"),
] + AUDIT_COLUMNS)

STG_VESSELS_SCHEMA = StructType([
    StructField("vessel_id", StringType(), False),
    StructField("vessel_name", StringType(), True),
    StructField("ssvid", StringType(), True),
    StructField("imo", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("vessel_flag", StringType(), True),
    StructField("length_m", DecimalType(10, 2), True),
    StructField("tonnage_gt", DecimalType(10, 2), True),
    StructField("engine_power_kw", DecimalType(10, 2), True),
    StructField("max_speed_kn", DecimalType(10, 2), True),
    StructField("ship_types", ArrayType(StringType()), True),
    StructField("gear_types", ArrayType(StringType()), True),
    StructField("_source_file", StringType(), True),
    StructField("_bronze_ingest_ts", TimestampType(), True),
    StructField("_silver_load_ts", TimestampType(), True),
    StructField("_run_id", StringType(), True),
])

SILVER_SCHEMAS = {
    "stg_port_visits": STG_PORT_VISITS_SCHEMA,
    "stg_vessel_events": STG_VESSEL_EVENTS_SCHEMA,
    "stg_rejects": STG_REJECTS_SCHEMA,
    "stg_vessels": STG_VESSELS_SCHEMA,
    "stg_ais_gaps": STG_AIS_GAPS_SCHEMA,
}

# MERGE keys
SILVER_KEYS = {
    "stg_port_visits": ["event_id"],
    "stg_vessel_events": ["event_id"],
    "stg_rejects": ["reject_id"],
    "stg_vessels": ["vessel_id"],
    "stg_ais_gaps": ["event_id"],
}

