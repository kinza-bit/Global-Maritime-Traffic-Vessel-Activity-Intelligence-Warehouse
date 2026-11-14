"""Tests for silver/vessel_events_logic.py.

Run from the repository root:
    python -m pytest -q tests/test_vessel_events_logic.py

Requires the repository's PySpark dependency and existing schemas/helpers.
"""
import json
import os
import sys

import pytest
from pyspark.sql import SparkSession


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from silver.vessel_events_logic import transform_vessel_events  # noqa: E402


FISHING_PATH = "gfw/events/fishing/load_type=initial/ingest_date=2025-01-05/sample.jsonl"
LOITERING_PATH = "gfw/events/loitering/load_type=initial/ingest_date=2025-01-05/sample.jsonl"
ENCOUNTER_PATH = "gfw/events/encounter/load_type=initial/ingest_date=2025-01-05/sample.jsonl"





@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def fishing_record(
    event_id="fish-1",
    start="2025-01-28T07:55:18.000Z",
    end="2025-01-28T09:55:18.000Z",
    vessel_id="v-fish",
    duration=None,
    lat=8.4448,
):
    return {
        "id": event_id,
        "type": "fishing",
        "start": start,
        "end": end,
        "position": {"lat": lat, "lon": -31.5893},
        "regions": {"highSeas": ["high_seas"], "mpa": [], "mpaNoTake": []},
        "distances": {
            "startDistanceFromShoreKm": 902,
            "endDistanceFromShoreKm": 1175,
            "startDistanceFromPortKm": 928.08,
            "endDistanceFromPortKm": 1204.41,
        },
        "vessel": {
            "id": vessel_id,
            "name": " TAIKO MARU NO.8 ",
            "ssvid": "432971000",
            "flag": "jpn",
            "type": "fishing",
        },
        "fishing": {
            "totalDistanceKm": 8032.77,
            "averageSpeedKnots": 5.14,
            "averageDurationHours": duration,
            "potentialRisk": False,
        },
    }

def loitering_record(duration=2.0):
    return {
        "id": "loiter-1",
        "type": "LOITERING",
        "start": "2024-06-01T10:00:00Z",
        "end": "2024-06-01T12:00:00Z",
        "vessel": {
            "id": "vessel-001",
            "name": "Test Vessel",
            "ssvid": "123456789",
            "flag": "PA",
            "type": "Fishing",
        },
        "position": {
            "lat": 24.86,
            "lon": 67.01,
        },
        "loitering": {
            "totalTimeHours": duration,
            "totalDistanceKm": 10.0,
            "averageSpeedKnots": 2.7,
            "averageDistanceFromShoreKm": 5.0,
        },
    }


def encounter_record(
    event_id="enc-1.1",
    start="2024-12-24T00:00:00Z",
    end="2025-01-01T00:00:00Z",
):
    return {
        "id": event_id,
        "type": "encounter",
        "start": start,
        "end": end,
        "position": {"lat": -0.8825, "lon": -112.2761},
        "regions": {"highSeas": ["high_seas"], "mpa": [], "mpaNoTake": []},
        "distances": {
            "startDistanceFromShoreKm": 1284,
            "endDistanceFromShoreKm": 1284,
            "startDistanceFromPortKm": 2019.55,
            "endDistanceFromPortKm": 2019.55,
        },
        "vessel": {
            "id": "v-main", "name": "FUYUANYU7635",
            "ssvid": "412440154", "flag": "chn", "type": "fishing",
        },
        "encounter": {
            "vessel": {
                "id": "v-other", "name": "FU YUAN YU7635",
                "ssvid": "412440150", "flag": "chn", "type": "fishing",
            },
            "medianDistanceKilometers": 0.012,
            "medianSpeedKnots": 1.224,
            "type": "fishing-fishing",
            "potentialRisk": False,
        },
    }


def raw_df(spark, records):
    return spark.createDataFrame(
        [(json.dumps(record), path) for record, path in records],
        ["_raw_json", "_source_file"],
    )


def test_transforms_all_three_event_types(spark):
    raw = raw_df(spark, [
        (fishing_record(), FISHING_PATH),
        (loitering_record(), LOITERING_PATH),
        (encounter_record(), ENCOUNTER_PATH),
    ])
    valid, rejects, counts = transform_vessel_events(raw)
    rows = {row.event_id: row for row in valid.collect()}
    print("VALID EVENT IDS:", sorted(rows.keys()))
    print("REJECT COUNT:", rejects.count())
    print("REJECTS:")
    rejects.select("reject_reason", "raw_json").show(truncate=False)
    print("COUNTS:", counts)

    assert set(rows) == {"fish-1", "loiter-1", "enc-1.1"}
    assert counts["valid"] == 3
    assert counts["rejects"] == 0
    assert counts["reconciled"] is True
    assert rows["fish-1"].event_type == "FISHING"
    assert rows["loiter-1"].event_type == "LOITERING"
    assert rows["enc-1.1"].event_type == "ENCOUNTER"


def test_fishing_duration_falls_back_to_elapsed_time(spark):
    raw = raw_df(spark, [(fishing_record(duration=None), FISHING_PATH)])
    valid, _, _ = transform_vessel_events(raw)
    row = valid.first()
    assert float(row.duration_hours) == 2.0


def test_loitering_prefers_source_duration(spark):
    raw = raw_df(spark, [(loitering_record(duration=116.5), LOITERING_PATH)])
    valid, _, _ = transform_vessel_events(raw)
    assert float(valid.first().duration_hours) == 116.5


def test_encounter_suffixes_are_retained_as_separate_events(spark):
    raw = raw_df(spark, [
        (encounter_record("enc-base.1"), ENCOUNTER_PATH),
        (encounter_record("enc-base.2"), ENCOUNTER_PATH),
    ])
    valid, _, counts = transform_vessel_events(raw)
    rows = {row.event_id: row for row in valid.collect()}

    assert set(rows) == {"enc-base.1", "enc-base.2"}
    assert rows["enc-base.1"].event_base_id == "enc-base"
    assert rows["enc-base.2"].event_base_id == "enc-base"
    assert counts["duplicates_removed"] == 0


def test_pre_2025_event_is_not_filtered_out(spark):
    raw = raw_df(spark, [
        (encounter_record("old-event.1", start="2024-12-24T00:00:00Z"), ENCOUNTER_PATH)
    ])
    valid, _, _ = transform_vessel_events(raw)
    assert valid.count() == 1
    assert str(valid.first().start_date) == "2024-12-24"


def test_duplicate_event_id_keeps_one_record_and_reconciles(spark):
    older = fishing_record(event_id="same-id")
    newer = fishing_record(event_id="same-id")
    newer["fishing"]["totalDistanceKm"] = 9999
    raw = raw_df(spark, [
        (older, FISHING_PATH.replace("initial", "initial")),
        (newer, FISHING_PATH.replace("initial", "incremental")),
    ])
    valid, _, counts = transform_vessel_events(raw)

    assert valid.count() == 1
    assert counts["duplicates_removed"] == 1
    assert counts["reconciled"] is True


def test_invalid_coordinates_are_rejected(spark):
    raw = raw_df(spark, [
        (fishing_record(event_id="good"), FISHING_PATH),
        (fishing_record(event_id="bad-lat", lat=95), FISHING_PATH),
    ])
    valid, rejects, counts = transform_vessel_events(raw)
    assert [row.event_id for row in valid.collect()] == ["good"]
    assert counts["rejects"] == 1
    assert counts["reconciled"] is True
    assert "DQ-V05" in rejects.first().reject_reason


def test_malformed_json_is_rejected_without_losing_row(spark):
    raw = spark.createDataFrame(
        [("{not json", FISHING_PATH)],
        ["_raw_json", "_source_file"],
    )
    valid, rejects, counts = transform_vessel_events(raw)
    assert valid.count() == 0
    assert rejects.count() == 1
    assert counts["reconciled"] is True


def test_negative_duration_is_rejected(spark):
    raw = raw_df(spark, [
        (fishing_record(
            event_id="negative",
            start="2025-01-02T10:00:00Z",
            end="2025-01-02T08:00:00Z",
        ), FISHING_PATH)
    ])
    valid, rejects, counts = transform_vessel_events(raw)
    assert valid.count() == 0
    assert rejects.count() == 1
    assert "DQ-V02" in rejects.first().reject_reason


def test_rerun_produces_same_counts(spark):
    raw = raw_df(spark, [
        (fishing_record(), FISHING_PATH),
        (loitering_record(), LOITERING_PATH),
        (encounter_record(), ENCOUNTER_PATH),
    ])
    _, _, first = transform_vessel_events(raw)
    _, _, second = transform_vessel_events(raw)
    assert first == second
