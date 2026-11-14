"""Local tests for silver/port_visits_logic.py.

Run from the repo root:  python -m pytest -q tests/test_port_visits_logic.py
Needs silver/__init__.py and schemas/__init__.py to exist.
Casing expectations assume title case for names; change them if the team
agreed something else.
"""
import json
import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from silver.port_visits_logic import transform_port_visits  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        # Databricks: reuse the existing session, never stop it
        s = SparkSession.builder.getOrCreate()
        try:
            s.conf.set("spark.sql.ansi.enabled", "true")
        except Exception:
            pass  # Serverless: ANSI is already on and may be locked
        s.conf.set("spark.sql.session.timeZone", "UTC")
        yield s
    else:
        s = (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        yield s
        s.stop()


def pv(id="e1", dur="5.5", start="2025-01-10T08:00:00.000Z",
       end="2025-01-10T13:30:00.000Z", vessel_id="v1", vname=" ever given ",
       anch="antwerp", port_dist="0", lat=51.2):
    d = {
        "id": id, "type": "PORT_VISIT", "start": start, "end": end,
        "vessel": {"id": vessel_id, "name": vname, "ssvid": "123",
                   "flag": "pan", "type": "cargo"},
        "position": {"lat": lat, "lon": 4.4},
        "port_visit": {
            "visitId": "x", "durationHrs": dur, "confidence": "4",
            "startAnchorage": {"name": anch},
            "endAnchorage": {"name": anch, "flag": "BEL", "lat": 51.2,
                             "lon": 4.4, "atDock": True},
        },
        "distances": {"startDistanceFromPortKm": port_dist,
                      "endDistanceFromPortKm": "1.5",
                      "startDistanceFromShoreKm": "2",
                      "endDistanceFromShoreKm": "2"},
        "regions": {"highSeas": []},
    }
    return json.dumps(d)


INIT = "gfw/events/port_visit/load_type=initial/ingest_date=2025-01-05/antwerp_2025-01.jsonl"
INCR = "gfw/events/port_visit/load_type=incremental/ingest_date=2025-03-05/antwerp_2025-01.jsonl"


def build_raw(spark):
    lines = [
        (pv("e1"), INIT),
        (pv("e2", anch="old"), INIT),
        (pv("e2", anch="new"), INCR),                      # duplicate, newer wins
        (pv("e3", dur="-1"), INIT),                        # negative duration
        (pv(None), INIT),                                  # NULL id
        (pv("e5", start="2025-01-10T10:00:00Z",
            end="2025-01-10T08:00:00Z"), INIT),            # end before start
        (pv("e6", start="not-a-date"), INIT),              # bad timestamp
        ("{not json", INIT),                               # malformed line
        (pv("e8", port_dist="0"), INIT),                   # zero must stay 0
        (pv("e9", vname=None), INIT),                      # -> 'Unknown'
        (pv("e10", anch=None), INIT),                      # NULL anchorage: warning only
    ]
    return spark.createDataFrame(lines, ["_raw_json", "_source_file"])


def test_valid_and_rejected_sets(spark):
    valid, rejects, c = transform_port_visits(build_raw(spark))
    v = {r.event_id: r for r in valid.collect()}

    assert set(v) == {"e1", "e2", "e8", "e9", "e10"}
    assert c["valid"] == 5 and c["rejects"] == 5


def test_reconciliation(spark):
    _, _, c = transform_port_visits(build_raw(spark))
    # 8 unique keyed ids + 2 unkeyed rows = 10 = 5 valid + 5 rejects
    assert c["unique_keyed"] == 8
    assert c["unkeyed"] == 2
    assert c["duplicates_removed"] == 1
    assert c["reconciled"]


def test_null_ids_do_not_collapse(spark):
    raw = spark.createDataFrame(
        [(pv(None), INIT), (pv(None, vessel_id="v2"), INIT), ("{bad", INIT), ("{bad2", INIT)],
        ["_raw_json", "_source_file"],
    )
    valid, rejects, c = transform_port_visits(raw)
    assert c["valid"] == 0
    assert c["rejects"] == 4  # four distinct rows, none lost
    assert c["reconciled"]


def test_field_rules(spark):
    valid, _, _ = transform_port_visits(build_raw(spark))
    v = {r.event_id: r for r in valid.collect()}

    assert v["e2"].start_anchorage_name == "New"            # latest wins
    assert float(v["e8"].start_dist_port_km) == 0.0         # zero preserved, not NULL
    assert v["e9"].vessel_name == "Unknown"
    assert v["e10"].end_anchorage_name is None              # warning, not reject
    assert v["e1"].vessel_name == "EVER GIVEN"
    assert v["e1"].vessel_flag == "PAN"


def test_reject_reasons(spark):
    _, rejects, _ = transform_port_visits(build_raw(spark))
    reasons = [r.reject_reason for r in rejects.collect()]

    assert any("MALFORMED_JSON" in x for x in reasons)
    assert any("DQ-C01" in x for x in reasons)
    assert any("DQ-V01" in x for x in reasons)
    assert any("DQ-V02" in x for x in reasons)
    assert any("DQ-C03" in x for x in reasons)


def test_rerun_gives_same_counts(spark):
    _, _, c1 = transform_port_visits(build_raw(spark))
    _, _, c2 = transform_port_visits(build_raw(spark))
    assert c1 == c2


def test_invalid_latitude_rejected(spark):
    raw = spark.createDataFrame(
        [(pv("ok"), INIT), (pv("badlat", lat=95.0), INIT)],
        ["_raw_json", "_source_file"],
    )
    valid, rejects, c = transform_port_visits(raw)
    assert [r.event_id for r in valid.collect()] == ["ok"]
    assert c["reconciled"]