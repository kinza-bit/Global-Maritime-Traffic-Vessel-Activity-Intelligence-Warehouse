"""Unit tests for silver/transform_utils.py (run locally: pytest -q; no Azure needed).

Runs with spark.sql.ansi.enabled=true to mimic Databricks serverless, so any
helper that raises on bad data fails here.
"""
import os
import sys
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, StringType, StructField,
                               StructType, TimestampType)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from silver.transform_utils import (add_audit_columns, array_flag, clean_text,  # noqa: E402
                                    clean_vessel_name, dedupe_latest, event_base_id,
                                    hours_between, is_unparseable, parse_json_list,
                                    split_valid_rejects, to_bool, to_decimal,
                                    to_timestamp_utc)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("test_transforms")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def one_col(spark, values, dtype=StringType()):
    schema = StructType([StructField("v", dtype, True)])
    return spark.createDataFrame([(v,) for v in values], schema)


def apply(spark, values, fn, dtype=StringType()):
    df = one_col(spark, values, dtype)
    return [r[0] for r in df.select(fn(F.col("v"))).collect()]


# ---- clean_text -----------------------------------------------------------
def test_clean_text_title(spark):
    vals = [" lahore ", "LAHORE", "", None, "  a   b ", "CHN-1969",
            "ROTTERDAM MAASVLAKTE", "\u00a0 x\ty ", "   "]
    out = apply(spark, vals, lambda c: clean_text(c, "title"))
    assert out == ["Lahore", "Lahore", None, None, "A B", "CHN-1969",
                   "Rotterdam Maasvlakte", "X Y", None]


def test_clean_text_upper_and_plain(spark):
    assert apply(spark, [" pan ", "", None], lambda c: clean_text(c, "upper")) == ["PAN", None, None]
    assert apply(spark, [" a  b "], lambda c: clean_text(c)) == ["a b"]


def test_clean_text_bad_case_raises():
    with pytest.raises(ValueError):
        clean_text("x", "weird")


def test_clean_vessel_name(spark):
    out = apply(spark, ["taiko maru no.8", None, "  ", "FU YUAN YU7635 "], clean_vessel_name)
    assert out == ["TAIKO MARU NO.8", "Unknown", "Unknown", "FU YUAN YU7635"]


# ---- to_timestamp_utc -----------------------------------------------------
def test_to_timestamp_utc(spark):
    vals = ["2025-01-28T07:55:18.000Z", "2025-01-28T07:55:18Z", "not a date",
            "", None, "2025-13-45T99:00:00.000Z"]
    out = apply(spark, vals, to_timestamp_utc)
    assert out[0] == datetime(2025, 1, 28, 7, 55, 18)
    assert out[1] == datetime(2025, 1, 28, 7, 55, 18)
    assert out[2:] == [None, None, None, None]


# ---- to_decimal -----------------------------------------------------------
def test_to_decimal_keeps_zero_and_nulls_bad(spark):
    vals = ["928.086375", "0", "0.0", None, "abc", "", "1e20", "-5", " 12.5 ",
            "99999999.999"]
    out = apply(spark, vals, lambda c: to_decimal(c, 10, 2))
    assert out == [Decimal("928.09"), Decimal("0.00"), Decimal("0.00"), None, None, None,
                   None, Decimal("-5.00"), Decimal("12.50"), None]


def test_to_decimal_numeric_input_and_zero_speed(spark):
    out = apply(spark, [0.0, 5.142056746522082, None], lambda c: to_decimal(c), DoubleType())
    assert out == [Decimal("0.00"), Decimal("5.14"), None]


def test_to_decimal_coordinates(spark):
    out = apply(spark, ["51.3609", "-112.2761"], lambda c: to_decimal(c, 10, 6))
    assert out == [Decimal("51.360900"), Decimal("-112.276100")]


def test_is_unparseable(spark):
    df = one_col(spark, ["1.5", "abc", None, ""])
    out = [r[0] for r in df.select(is_unparseable(F.col("v"), to_decimal("v"))).collect()]
    assert out == [False, True, False, False]


# ---- to_bool / array_flag -------------------------------------------------
def test_to_bool(spark):
    vals = ["true", "TRUE", "false", "0", "1", None, "maybe", " yes "]
    assert apply(spark, vals, to_bool) == [True, True, False, False, True, False, False, True]
    assert apply(spark, ["maybe", None, "true"], lambda c: to_bool(c, default=None)) == [None, None, True]


def test_array_flag(spark):
    out = apply(spark, [["high_seas"], [], None], array_flag, ArrayType(StringType()))
    assert out == [True, False, False]


# ---- parse_json_list ------------------------------------------------------
def test_parse_json_list(spark):
    vals = ['["CARGO","TANKER"]', "bad json", "", None, '[" a ", ""]', "[]"]
    out = apply(spark, vals, parse_json_list)
    assert out == [["CARGO", "TANKER"], None, None, None, ["a"], []]


# ---- audit columns --------------------------------------------------------
def test_add_audit_columns(spark):
    schema = StructType([StructField("_source_file", StringType()),
                         StructField("x", StringType())])
    df = spark.createDataFrame(
        [("abfss://b/gfw/events/gap/load_type=initial/ingest_date=2026-11-12/world_2025-01.jsonl", "a"),
         ("no-date-in-path.jsonl", "b")], schema)
    rows = add_audit_columns(df, "run_1").orderBy("x").collect()
    assert rows[0]["_run_id"] == "run_1"
    assert rows[0]["_bronze_ingest_ts"] == datetime(2026, 11, 12, 0, 0, 0)
    assert rows[0]["_silver_load_ts"] is not None
    assert rows[1]["_bronze_ingest_ts"] is None
    with pytest.raises(ValueError):
        add_audit_columns(df, "")
    with pytest.raises(ValueError):
        add_audit_columns(df.drop("_source_file"), "run_1")


def test_add_audit_columns_override(spark):
    schema = StructType([StructField("_source_file", StringType()),
                         StructField("ts", TimestampType())])
    df = spark.createDataFrame([("p.jsonl", datetime(2026, 1, 2, 3, 4, 5))], schema)
    row = add_audit_columns(df, "r", bronze_ingest_ts=F.col("ts")).collect()[0]
    assert row["_bronze_ingest_ts"] == datetime(2026, 1, 2, 3, 4, 5)


# ---- dedupe_latest --------------------------------------------------------
P = "abfss://b/gfw/events/port_visit/load_type={lt}/ingest_date=2026-11-12/antwerp_{m}.jsonl"


def test_dedupe_latest(spark):
    schema = StructType([StructField("event_id", StringType()),
                         StructField("_source_file", StringType()),
                         StructField("val", StringType())])
    rows = [
        ("A", P.format(lt="initial", m="2025-01"), "a1"),
        ("A", P.format(lt="initial", m="2025-02"), "a2"),
        ("A", P.format(lt="incremental", m="2025-03"), "a3"),
        ("B", P.format(lt="initial", m="2025-03"), "b_initial"),
        ("B", P.format(lt="incremental", m="2025-03"), "b_incr"),
        ("C", P.format(lt="initial", m="2025-01"), "c"),
        (None, P.format(lt="initial", m="2025-01"), "n1"),
        (None, P.format(lt="initial", m="2025-01"), "n2"),
    ]
    out = {r["val"] for r in dedupe_latest(spark.createDataFrame(rows, schema), "event_id").collect()}
    assert out == {"a3", "b_incr", "c", "n1", "n2"}


def test_dedupe_is_repeatable(spark):
    schema = StructType([StructField("event_id", StringType()),
                         StructField("_source_file", StringType()),
                         StructField("val", StringType())])
    rows = [("A", P.format(lt="initial", m="2025-01"), "x"),
            ("A", P.format(lt="initial", m="2025-01"), "x")]
    df = spark.createDataFrame(rows, schema)
    assert dedupe_latest(df, ["event_id"]).count() == 1
    assert dedupe_latest(dedupe_latest(df, ["event_id"]), ["event_id"]).count() == 1


# ---- split_valid_rejects --------------------------------------------------
def test_split_valid_rejects(spark):
    schema = StructType([StructField("event_id", StringType()),
                         StructField("vessel_id", StringType()),
                         StructField("duration", DoubleType()),
                         StructField("_source_file", StringType()),
                         StructField("_raw_json", StringType())])
    rows = [("e1", "v1", 5.0, "f", "{1}"),
            ("e2", "v1", 0.0, "f", "{2}"),      # zero duration is valid
            ("e3", "v1", None, "f", "{3}"),     # NULL condition is not failing
            ("e4", "v1", -1.0, "f", "{4}"),
            (None, "v1", -2.0, "f", "{5}")]
    df = spark.createDataFrame(rows, schema)
    rules = [("DQ-C01 event_id NULL", F.col("event_id").isNull()),
             ("DQ-V01 duration < 0", F.col("duration") < 0)]
    valid, rejects = split_valid_rejects(df, rules, "port_visit")
    assert valid.count() + rejects.count() == df.count()
    assert {r["event_id"] for r in valid.collect()} == {"e1", "e2", "e3"}
    assert valid.columns == df.columns
    rj = {r["raw_json"]: r["reject_reason"] for r in rejects.collect()}
    assert rj == {"{4}": "DQ-V01 duration < 0",
                  "{5}": "DQ-C01 event_id NULL; DQ-V01 duration < 0"}
    assert rejects.columns == ["reject_id", "entity", "reject_reason", "raw_json", "_source_file"]
    ids1 = sorted(r["reject_id"] for r in rejects.collect())
    ids2 = sorted(r["reject_id"] for r in split_valid_rejects(df, rules, "port_visit")[1].collect())
    assert ids1 == ids2                          # deterministic ids


def test_split_with_no_failures(spark):
    schema = StructType([StructField("a", StringType()), StructField("_source_file", StringType()),
                         StructField("_raw_json", StringType())])
    df = spark.createDataFrame([("x", "f", "{}")], schema)
    valid, rejects = split_valid_rejects(df, [("r", F.col("a").isNull())], "gap")
    assert valid.count() == 1 and rejects.count() == 0


# ---- extras ---------------------------------------------------------------
def test_event_base_id(spark):
    assert apply(spark, ["abc.1", "abc.2", "abc", None], event_base_id) == ["abc", "abc", "abc", None]


def test_hours_between(spark):
    schema = StructType([StructField("s", TimestampType()), StructField("e", TimestampType())])
    df = spark.createDataFrame([(datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 3, 30)),
                                (datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 0, 0)),
                                (None, datetime(2025, 1, 1, 0, 0))], schema)
    out = [r[0] for r in df.select(hours_between("s", "e")).collect()]
    assert out == [Decimal("3.50"), Decimal("0.00"), None]