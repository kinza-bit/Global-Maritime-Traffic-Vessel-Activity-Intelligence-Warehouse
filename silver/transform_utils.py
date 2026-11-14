"""Shared Silver transformation helpers (Member 3, contract C4).

Pure PySpark column/DataFrame functions: no Azure, no secrets, no I/O.
Rules come from docs/silver_data_dictionary.md.

Design notes
  * ANSI-safe: no helper raises on bad data. Unparseable values become NULL;
    callers count them with is_unparseable() for the audit.
  * A real 0 is never replaced (no `a or b` fallbacks).
  * Helpers accept a column name (str) or a Column.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DecimalType, StringType

_WS = r"[\s\u00a0]+"                       # whitespace incl. non-breaking space
_NUM_RE = r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"
_TS_FORMATS = ["yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX"]


def _c(col) -> Column:
    return F.col(col) if isinstance(col, str) else col


# ---------------------------------------------------------------------------
# 1. clean_text
# ---------------------------------------------------------------------------
def clean_text(col, case=None) -> Column:
    """Collapse whitespace, trim, empty string -> NULL.

    case: None (as is) | "upper" | "title".
    "title" = Title Case per word, but a word containing a digit is left
    unchanged (so 'CHN-1969' stays 'CHN-1969').
    """
    s = F.trim(F.regexp_replace(_c(col).cast("string"), _WS, " "))
    s = F.when(s == "", F.lit(None).cast("string")).otherwise(s)
    if case is None:
        return s
    if case == "upper":
        return F.upper(s)
    if case == "title":
        words = F.transform(
            F.split(s, " "),
            lambda t: F.when(t.rlike("[0-9]"), t).otherwise(F.initcap(F.lower(t))),
        )
        return F.array_join(words, " ")
    raise ValueError(f"unknown case: {case!r}")


def clean_vessel_name(col) -> Column:
    """Vessel name rule: clean, UPPER, missing -> 'Unknown'."""
    return F.coalesce(clean_text(col, "upper"), F.lit("Unknown"))


# ---------------------------------------------------------------------------
# 2. to_timestamp_utc
# ---------------------------------------------------------------------------
def to_timestamp_utc(col) -> Column:
    """ISO-8601 string (e.g. 2025-01-28T07:55:18.000Z) -> TIMESTAMP, else NULL."""
    s = clean_text(col)
    attempts = [F.try_to_timestamp(s, F.lit(f)) for f in _TS_FORMATS]
    attempts.append(F.try_to_timestamp(s))          # generic ISO fallback
    return F.coalesce(*attempts)


# ---------------------------------------------------------------------------
# 3. to_decimal
# ---------------------------------------------------------------------------
def to_decimal(col, precision: int = 10, scale: int = 2) -> Column:
    """String/number -> DECIMAL(precision, scale); NULL if unparseable or too big.

    0 stays 0. Never raises (safe under ANSI mode).
    """
    s = clean_text(col)
    d = F.when(s.rlike(_NUM_RE), s.cast("double"))
    rounded = F.round(d, scale)
    limit = float(10 ** (precision - scale))
    return F.when(rounded.isNotNull() & (F.abs(rounded) < limit),
                  rounded.cast(DecimalType(precision, scale)))


def is_unparseable(raw, parsed) -> Column:
    """TRUE where the raw value existed but the parsed result is NULL (for audit counts)."""
    return clean_text(raw).isNotNull() & _c(parsed).isNull()


# ---------------------------------------------------------------------------
# 4. to_bool
# ---------------------------------------------------------------------------
def to_bool(col, default=False) -> Column:
    """'true/t/1/yes/y' -> True, 'false/f/0/no/n' -> False, else `default`.

    default=False follows the team rule (missing flag -> FALSE, counted in
    audit). Use default=None to keep unknown values NULL.
    """
    s = F.lower(clean_text(col))
    parsed = (F.when(s.isin("true", "t", "1", "yes", "y"), F.lit(True))
               .when(s.isin("false", "f", "0", "no", "n"), F.lit(False)))
    fallback = F.lit(None).cast("boolean") if default is None else F.lit(bool(default))
    return F.coalesce(parsed, fallback)


def array_flag(col) -> Column:
    """TRUE if the array column is non-empty (e.g. regions.highSeas); NULL/[] -> FALSE."""
    c = _c(col)
    return F.coalesce(c.isNotNull() & (F.size(c) > 0), F.lit(False))


# ---------------------------------------------------------------------------
# 5. parse_json_list
# ---------------------------------------------------------------------------
def parse_json_list(col) -> Column:
    """JSON list string, e.g. '["CARGO","TANKER"]' -> ARRAY<STRING>.

    Elements are cleaned; empty elements dropped; invalid JSON / empty -> NULL.
    """
    arr = F.from_json(clean_text(col), ArrayType(StringType()))
    return F.filter(F.transform(arr, lambda x: clean_text(x)),
                    lambda x: x.isNotNull())


# ---------------------------------------------------------------------------
# 6. add_audit_columns
# ---------------------------------------------------------------------------
def add_audit_columns(df: DataFrame, run_id: str, source_col: str = "_source_file",
                      bronze_ingest_ts=None) -> DataFrame:
    """Add _source_file, _bronze_ingest_ts, _silver_load_ts, _run_id.

    _bronze_ingest_ts defaults to midnight UTC of the `ingest_date=YYYY-MM-DD`
    folder in the Bronze path (the files carry no finer timestamp). Pass a
    Column (e.g. from the manifest) to override.
    """
    if not run_id:
        raise ValueError("run_id is required")
    if source_col not in df.columns:
        raise ValueError(f"missing source column {source_col!r}")
    d = F.regexp_extract(F.col(source_col), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    from_path = F.try_to_timestamp(F.concat(d, F.lit("T00:00:00Z")),
                                   F.lit("yyyy-MM-dd'T'HH:mm:ssX"))
    ingest = from_path if bronze_ingest_ts is None else F.coalesce(_c(bronze_ingest_ts), from_path)
    out = df if source_col == "_source_file" else df.withColumn("_source_file", F.col(source_col))
    return (out.withColumn("_bronze_ingest_ts", ingest)
               .withColumn("_silver_load_ts", F.current_timestamp())
               .withColumn("_run_id", F.lit(run_id)))


# ---------------------------------------------------------------------------
# 7. dedupe_latest
# ---------------------------------------------------------------------------
def window_month_from_path(path) -> Column:
    """'.../antwerp_2025-03.jsonl' -> '2025-03' (NULL if the name has no month)."""
    m = F.regexp_extract(_c(path), r"(\d{4}-\d{2})\.jsonl$", 1)
    return F.when(m == "", F.lit(None).cast("string")).otherwise(m)


def load_type_rank(path) -> Column:
    """incremental = 2, initial = 1, other = 0."""
    p = _c(path)
    return (F.when(p.contains("load_type=incremental"), 2)
             .when(p.contains("load_type=initial"), 1)
             .otherwise(0))


def dedupe_latest(df: DataFrame, key_cols, source_col: str = "_source_file") -> DataFrame:
    """Keep one row per key. Preference: latest window month in the file name,
    then incremental over initial, then file path (so results are repeatable).

    Rows with a NULL key are NOT collapsed together; they pass through so that
    split_valid_rejects can reject them individually.
    """
    keys = [key_cols] if isinstance(key_cols, str) else list(key_cols)
    any_null = F.lit(False)
    for k in keys:
        any_null = any_null | F.col(k).isNull()
    unique = F.concat(F.lit("__null__"), F.monotonically_increasing_id().cast("string"))
    parts = [F.when(any_null, unique).otherwise(F.col(k).cast("string")) for k in keys]
    w = Window.partitionBy(*parts).orderBy(
        window_month_from_path(source_col).desc_nulls_last(),
        load_type_rank(source_col).desc(),
        F.col(source_col).desc_nulls_last(),
    )
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


# ---------------------------------------------------------------------------
# 8. split_valid_rejects
# ---------------------------------------------------------------------------
def split_valid_rejects(df: DataFrame, rules, entity: str,
                        raw_json_col: str = "_raw_json",
                        source_col: str = "_source_file"):
    """Split into (valid_df, rejects_df).

    rules: list of (reason_text, failing_condition). A condition that is TRUE
    means the row FAILS; NULL counts as not failing (missing-value rules
    must be written as their own condition, e.g. col.isNull()).
    All failed reasons are joined with '; '.

    rejects_df columns: reject_id, entity, reject_reason, raw_json, _source_file.
    Call add_audit_columns on it afterwards. reject_id is a hash, so reruns
    produce the same ids (safe for MERGE).
    """
    flags = [F.when(F.coalesce(cond, F.lit(False)), F.lit(reason)) for reason, cond in rules]
    reason = F.concat_ws("; ", F.filter(F.array(*flags), lambda x: x.isNotNull()))
    tagged = df.withColumn("_reject_reason", reason)

    valid = tagged.filter(F.col("_reject_reason") == "").drop("_reject_reason")
    rejects = (tagged.filter(F.col("_reject_reason") != "")
               .select(
                   F.sha2(F.concat_ws("||", F.lit(entity), F.col("_reject_reason"),
                                      F.col(raw_json_col)), 256).alias("reject_id"),
                   F.lit(entity).alias("entity"),
                   F.col("_reject_reason").alias("reject_reason"),
                   F.col(raw_json_col).alias("raw_json"),
                   F.col(source_col).alias("_source_file"))
               .dropDuplicates(["reject_id"]))
    return valid, rejects


# ---------------------------------------------------------------------------
# Small extras used by the notebooks
# ---------------------------------------------------------------------------
def event_base_id(col) -> Column:
    """'abc.1' -> 'abc' (encounters are stored once per vessel with .1/.2)."""
    return F.regexp_replace(clean_text(col), r"\.\d+$", "")


def hours_between(start_ts, end_ts) -> Column:
    """(end - start) in hours as DECIMAL(10,2); used for fishing/encounter duration."""
    secs = _c(end_ts).cast("double") - _c(start_ts).cast("double")
    return to_decimal(secs / F.lit(3600.0), 10, 2)