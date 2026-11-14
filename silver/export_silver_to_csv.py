# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# export_silver_to_csv.py  -- exports the Silver Delta tables to CSV so they can be
# loaded into PostgreSQL with postgres/03_load_staging_from_csv.sql (\copy).
# Column order is kept identical to the Delta tables = identical to the PG DDL.
from pyspark.sql import functions as F

STORAGE = "stmaritimeks"   # storage account name used in sql/10_stg_tables.sql
TABLES = ["stg_port_visits", "stg_vessel_events", "stg_rejects", "stg_ais_gaps", "stg_vessels"]

for t in TABLES:
    df = spark.table(f"project_staging.{t}")
    if t == "stg_vessels":   # arrays -> 'a|b|c' text
        df = (df.withColumn("ship_types", F.concat_ws("|", "ship_types"))
                .withColumn("gear_types", F.concat_ws("|", "gear_types")))
    out = f"abfss://silver@{STORAGE}.dfs.core.windows.net/_export/{t}"
    (df.coalesce(1).write.mode("overwrite")
       .option("header", True)
       .option("quote", '"').option("escape", '"')
       .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
       .csv(out))
    print(t, df.count(), "rows ->", out)
# Then download each part-*.csv from the _export/<table>/ folder (Storage Explorer)
# and save it as export/<table>.csv next to the SQL scripts.