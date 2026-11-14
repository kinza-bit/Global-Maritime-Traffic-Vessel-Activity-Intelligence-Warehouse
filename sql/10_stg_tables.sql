-- sql/10_stg_tables.sql  (Member 3)
-- Generated from schemas/silver_schemas.py - keep the two in sync.
-- Run after sql/00_create_schemas.sql (Member 1). Safe to re-run.
-- Tables are Delta, stored in the `silver` container.
-- NOTE: names are two-level (project_staging.<table>). If the workspace uses
-- Unity Catalog, prefix the catalog. If CREATE ... LOCATION is refused, report the error.

-- stg_port_visits: merge key = event_id
CREATE TABLE IF NOT EXISTS project_staging.stg_port_visits (
  event_id STRING NOT NULL,
  visit_id STRING,
  vessel_id STRING NOT NULL,
  vessel_name STRING,
  ssvid STRING,
  vessel_flag STRING,
  vessel_type STRING,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  start_date DATE,
  duration_hrs DECIMAL(10,2),
  confidence INT,
  lat DECIMAL(10,6),
  lon DECIMAL(10,6),
  start_anchorage_name STRING,
  end_anchorage_name STRING,
  end_anchorage_flag STRING,
  end_anchorage_lat DECIMAL(10,6),
  end_anchorage_lon DECIMAL(10,6),
  end_at_dock BOOLEAN,
  start_dist_port_km DECIMAL(10,2),
  end_dist_port_km DECIMAL(10,2),
  start_dist_shore_km DECIMAL(10,2),
  end_dist_shore_km DECIMAL(10,2),
  high_seas BOOLEAN,
  _source_file STRING NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id STRING NOT NULL
)
USING DELTA
LOCATION 'abfss://silver@stmaritimeks.dfs.core.windows.net/stg_port_visits'
COMMENT 'Silver: one row per port visit (key event_id). Owner: Member 3.';

-- stg_vessel_events: merge key = event_id
CREATE TABLE IF NOT EXISTS project_staging.stg_vessel_events (
  event_id STRING NOT NULL,
  event_base_id STRING,
  event_type STRING NOT NULL,
  vessel_id STRING NOT NULL,
  vessel_name STRING,
  ssvid STRING,
  vessel_flag STRING,
  vessel_type STRING,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  start_date DATE,
  lat DECIMAL(10,6),
  lon DECIMAL(10,6),
  duration_hours DECIMAL(10,2),
  avg_speed_knots DECIMAL(10,2),
  total_distance_km DECIMAL(10,2),
  avg_dist_shore_km DECIMAL(10,2),
  potential_risk BOOLEAN,
  encounter_type STRING,
  encounter_median_speed_knots DECIMAL(10,2),
  encounter_median_distance_km DECIMAL(10,2),
  encountered_vessel_id STRING,
  encountered_vessel_name STRING,
  encountered_vessel_flag STRING,
  encountered_vessel_type STRING,
  start_dist_shore_km DECIMAL(10,2),
  end_dist_shore_km DECIMAL(10,2),
  start_dist_port_km DECIMAL(10,2),
  end_dist_port_km DECIMAL(10,2),
  high_seas BOOLEAN,
  mpa BOOLEAN,
  mpa_no_take BOOLEAN,
  _source_file STRING NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id STRING NOT NULL
)
USING DELTA
LOCATION 'abfss://silver@stmaritimeks.dfs.core.windows.net/stg_vessel_events'
COMMENT 'Silver: fishing, loitering and encounter events (key event_id). Owner: Member 3.';

-- stg_rejects: merge key = reject_id
CREATE TABLE IF NOT EXISTS project_staging.stg_rejects (
  reject_id STRING NOT NULL,
  entity STRING NOT NULL,
  reject_reason STRING NOT NULL,
  raw_json STRING,
  _source_file STRING NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id STRING NOT NULL
)
USING DELTA
LOCATION 'abfss://silver@stmaritimeks.dfs.core.windows.net/stg_rejects'
COMMENT 'Silver: rows that failed critical rules, with reason and original JSON (key reject_id). Owner: Member 3.';

-- stg_ais_gaps  (Member 1) and stg_vessels (Member 2)
-- TODO: add their DDL here once their schemas are final. stg_vessels is already
-- written by silver/23_stg_vessels.py to
--   abfss://silver@stmaritimeks.dfs.core.windows.net/stg_vessels
-- so copy its columns from that Delta table:  DESCRIBE TABLE delta.`<that path>`