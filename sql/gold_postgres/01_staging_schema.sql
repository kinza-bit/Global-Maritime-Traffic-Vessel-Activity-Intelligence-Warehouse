-- 01_staging_schema.sql  (PostgreSQL)
-- Staging tables = the Silver tables, same names, same column ORDER as
-- sql/10_stg_tables.sql (Spark SQL), so exported CSVs load with \copy.
-- Type mapping: STRING->TEXT, DECIMAL->NUMERIC, ARRAY<STRING>-> TEXT ('a|b|c').

CREATE TABLE IF NOT EXISTS project_staging.stg_port_visits (
  event_id TEXT NOT NULL PRIMARY KEY,
  visit_id TEXT,
  vessel_id TEXT NOT NULL,
  vessel_name TEXT,
  ssvid TEXT,
  vessel_flag TEXT,
  vessel_type TEXT,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  start_date DATE,
  duration_hrs NUMERIC(10,2),
  confidence INT,
  lat NUMERIC(10,6),
  lon NUMERIC(10,6),
  start_anchorage_name TEXT,
  end_anchorage_name TEXT,
  end_anchorage_flag TEXT,
  end_anchorage_lat NUMERIC(10,6),
  end_anchorage_lon NUMERIC(10,6),
  end_at_dock BOOLEAN,
  start_dist_port_km NUMERIC(10,2),
  end_dist_port_km NUMERIC(10,2),
  start_dist_shore_km NUMERIC(10,2),
  end_dist_shore_km NUMERIC(10,2),
  high_seas BOOLEAN,
  _source_file TEXT NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_staging.stg_vessel_events (
  event_id TEXT NOT NULL PRIMARY KEY,
  event_base_id TEXT,
  event_type TEXT NOT NULL,
  vessel_id TEXT NOT NULL,
  vessel_name TEXT,
  ssvid TEXT,
  vessel_flag TEXT,
  vessel_type TEXT,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  start_date DATE,
  lat NUMERIC(10,6),
  lon NUMERIC(10,6),
  duration_hours NUMERIC(10,2),
  avg_speed_knots NUMERIC(10,2),
  total_distance_km NUMERIC(10,2),
  avg_dist_shore_km NUMERIC(10,2),
  potential_risk BOOLEAN,
  encounter_type TEXT,
  encounter_median_speed_knots NUMERIC(10,2),
  encounter_median_distance_km NUMERIC(10,2),
  encountered_vessel_id TEXT,
  encountered_vessel_name TEXT,
  encountered_vessel_flag TEXT,
  encountered_vessel_type TEXT,
  start_dist_shore_km NUMERIC(10,2),
  end_dist_shore_km NUMERIC(10,2),
  start_dist_port_km NUMERIC(10,2),
  end_dist_port_km NUMERIC(10,2),
  high_seas BOOLEAN,
  mpa BOOLEAN,
  mpa_no_take BOOLEAN,
  _source_file TEXT NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_staging.stg_rejects (
  reject_id TEXT NOT NULL PRIMARY KEY,
  entity TEXT NOT NULL,
  reject_reason TEXT NOT NULL,
  raw_json TEXT,
  _source_file TEXT NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_staging.stg_ais_gaps (
  event_id TEXT NOT NULL PRIMARY KEY,
  vessel_id TEXT NOT NULL,
  vessel_name TEXT,
  ssvid TEXT,
  vessel_flag TEXT,
  vessel_type TEXT,
  start_ts TIMESTAMP NOT NULL,
  end_ts TIMESTAMP NOT NULL,
  start_date DATE,
  duration_hours NUMERIC(10,2),
  distance_km NUMERIC(10,2),
  implied_speed_knots NUMERIC(10,2),
  intentional_disabling BOOLEAN,
  off_lat NUMERIC(10,6),
  off_lon NUMERIC(10,6),
  on_lat NUMERIC(10,6),
  on_lon NUMERIC(10,6),
  positions_per_day_sat_reception NUMERIC(10,2),
  high_seas BOOLEAN,
  mpa BOOLEAN,
  mpa_no_take BOOLEAN,
  _source_file TEXT NOT NULL,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP NOT NULL,
  _run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_staging.stg_vessels (
  vessel_id TEXT NOT NULL PRIMARY KEY,
  vessel_name TEXT,
  ssvid TEXT,
  imo TEXT,
  callsign TEXT,
  vessel_flag TEXT,
  length_m NUMERIC(10,2),
  tonnage_gt NUMERIC(10,2),
  engine_power_kw NUMERIC(10,2),
  max_speed_kn NUMERIC(10,2),
  ship_types TEXT,      -- pipe-separated: 'CARGO|TANKER'
  gear_types TEXT,      -- pipe-separated
  _source_file TEXT,
  _bronze_ingest_ts TIMESTAMP,
  _silver_load_ts TIMESTAMP,
  _run_id TEXT
);
