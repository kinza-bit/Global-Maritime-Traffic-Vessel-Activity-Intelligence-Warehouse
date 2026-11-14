-- 03_load_staging_from_csv.sql  (psql)
-- Loads the CSV exports of the Silver Delta tables into project_staging.
-- Produce the CSVs first with etl/export_silver_to_csv.py (Databricks), download
-- each table's single part file and rename it to <table>.csv inside ./export/.
-- Run from the folder that contains ./export:  psql -d maritime_dwbi -f 03_load_staging_from_csv.sql

TRUNCATE project_staging.stg_port_visits, project_staging.stg_vessel_events,
         project_staging.stg_rejects, project_staging.stg_ais_gaps, project_staging.stg_vessels;

\copy project_staging.stg_port_visits   FROM 'export/stg_port_visits.csv'   WITH (FORMAT csv, HEADER true)
\copy project_staging.stg_vessel_events FROM 'export/stg_vessel_events.csv' WITH (FORMAT csv, HEADER true)
\copy project_staging.stg_rejects       FROM 'export/stg_rejects.csv'       WITH (FORMAT csv, HEADER true)
\copy project_staging.stg_ais_gaps      FROM 'export/stg_ais_gaps.csv'      WITH (FORMAT csv, HEADER true)
\copy project_staging.stg_vessels       FROM 'export/stg_vessels.csv'       WITH (FORMAT csv, HEADER true)

SELECT 'stg_port_visits' t, COUNT(*) FROM project_staging.stg_port_visits
UNION ALL SELECT 'stg_vessel_events', COUNT(*) FROM project_staging.stg_vessel_events
UNION ALL SELECT 'stg_ais_gaps',      COUNT(*) FROM project_staging.stg_ais_gaps
UNION ALL SELECT 'stg_vessels',       COUNT(*) FROM project_staging.stg_vessels
UNION ALL SELECT 'stg_rejects',       COUNT(*) FROM project_staging.stg_rejects;
