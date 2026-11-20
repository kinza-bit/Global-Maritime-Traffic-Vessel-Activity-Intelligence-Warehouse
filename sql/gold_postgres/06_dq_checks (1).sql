-- 06_dq_checks.sql  (PostgreSQL)  -- Data quality validation
-- Run AFTER the facts are loaded. Appends one row per check to
-- project_audit.dq_results; the SELECT at the bottom is the report table.
-- Every check = rows_checked vs rows_failed. PASS if rows_failed = 0.

INSERT INTO project_audit.dq_results
  (check_id, dimension, description, table_name, rows_checked, rows_failed, severity, status)
SELECT check_id, dimension, description, table_name, rows_checked, rows_failed, severity,
       CASE WHEN rows_failed = 0 THEN 'PASS'
            WHEN severity = 'WARNING' THEN 'WARN' ELSE 'FAIL' END
FROM (

  -- ============================ COMPLETENESS ============================
  SELECT 'DQ-C01','Completeness','Fact rows with NULL surrogate key (date/vessel/location)','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit WHERE sk_date IS NULL OR sk_vessel IS NULL OR sk_location IS NULL OR sk_port IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-C02','Completeness','Fact rows with NULL surrogate key (date/vessel/location)','fact_ais_gap',
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap),
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap WHERE sk_date IS NULL OR sk_vessel IS NULL OR sk_location IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-C03','Completeness','Fact rows with NULL surrogate key (date/vessel/location)','fact_vessel_event',
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event),
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event WHERE sk_date IS NULL OR sk_vessel IS NULL OR sk_location IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-C04','Completeness','Port visits mapped to Unknown port (anchorage name missing)','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit WHERE sk_port = -1),
         'WARNING'
  UNION ALL
  SELECT 'DQ-C05','Completeness','Fact rows mapped to Unknown vessel','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit WHERE sk_vessel = -1),
         'WARNING'
  UNION ALL
  SELECT 'DQ-C06','Completeness','Vessels in dim_vessel without detail record (length/tonnage/power)','dim_vessel',
         (SELECT COUNT(*) FROM project_dw.dim_vessel WHERE sk_vessel <> -1),
         (SELECT COUNT(*) FROM project_dw.dim_vessel WHERE sk_vessel <> -1 AND NOT has_vessel_details),
         'WARNING'
  UNION ALL
  SELECT 'DQ-C07','Completeness','Vessels with NULL flag_country','dim_vessel',
         (SELECT COUNT(*) FROM project_dw.dim_vessel WHERE sk_vessel <> -1),
         (SELECT COUNT(*) FROM project_dw.dim_vessel WHERE sk_vessel <> -1 AND flag_country IS NULL),
         'WARNING'

  -- ============================== ACCURACY ==============================
  UNION ALL
  SELECT 'DQ-A01','Accuracy','Staging row count = fact row count (difference)','fact_port_visit',
         (SELECT COUNT(*) FROM project_staging.stg_port_visits),
         ABS((SELECT COUNT(*) FROM project_staging.stg_port_visits) - (SELECT COUNT(*) FROM project_dw.fact_port_visit)),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A02','Accuracy','Staging row count = fact row count (difference)','fact_ais_gap',
         (SELECT COUNT(*) FROM project_staging.stg_ais_gaps),
         ABS((SELECT COUNT(*) FROM project_staging.stg_ais_gaps) - (SELECT COUNT(*) FROM project_dw.fact_ais_gap)),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A03','Accuracy','Staging row count = fact row count (difference)','fact_vessel_event',
         (SELECT COUNT(*) FROM project_staging.stg_vessel_events),
         ABS((SELECT COUNT(*) FROM project_staging.stg_vessel_events) - (SELECT COUNT(*) FROM project_dw.fact_vessel_event)),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A04','Accuracy','SUM(duration_hrs) staging vs fact (1 = mismatch)','fact_port_visit',
         1,
         CASE WHEN COALESCE((SELECT SUM(duration_hrs) FROM project_staging.stg_port_visits),0)
                 = COALESCE((SELECT SUM(duration_hrs) FROM project_dw.fact_port_visit),0) THEN 0 ELSE 1 END,
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A05','Accuracy','SUM(duration_hours) staging vs fact (1 = mismatch)','fact_ais_gap',
         1,
         CASE WHEN COALESCE((SELECT SUM(duration_hours) FROM project_staging.stg_ais_gaps),0)
                 = COALESCE((SELECT SUM(duration_hours) FROM project_dw.fact_ais_gap),0) THEN 0 ELSE 1 END,
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A06','Accuracy','SUM(duration_hours) staging vs fact (1 = mismatch)','fact_vessel_event',
         1,
         CASE WHEN COALESCE((SELECT SUM(duration_hours) FROM project_staging.stg_vessel_events),0)
                 = COALESCE((SELECT SUM(duration_hours) FROM project_dw.fact_vessel_event),0) THEN 0 ELSE 1 END,
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-A07','Accuracy','Staging vessel_ids missing from dim_vessel','dim_vessel',
         (SELECT COUNT(DISTINCT vessel_id) FROM project_staging.stg_port_visits),
         (SELECT COUNT(DISTINCT s.vessel_id) FROM (
              SELECT vessel_id FROM project_staging.stg_port_visits
              UNION SELECT vessel_id FROM project_staging.stg_vessel_events
              UNION SELECT vessel_id FROM project_staging.stg_ais_gaps) s
          LEFT JOIN project_dw.dim_vessel d ON d.vessel_id = s.vessel_id
          WHERE d.vessel_id IS NULL),
         'CRITICAL'

  -- ============================== INTEGRITY =============================
  -- "Fact rows without matching dimension = 0"
  UNION ALL
  SELECT 'DQ-I01','Integrity','Fact rows without matching dim_date','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit f LEFT JOIN project_dw.dim_date d ON d.sk_date = f.sk_date WHERE d.sk_date IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I02','Integrity','Fact rows without matching dim_vessel','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit f LEFT JOIN project_dw.dim_vessel d ON d.sk_vessel = f.sk_vessel WHERE d.sk_vessel IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I03','Integrity','Fact rows without matching dim_port','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit f LEFT JOIN project_dw.dim_port d ON d.sk_port = f.sk_port WHERE d.sk_port IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I04','Integrity','Fact rows without matching dim_location','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) FROM project_dw.fact_port_visit f LEFT JOIN project_dw.dim_location d ON d.sk_location = f.sk_location WHERE d.sk_location IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I05','Integrity','Fact rows without matching dim_date','fact_ais_gap',
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap),
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap f LEFT JOIN project_dw.dim_date d ON d.sk_date = f.sk_date WHERE d.sk_date IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I06','Integrity','Fact rows without matching dim_vessel','fact_ais_gap',
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap),
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap f LEFT JOIN project_dw.dim_vessel d ON d.sk_vessel = f.sk_vessel WHERE d.sk_vessel IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I07','Integrity','Fact rows without matching dim_location','fact_ais_gap',
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap),
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap f LEFT JOIN project_dw.dim_location d ON d.sk_location = f.sk_location WHERE d.sk_location IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I08','Integrity','Fact rows without matching dim_date','fact_vessel_event',
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event),
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event f LEFT JOIN project_dw.dim_date d ON d.sk_date = f.sk_date WHERE d.sk_date IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I09','Integrity','Fact rows without matching dim_vessel','fact_vessel_event',
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event),
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event f LEFT JOIN project_dw.dim_vessel d ON d.sk_vessel = f.sk_vessel WHERE d.sk_vessel IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I10','Integrity','Fact rows without matching dim_location','fact_vessel_event',
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event),
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event f LEFT JOIN project_dw.dim_location d ON d.sk_location = f.sk_location WHERE d.sk_location IS NULL),
         'CRITICAL'
  UNION ALL
  SELECT 'DQ-I11','Integrity','Encounter rows whose encountered vessel is missing from dim_vessel','fact_vessel_event',
         (SELECT COUNT(*) FROM project_staging.stg_vessel_events WHERE encountered_vessel_id IS NOT NULL),
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event f
            JOIN project_staging.stg_vessel_events s ON s.event_id = f.event_id
           WHERE s.encountered_vessel_id IS NOT NULL AND f.sk_encountered_vessel IS NULL),
         'CRITICAL'

  -- ============================== UNIQUENESS ============================
  UNION ALL
  SELECT 'DQ-U01','Uniqueness','Duplicate event_id','fact_port_visit',
         (SELECT COUNT(*) FROM project_dw.fact_port_visit),
         (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM project_dw.fact_port_visit), 'CRITICAL'
  UNION ALL
  SELECT 'DQ-U02','Uniqueness','Duplicate event_id','fact_ais_gap',
         (SELECT COUNT(*) FROM project_dw.fact_ais_gap),
         (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM project_dw.fact_ais_gap), 'CRITICAL'
  UNION ALL
  SELECT 'DQ-U03','Uniqueness','Duplicate event_id','fact_vessel_event',
         (SELECT COUNT(*) FROM project_dw.fact_vessel_event),
         (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM project_dw.fact_vessel_event), 'CRITICAL'
  UNION ALL
  SELECT 'DQ-U04','Uniqueness','Duplicate natural key (vessel_id)','dim_vessel',
         (SELECT COUNT(*) FROM project_dw.dim_vessel),
         (SELECT COUNT(*) - COUNT(DISTINCT vessel_id) FROM project_dw.dim_vessel), 'CRITICAL'
  UNION ALL
  SELECT 'DQ-U05','Uniqueness','Duplicate natural key (port_name)','dim_port',
         (SELECT COUNT(*) FROM project_dw.dim_port),
         (SELECT COUNT(*) - COUNT(DISTINCT port_name) FROM project_dw.dim_port), 'CRITICAL'

) c(check_id, dimension, description, table_name, rows_checked, rows_failed, severity);

-- ----- Report: latest run
SELECT check_id, dimension, description, table_name, rows_checked, rows_failed, severity, status
FROM project_audit.dq_results
WHERE checked_at = (SELECT MAX(checked_at) FROM project_audit.dq_results)
ORDER BY check_id;
