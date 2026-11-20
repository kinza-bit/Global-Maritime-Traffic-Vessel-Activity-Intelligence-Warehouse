-- 07_reporting_views.sql (PostgreSQL) - clean project_reporting views for Phase 3 and Power BI

-- Port visits that START inside our load window (excludes legacy open visits)
CREATE OR REPLACE VIEW project_reporting.vw_port_visits_window AS
SELECT f.*, d.year, d.month_number, d.month_name, d.day_of_week, d.day_of_week_num,
       p.port_name, p.port_country,
       (p.port_name !~ '^[A-Z]{3}-[0-9]+$') AS is_named_port,
       v.flag_country, v.primary_vessel_type
FROM project_dw.fact_port_visit f
JOIN project_dw.dim_date   d ON d.sk_date   = f.sk_date
JOIN project_dw.dim_port   p ON p.sk_port   = f.sk_port
JOIN project_dw.dim_vessel v ON v.sk_vessel = f.sk_vessel
WHERE f.start_ts >= '2025-01-01' AND f.sk_port <> -1;

DROP VIEW IF EXISTS project_reporting.vw_monthly_port_traffic;
DROP VIEW IF EXISTS project_reporting.vw_port_performance;
DROP VIEW IF EXISTS project_reporting.vw_ais_gap_analysis;
DROP VIEW IF EXISTS project_reporting.vw_vessel_events_analysis;

-- Monthly port traffic (replaces vw_monthly_sales)
CREATE VIEW project_reporting.vw_monthly_port_traffic AS
SELECT year, month_number, month_name, port_name, port_country,
       COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS median_dwell_hrs
FROM project_reporting.vw_port_visits_window
WHERE is_named_port
GROUP BY year, month_number, month_name, port_name, port_country;

-- Fleet analysis (replaces vw_customer_analysis); unchanged logic
CREATE OR REPLACE VIEW project_reporting.vw_vessel_analysis AS
SELECT COALESCE(flag_country, 'UNKNOWN')         AS flag_country,
       COALESCE(primary_vessel_type, 'UNKNOWN')  AS vessel_type,
       COUNT(*)                                  AS vessels,
       ROUND(AVG(length_m), 2)                   AS avg_length_m,
       ROUND(AVG(tonnage_gt), 2)                 AS avg_tonnage_gt,
       ROUND(AVG(engine_power_kw), 2)            AS avg_engine_power_kw
FROM project_dw.dim_vessel
WHERE sk_vessel <> -1 AND has_vessel_details
GROUP BY 1, 2;

-- Port performance (replaces vw_product_performance)
CREATE VIEW project_reporting.vw_port_performance AS
SELECT port_name, port_country,
       COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS median_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS p90_dwell_hrs,
       ROUND(100.0 * AVG(CASE WHEN end_at_dock THEN 1 ELSE 0 END), 1) AS pct_ended_at_dock
FROM project_reporting.vw_port_visits_window
WHERE is_named_port
GROUP BY port_name, port_country;

-- AIS gaps (dark fleet): GEAR buoys excluded
CREATE VIEW project_reporting.vw_ais_gap_analysis AS
SELECT d.year, d.month_number,
       COALESCE(v.flag_country, 'UNKNOWN') AS flag_country,
       v.vessel_id, v.vessel_name,
       COUNT(*) AS gaps,
       SUM(CASE WHEN g.intentional_disabling THEN 1 ELSE 0 END) AS intentional_gaps,
       ROUND(SUM(g.duration_hours), 1) AS total_gap_hours,
       ROUND(AVG(g.duration_hours), 1) AS avg_gap_hours
FROM project_dw.fact_ais_gap g
JOIN project_dw.dim_date   d ON d.sk_date   = g.sk_date
JOIN project_dw.dim_vessel v ON v.sk_vessel = g.sk_vessel
WHERE v.sk_vessel <> -1 AND COALESCE(v.primary_vessel_type, '') <> 'GEAR'
GROUP BY d.year, d.month_number, v.flag_country, v.vessel_id, v.vessel_name;

-- Fishing / loitering / encounter events: GEAR buoys and events over 30 days excluded
CREATE VIEW project_reporting.vw_vessel_events_analysis AS
SELECT d.year, d.month_number, e.event_type,
       COALESCE(v.flag_country, 'UNKNOWN') AS flag_country,
       l.high_seas_flag, l.mpa_flag, l.mpa_no_take_flag,
       COUNT(*) AS vessel_event_rows,
       COUNT(DISTINCT COALESCE(e.event_base_id, e.event_id)) AS distinct_events,
       ROUND(SUM(e.duration_hours), 1) AS total_hours
FROM project_dw.fact_vessel_event e
JOIN project_dw.dim_date     d ON d.sk_date     = e.sk_date
JOIN project_dw.dim_vessel   v ON v.sk_vessel   = e.sk_vessel
JOIN project_dw.dim_location l ON l.sk_location = e.sk_location
WHERE COALESCE(v.primary_vessel_type, '') <> 'GEAR' AND e.duration_hours <= 720
GROUP BY d.year, d.month_number, e.event_type, v.flag_country,
         l.high_seas_flag, l.mpa_flag, l.mpa_no_take_flag;