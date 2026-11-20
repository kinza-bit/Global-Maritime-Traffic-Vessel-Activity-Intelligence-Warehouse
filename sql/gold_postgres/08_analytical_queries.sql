-- 08_analytical_queries.sql (PostgreSQL) - Phase 3 analytical SQL (v2)
-- Port analysis uses vw_port_visits_window (visits starting 2025-01-01 or later).

-- A1. Monthly port visits with growth %
WITH m AS (
  SELECT DATE_TRUNC('month', start_ts)::DATE AS month, COUNT(*) AS visits,
         ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs,
         ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS median_dwell_hrs
  FROM project_reporting.vw_port_visits_window GROUP BY 1)
SELECT month, visits, avg_dwell_hrs, median_dwell_hrs,
       LAG(visits) OVER (ORDER BY month) AS prev_month_visits,
       ROUND(100.0 * (visits - LAG(visits) OVER (ORDER BY month))
             / NULLIF(LAG(visits) OVER (ORDER BY month), 0), 1) AS growth_pct
FROM m ORDER BY month;

-- A2. Busiest named ports: visits and dwell time (congestion)
SELECT port_name, port_country, COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS median_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS p90_dwell_hrs
FROM project_reporting.vw_port_visits_window
WHERE is_named_port
GROUP BY port_name, port_country HAVING COUNT(*) >= 100
ORDER BY visits DESC LIMIT 15;

-- A3. Arrivals by hour of day (UTC)
SELECT EXTRACT(HOUR FROM start_ts)::INT AS arrival_hour_utc, COUNT(*) AS arrivals
FROM project_reporting.vw_port_visits_window GROUP BY 1 ORDER BY 1;

-- A4. Arrivals by day of week
SELECT day_of_week_num, day_of_week, COUNT(*) AS arrivals
FROM project_reporting.vw_port_visits_window
GROUP BY day_of_week_num, day_of_week ORDER BY day_of_week_num;

-- A5. Dwell time by vessel type
SELECT COALESCE(primary_vessel_type, 'UNKNOWN') AS vessel_type, COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs,
       ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hrs))::NUMERIC, 1) AS median_dwell_hrs
FROM project_reporting.vw_port_visits_window
GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY median_dwell_hrs DESC;

-- A6. Top flag states by port visits
SELECT COALESCE(flag_country, 'UNKNOWN') AS flag_country, COUNT(*) AS visits
FROM project_reporting.vw_port_visits_window
GROUP BY 1 ORDER BY visits DESC LIMIT 15;

-- A7. Fleet capacity: correlation of length, tonnage and engine power
SELECT COALESCE(primary_vessel_type, 'UNKNOWN') AS vessel_type, COUNT(*) AS vessels,
       ROUND(CORR(length_m, tonnage_gt)::NUMERIC, 3)      AS corr_length_tonnage,
       ROUND(CORR(length_m, engine_power_kw)::NUMERIC, 3) AS corr_length_power
FROM project_dw.dim_vessel
WHERE has_vessel_details AND length_m IS NOT NULL
  AND tonnage_gt IS NOT NULL AND engine_power_kw IS NOT NULL
GROUP BY 1 HAVING COUNT(*) >= 50 ORDER BY vessels DESC;

-- A8. Flag states with the most apparent fishing hours
SELECT COALESCE(v.flag_country, 'UNKNOWN') AS flag_country,
       COUNT(*) AS fishing_events, ROUND(SUM(e.duration_hours), 1) AS fishing_hours
FROM project_dw.fact_vessel_event e
JOIN project_dw.dim_vessel v ON v.sk_vessel = e.sk_vessel
WHERE e.event_type = 'FISHING'
GROUP BY 1 ORDER BY fishing_hours DESC LIMIT 15;

-- A9. Events inside protected areas (encounters counted once per encounter)
SELECT e.event_type,
       COUNT(DISTINCT COALESCE(e.event_base_id, e.event_id)) FILTER (WHERE l.mpa_flag)         AS events_in_mpa,
       COUNT(DISTINCT COALESCE(e.event_base_id, e.event_id)) FILTER (WHERE l.mpa_no_take_flag) AS events_in_no_take
FROM project_dw.fact_vessel_event e
JOIN project_dw.dim_location l ON l.sk_location = e.sk_location
GROUP BY e.event_type ORDER BY e.event_type;

-- A10. Loitering vessels (platforms/FPSOs excluded; events longer than 30 days excluded)
SELECT v.vessel_id, v.vessel_name, v.flag_country,
       COUNT(*) AS loitering_events, ROUND(SUM(e.duration_hours), 1) AS loitering_hours
FROM project_dw.fact_vessel_event e
JOIN project_dw.dim_vessel v ON v.sk_vessel = e.sk_vessel
WHERE e.event_type = 'LOITERING' AND v.sk_vessel <> -1
  AND e.duration_hours <= 720
  AND COALESCE(v.primary_vessel_type, '') <> 'GEAR'
  AND COALESCE(v.vessel_name, '') !~* '(fpso|fso|platform|plataforma)'
GROUP BY v.vessel_id, v.vessel_name, v.flag_country
ORDER BY loitering_hours DESC LIMIT 15;

-- A11. Dark fleet: intentional AIS disabling (aids to navigation, MMSI 99..., excluded)
SELECT v.vessel_id, v.vessel_name, v.flag_country, v.ssvid,
       COUNT(*) FILTER (WHERE g.intentional_disabling) AS intentional_gaps,
       COUNT(*) AS total_gaps, ROUND(AVG(g.duration_hours), 1) AS avg_gap_hours
FROM project_dw.fact_ais_gap g
JOIN project_dw.dim_vessel v ON v.sk_vessel = g.sk_vessel
WHERE v.sk_vessel <> -1 AND COALESCE(v.primary_vessel_type, '') <> 'GEAR'
GROUP BY v.vessel_id, v.vessel_name, v.flag_country, v.ssvid
ORDER BY intentional_gaps DESC, total_gaps DESC LIMIT 15;