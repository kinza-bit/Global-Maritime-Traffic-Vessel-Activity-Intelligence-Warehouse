-- 09_olap_queries.sql (PostgreSQL) - Phase 3 OLAP: ROLLUP and CUBE

-- O1. ROLLUP: Flag -> Vessel type (port visits since 2025-01-01)
-- Output: type total, flag total, grand total
WITH base AS (
  SELECT COALESCE(flag_country, 'UNKNOWN') AS flag_country,
         COALESCE(primary_vessel_type, 'UNKNOWN') AS vessel_type,
         duration_hrs
  FROM project_reporting.vw_port_visits_window
  WHERE COALESCE(flag_country, 'UNKNOWN') IN ('CHN','KOR','SGP','NLD','IDN')
)
SELECT CASE WHEN GROUPING(flag_country) = 1 THEN 'GRAND TOTAL' ELSE flag_country END AS flag_country,
       CASE WHEN GROUPING(vessel_type) = 1
            THEN CASE WHEN GROUPING(flag_country) = 1 THEN '-' ELSE 'FLAG TOTAL' END
            ELSE vessel_type END AS vessel_type,
       COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs
FROM base
GROUP BY ROLLUP (flag_country, vessel_type)
ORDER BY GROUPING(flag_country), flag_country, GROUPING(vessel_type), visits DESC;

-- O2. ROLLUP: Port country -> Port (named ports with 2,000+ visits)
WITH base AS (
  SELECT port_country, port_name, duration_hrs
  FROM project_reporting.vw_port_visits_window
  WHERE is_named_port
    AND port_name IN (SELECT port_name FROM project_reporting.vw_port_visits_window
                      WHERE is_named_port GROUP BY port_name HAVING COUNT(*) >= 2000)
)
SELECT CASE WHEN GROUPING(port_country) = 1 THEN 'GRAND TOTAL' ELSE port_country END AS port_country,
       CASE WHEN GROUPING(port_name) = 1
            THEN CASE WHEN GROUPING(port_country) = 1 THEN '-' ELSE 'COUNTRY TOTAL' END
            ELSE port_name END AS port_name,
       COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs
FROM base
GROUP BY ROLLUP (port_country, port_name)
ORDER BY GROUPING(port_country), port_country, GROUPING(port_name), visits DESC;

-- O3. CUBE: Flag x Event type (fishing / loitering / encounter), total hours
WITH base AS (
  SELECT COALESCE(v.flag_country, 'UNKNOWN') AS flag_country,
         e.event_type, e.duration_hours
  FROM project_dw.fact_vessel_event e
  JOIN project_dw.dim_vessel v ON v.sk_vessel = e.sk_vessel
  WHERE COALESCE(v.primary_vessel_type, '') <> 'GEAR'
    AND COALESCE(v.flag_country, 'UNKNOWN') IN ('CHN','KOR','ESP','TWN','JPN','VNM')
    AND e.duration_hours <= 720
)
SELECT CASE WHEN GROUPING(flag_country) = 1 THEN 'ALL FLAGS' ELSE flag_country END AS flag_country,
       CASE WHEN GROUPING(event_type) = 1 THEN 'ALL TYPES' ELSE event_type END AS event_type,
       COUNT(*) AS events,
       ROUND(SUM(duration_hours), 1) AS total_hours
FROM base
GROUP BY CUBE (flag_country, event_type)
ORDER BY GROUPING(flag_country), flag_country, GROUPING(event_type), event_type;

-- O4. CUBE: Port country x Vessel type (average dwell and visits)
WITH base AS (
  SELECT port_country, COALESCE(primary_vessel_type, 'UNKNOWN') AS vessel_type, duration_hrs
  FROM project_reporting.vw_port_visits_window
  WHERE is_named_port AND port_country IN ('CHN','KOR','SGP','NLD')
    AND COALESCE(primary_vessel_type, 'UNKNOWN') IN ('CARGO','PASSENGER','FISHING','OTHER_NON_FISHING')
)
SELECT CASE WHEN GROUPING(port_country) = 1 THEN 'ALL COUNTRIES' ELSE port_country END AS port_country,
       CASE WHEN GROUPING(vessel_type) = 1 THEN 'ALL TYPES' ELSE vessel_type END AS vessel_type,
       COUNT(*) AS visits,
       ROUND(AVG(duration_hrs), 1) AS avg_dwell_hrs
FROM base
GROUP BY CUBE (port_country, vessel_type)
ORDER BY GROUPING(port_country), port_country, GROUPING(vessel_type), vessel_type;