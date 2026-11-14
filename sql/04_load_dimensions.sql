-- 04_load_dimensions.sql  (PostgreSQL)
-- Loads project_dw dimensions from project_staging. Re-runnable (upserts).
-- Business key -> surrogate key: the SK is generated here; facts look it up later.

-- ------------------------------------------------------------------ Dim_Date
-- Covers every start_date in Silver (port visits start as early as 2012).
INSERT INTO project_dw.dim_date
  (sk_date, full_date, day_of_week_num, day_of_week, day_of_month,
   month_number, month_name, quarter, year, fiscal_year)
SELECT TO_CHAR(d, 'YYYYMMDD')::INT,
       d::DATE,
       EXTRACT(ISODOW  FROM d)::SMALLINT,
       TRIM(TO_CHAR(d, 'Day')),
       EXTRACT(DAY     FROM d)::SMALLINT,
       EXTRACT(MONTH   FROM d)::SMALLINT,
       TRIM(TO_CHAR(d, 'Month')),
       EXTRACT(QUARTER FROM d)::SMALLINT,
       EXTRACT(YEAR    FROM d)::SMALLINT,
       (EXTRACT(YEAR FROM d) + CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN 1 ELSE 0 END)::SMALLINT
FROM generate_series('2012-01-01'::DATE, '2026-12-31'::DATE, INTERVAL '1 day') AS g(d)
ON CONFLICT (sk_date) DO NOTHING;

-- ---------------------------------------------------------------- Dim_Vessel
-- Unknown member (for any fact row whose vessel cannot be found)
INSERT INTO project_dw.dim_vessel (sk_vessel, vessel_id, vessel_name, flag_country, primary_vessel_type)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN', 'Unknown', NULL, NULL)
ON CONFLICT (vessel_id) DO NOTHING;

-- Every vessel seen in any event table (incl. encountered vessels), enriched
-- with stg_vessels details where they exist (only ~8,000 vessels are enriched).
WITH seen AS (
  SELECT vessel_id, vessel_name, ssvid, vessel_flag, vessel_type FROM project_staging.stg_port_visits
  UNION ALL
  SELECT vessel_id, vessel_name, ssvid, vessel_flag, vessel_type FROM project_staging.stg_vessel_events
  UNION ALL
  SELECT vessel_id, vessel_name, ssvid, vessel_flag, vessel_type FROM project_staging.stg_ais_gaps
  UNION ALL
  SELECT encountered_vessel_id, encountered_vessel_name, NULL, encountered_vessel_flag, encountered_vessel_type
  FROM project_staging.stg_vessel_events WHERE encountered_vessel_id IS NOT NULL
),
ev AS (
  SELECT vessel_id,
         MAX(NULLIF(vessel_name, 'Unknown')) AS vessel_name,
         MAX(ssvid)                          AS ssvid,
         MAX(vessel_flag)                    AS vessel_flag,
         MAX(vessel_type)                    AS vessel_type
  FROM seen GROUP BY vessel_id
),
ids AS (
  SELECT vessel_id FROM ev
  UNION
  SELECT vessel_id FROM project_staging.stg_vessels
)
INSERT INTO project_dw.dim_vessel
  (vessel_id, vessel_name, ssvid, imo, callsign, flag_country, primary_vessel_type,
   length_m, tonnage_gt, engine_power_kw, max_speed_kn, has_vessel_details)
SELECT ids.vessel_id,
       COALESCE(NULLIF(sv.vessel_name, 'Unknown'), ev.vessel_name, 'Unknown'),
       COALESCE(sv.ssvid, ev.ssvid),
       sv.imo,
       sv.callsign,
       COALESCE(sv.vessel_flag, ev.vessel_flag),
       COALESCE(ev.vessel_type, NULLIF(SPLIT_PART(sv.ship_types, '|', 1), '')),
       sv.length_m, sv.tonnage_gt, sv.engine_power_kw, sv.max_speed_kn,
       (sv.vessel_id IS NOT NULL)
FROM ids
LEFT JOIN ev                          ON ev.vessel_id = ids.vessel_id
LEFT JOIN project_staging.stg_vessels sv ON sv.vessel_id = ids.vessel_id
ON CONFLICT (vessel_id) DO UPDATE SET
  vessel_name         = EXCLUDED.vessel_name,
  ssvid               = EXCLUDED.ssvid,
  imo                 = EXCLUDED.imo,
  callsign            = EXCLUDED.callsign,
  flag_country        = EXCLUDED.flag_country,
  primary_vessel_type = EXCLUDED.primary_vessel_type,
  length_m            = EXCLUDED.length_m,
  tonnage_gt          = EXCLUDED.tonnage_gt,
  engine_power_kw     = EXCLUDED.engine_power_kw,
  max_speed_kn        = EXCLUDED.max_speed_kn,
  has_vessel_details  = EXCLUDED.has_vessel_details,
  dw_load_ts          = now();

-- ------------------------------------------------------------------ Dim_Port
INSERT INTO project_dw.dim_port (sk_port, port_name, anchorage_name, port_country)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'Unknown', 'Unknown', NULL)
ON CONFLICT (port_name) DO NOTHING;

INSERT INTO project_dw.dim_port (port_name, anchorage_name, port_country, latitude, longitude)
SELECT end_anchorage_name,
       end_anchorage_name,
       MAX(end_anchorage_flag),
       ROUND(AVG(end_anchorage_lat), 6),
       ROUND(AVG(end_anchorage_lon), 6)
FROM project_staging.stg_port_visits
WHERE end_anchorage_name IS NOT NULL
GROUP BY end_anchorage_name
ON CONFLICT (port_name) DO UPDATE SET
  port_country = EXCLUDED.port_country,
  latitude     = EXCLUDED.latitude,
  longitude    = EXCLUDED.longitude,
  dw_load_ts   = now();

-- -------------------------------------------------------------- Dim_Location
-- Silver only keeps region flags (not EEZ / MPA names), so the dimension is
-- the full set of high_seas x mpa x mpa_no_take combinations (8 rows) + Unknown.
INSERT INTO project_dw.dim_location (sk_location, location_id)
OVERRIDING SYSTEM VALUE
VALUES (-1, 'UNKNOWN')
ON CONFLICT (location_id) DO NOTHING;

INSERT INTO project_dw.dim_location (location_id, high_seas_flag, mpa_flag, mpa_no_take_flag)
SELECT 'HS' || h::INT || '-MPA' || m::INT || '-NT' || n::INT, h, m, n
FROM (VALUES (FALSE), (TRUE)) AS hs(h)
CROSS JOIN (VALUES (FALSE), (TRUE)) AS mp(m)
CROSS JOIN (VALUES (FALSE), (TRUE)) AS nt(n)
ON CONFLICT (location_id) DO NOTHING;
