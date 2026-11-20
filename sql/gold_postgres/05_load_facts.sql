-- 05_load_facts.sql  (PostgreSQL)
-- Business key -> surrogate key mapping. Run AFTER 04_load_dimensions.sql.
-- Every staging row is loaded: an unmatched lookup falls back to the Unknown
-- member (-1) instead of dropping the row, so Silver rows = fact rows.
-- Re-runnable: ON CONFLICT (event_id) DO NOTHING.

-- -------------------------------------------------------------- Fact_Port_Visit
-- Port visits carry no MPA info, so mpa / mpa_no_take are FALSE for the lookup.
INSERT INTO project_dw.fact_port_visit
  (event_id, visit_id, sk_date, sk_vessel, sk_port, sk_location,
   start_ts, end_ts, duration_hrs, confidence, end_at_dock,
   start_dist_port_km, end_dist_port_km, start_dist_shore_km, end_dist_shore_km,
   silver_run_id)
SELECT s.event_id, s.visit_id,
       d.sk_date,
       COALESCE(v.sk_vessel, -1),
       COALESCE(p.sk_port, -1),
       COALESCE(l.sk_location, -1),
       s.start_ts, s.end_ts, s.duration_hrs, s.confidence, s.end_at_dock,
       s.start_dist_port_km, s.end_dist_port_km, s.start_dist_shore_km, s.end_dist_shore_km,
       s._run_id
FROM project_staging.stg_port_visits s
JOIN      project_dw.dim_date     d ON d.full_date  = s.start_date
LEFT JOIN project_dw.dim_vessel   v ON v.vessel_id  = s.vessel_id
LEFT JOIN project_dw.dim_port     p ON p.port_name  = s.end_anchorage_name
LEFT JOIN project_dw.dim_location l ON l.high_seas_flag   = COALESCE(s.high_seas, FALSE)
                                   AND l.mpa_flag         = FALSE
                                   AND l.mpa_no_take_flag = FALSE
ON CONFLICT (event_id) DO NOTHING;

-- ---------------------------------------------------------------- Fact_AIS_Gap
INSERT INTO project_dw.fact_ais_gap
  (event_id, sk_date, sk_vessel, sk_location, start_ts, end_ts,
   duration_hours, distance_km, implied_speed_knots, intentional_disabling,
   off_lat, off_lon, on_lat, on_lon, positions_per_day_sat_reception, silver_run_id)
SELECT s.event_id,
       d.sk_date,
       COALESCE(v.sk_vessel, -1),
       COALESCE(l.sk_location, -1),
       s.start_ts, s.end_ts,
       s.duration_hours, s.distance_km, s.implied_speed_knots, s.intentional_disabling,
       s.off_lat, s.off_lon, s.on_lat, s.on_lon, s.positions_per_day_sat_reception,
       s._run_id
FROM project_staging.stg_ais_gaps s
JOIN      project_dw.dim_date     d ON d.full_date = s.start_date
LEFT JOIN project_dw.dim_vessel   v ON v.vessel_id = s.vessel_id
LEFT JOIN project_dw.dim_location l ON l.high_seas_flag   = COALESCE(s.high_seas, FALSE)
                                   AND l.mpa_flag         = COALESCE(s.mpa, FALSE)
                                   AND l.mpa_no_take_flag = COALESCE(s.mpa_no_take, FALSE)
ON CONFLICT (event_id) DO NOTHING;

-- ----------------------------------------------------------- Fact_Vessel_Event
INSERT INTO project_dw.fact_vessel_event
  (event_id, event_base_id, event_type, sk_date, sk_vessel, sk_encountered_vessel,
   sk_location, start_ts, end_ts, lat, lon, duration_hours, avg_speed_knots,
   total_distance_km, avg_dist_shore_km, start_dist_shore_km, end_dist_shore_km,
   start_dist_port_km, end_dist_port_km, potential_risk, encounter_type,
   encounter_median_speed_knots, encounter_median_distance_km, silver_run_id)
SELECT s.event_id, s.event_base_id, s.event_type,
       d.sk_date,
       COALESCE(v.sk_vessel, -1),
       ev.sk_vessel,                           -- NULL unless an encounter
       COALESCE(l.sk_location, -1),
       s.start_ts, s.end_ts, s.lat, s.lon, s.duration_hours, s.avg_speed_knots,
       s.total_distance_km, s.avg_dist_shore_km, s.start_dist_shore_km, s.end_dist_shore_km,
       s.start_dist_port_km, s.end_dist_port_km, s.potential_risk, s.encounter_type,
       s.encounter_median_speed_knots, s.encounter_median_distance_km, s._run_id
FROM project_staging.stg_vessel_events s
JOIN      project_dw.dim_date     d  ON d.full_date  = s.start_date
LEFT JOIN project_dw.dim_vessel   v  ON v.vessel_id  = s.vessel_id
LEFT JOIN project_dw.dim_vessel   ev ON ev.vessel_id = s.encountered_vessel_id
LEFT JOIN project_dw.dim_location l  ON l.high_seas_flag   = COALESCE(s.high_seas, FALSE)
                                    AND l.mpa_flag         = COALESCE(s.mpa, FALSE)
                                    AND l.mpa_no_take_flag = COALESCE(s.mpa_no_take, FALSE)
ON CONFLICT (event_id) DO NOTHING;

ANALYZE project_dw.fact_port_visit;
ANALYZE project_dw.fact_ais_gap;
ANALYZE project_dw.fact_vessel_event;
