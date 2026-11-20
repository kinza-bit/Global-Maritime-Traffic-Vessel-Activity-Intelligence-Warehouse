-- 10_indexing_before_after.sql (PostgreSQL) - Phase 3 query optimization
-- Each test: EXPLAIN ANALYZE before, CREATE INDEX, EXPLAIN ANALYZE after.
SET max_parallel_workers_per_gather = 0;   -- same plan style for fair comparison

-- Start clean so the "before" runs have no helping index
DROP INDEX IF EXISTS project_dw.ix_fpv_start_ts;
DROP INDEX IF EXISTS project_dw.ix_fag_intentional_dur;
DROP INDEX IF EXISTS project_dw.ix_dv_flag_type;
ANALYZE;

-- ===== TEST 1: port visits in one week, grouped by port =====
\echo '=== TEST 1 BEFORE ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT sk_port, COUNT(*) AS visits, ROUND(AVG(duration_hrs), 1) AS avg_dwell
FROM project_dw.fact_port_visit
WHERE start_ts >= '2025-02-01' AND start_ts < '2025-02-08'
GROUP BY sk_port;

CREATE INDEX ix_fpv_start_ts ON project_dw.fact_port_visit (start_ts);
ANALYZE project_dw.fact_port_visit;

\echo '=== TEST 1 AFTER ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT sk_port, COUNT(*) AS visits, ROUND(AVG(duration_hrs), 1) AS avg_dwell
FROM project_dw.fact_port_visit
WHERE start_ts >= '2025-02-01' AND start_ts < '2025-02-08'
GROUP BY sk_port;

-- ===== TEST 2: long intentional AIS gaps, grouped by vessel =====
\echo '=== TEST 2 BEFORE ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT sk_vessel, COUNT(*) AS long_gaps
FROM project_dw.fact_ais_gap
WHERE intentional_disabling AND duration_hours > 48
GROUP BY sk_vessel ORDER BY long_gaps DESC LIMIT 20;

CREATE INDEX ix_fag_intentional_dur ON project_dw.fact_ais_gap (intentional_disabling, duration_hours);
ANALYZE project_dw.fact_ais_gap;

\echo '=== TEST 2 AFTER ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT sk_vessel, COUNT(*) AS long_gaps
FROM project_dw.fact_ais_gap
WHERE intentional_disabling AND duration_hours > 48
GROUP BY sk_vessel ORDER BY long_gaps DESC LIMIT 20;

-- ===== TEST 3: vessels by flag and type =====
\echo '=== TEST 3 BEFORE ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT vessel_name, length_m, tonnage_gt
FROM project_dw.dim_vessel
WHERE flag_country = 'PAN' AND primary_vessel_type = 'CARGO';

CREATE INDEX ix_dv_flag_type ON project_dw.dim_vessel (flag_country, primary_vessel_type);
ANALYZE project_dw.dim_vessel;

\echo '=== TEST 3 AFTER ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT vessel_name, length_m, tonnage_gt
FROM project_dw.dim_vessel
WHERE flag_country = 'PAN' AND primary_vessel_type = 'CARGO';