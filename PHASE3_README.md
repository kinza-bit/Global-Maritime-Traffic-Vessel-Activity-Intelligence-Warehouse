# Phase 3 – Gold Layer, OLAP Analytics, Query Optimization and Power BI Dashboard

**Project:** Global Maritime Traffic & Vessel Activity Intelligence Warehouse
**Course:** DL3004 Data Warehousing & Business Intelligence Lab – Final Project (Phase 3 = 30 marks)
**Data source:** Global Fishing Watch (GFW) API v3
**Database:** PostgreSQL 18 (local), database `maritime_dwbi`
**BI tool:** Power BI Desktop

---

## 1. Summary

Phase 3 turns the cleaned Silver data into a decision-support system. Work done:

1. Loaded the **Gold layer** (star schema: 4 dimensions and 3 facts) into PostgreSQL from the Silver tables, using the existing SQL files `01` to `06`.
2. Ran **30 data quality checks** on Gold. All critical checks pass.
3. Wrote **5 reporting views** in schema `project_reporting`.
4. Wrote **11 analytical queries** (SUM, AVG, COUNT, growth, percentiles, correlation).
5. Wrote **4 OLAP queries** (2 ROLLUP and 2 CUBE).
6. Ran **query optimization** tests with `EXPLAIN ANALYZE` before and after `CREATE INDEX`.
7. Built a **3-page Power BI dashboard** on the reporting views (Executive Overview, Detailed Analysis, Decision Page).

### Scope change from Phase 2
The Azure README originally limited the work to the Silver layer. Phase 3 requires dimensions, facts and BI, so **Gold and Phase 3 are now in scope**. This document describes that work.

### Where each layer runs
| Layer | Where it runs |
|---|---|
| Bronze (raw JSON Lines) | Azure Data Lake Storage Gen2 (`stmaritimeks`, container `bronze`) |
| Silver (Delta tables, schema `project_staging`) | Azure Databricks, container `silver` |
| Gold + reporting (PostgreSQL) | **Local PostgreSQL**, loaded from CSV exports of the Silver tables |
| Dashboard | Power BI Desktop, connected to the PostgreSQL views |

Gold runs on PostgreSQL because the Phase 3 PDF requires `EXPLAIN ANALYZE` and `CREATE INDEX`, which are PostgreSQL features, and the existing load scripts are written in PostgreSQL SQL.

---

## 2. Files in `sql/gold_postgres/`

| File | Purpose |
|---|---|
| `run_gold.sql` | Runs the whole Gold layer in order using `\ir` (creates schemas, then calls files 01 to 10) |
| `01_staging_schema.sql` | PostgreSQL staging tables (`project_staging`) |
| `02_dw_schema.sql` | Dimension and fact tables (`project_dw`), foreign-key indexes, and the `project_audit.dq_results` table |
| `03_load_staging_from_csv.sql` | Loads the 5 Silver CSV exports with `\copy` |
| `04_load_dimensions.sql` | Loads `dim_date`, `dim_vessel`, `dim_port`, `dim_location` with surrogate keys and a `-1` Unknown member |
| `05_load_facts.sql` | Loads facts with business-key to surrogate-key lookups |
| `06_dq_checks.sql` | 30 data quality checks written to `project_audit.dq_results` |
| `07_reporting_views.sql` | 5 reporting views and 1 helper view in `project_reporting` |
| `08_analytical_queries.sql` | 11 analytical queries |
| `09_olap_queries.sql` | ROLLUP and CUBE queries |
| `10_indexing_before_after.sql` | `EXPLAIN ANALYZE` before and after indexing, 3 tests |
| `indexing_results.txt` | Saved output of file 10 (evidence for the Indexing Report) |

The `export/` folder with the five CSV files is required to run file `03`. It is git-ignored (`*.csv`).

### How to run
```
cd sql/gold_postgres
psql -U postgres -d maritime_dwbi -f run_gold.sql
```
`run_gold.sql` stops at the first error. File `03` reads `export/<table>.csv` relative to the folder it is run from.

---

## 3. Gold layer (star schema)

### Dimensions (schema `project_dw`)
| Table | Rows loaded | Natural key | Surrogate key |
|---|---|---|---|
| `dim_date` | 5,479 (2012-01-01 to 2026-12-31) | `full_date` | `sk_date` (YYYYMMDD) |
| `dim_vessel` | 188,714 + Unknown | `vessel_id` | `sk_vessel` (identity) |
| `dim_port` | 840 + Unknown | `port_name` | `sk_port` (identity) |
| `dim_location` | 8 + Unknown | `location_id` (flag combinations) | `sk_location` (identity) |

The Unknown member (`sk = -1`) catches any fact row whose lookup fails, so no staging row is dropped.

### Facts (schema `project_dw`)
| Table | Rows | Grain |
|---|---|---|
| `fact_port_visit` | 296,199 | One completed port visit by one vessel at one anchorage |
| `fact_vessel_event` | 133,694 | One fishing, loitering or encounter segment for one vessel (encounters appear once per vessel) |
| `fact_ais_gap` | 39,056 | One AIS transponder gap for one vessel |

### Staging load check (from `03`)
`stg_port_visits` 296,199 · `stg_vessel_events` 133,694 · `stg_ais_gaps` 39,056 · `stg_vessels` 180,705 · `stg_rejects` 0.

`stg_rejects` is empty, meaning no row failed a critical rule in Silver.

### Surrogate key implementation
Business key to surrogate key mapping is done in `05_load_facts.sql` by joining each staging row to the dimension on its natural key (for example `dim_vessel.vessel_id = stg.vessel_id`). Unmatched keys fall back to `-1`. `ON CONFLICT (event_id) DO NOTHING` makes the load re-runnable.

---

## 4. Data quality validation (30 checks, `06_dq_checks.sql`)

Results are stored in `project_audit.dq_results`.

| Dimension | Checks | Result |
|---|---|---|
| Completeness | DQ-C01 to C07 | C01–C03, C05 pass. C04, C06, C07 are warnings (below) |
| Accuracy | DQ-A01 to A07 | All pass: staging and fact row counts match, `SUM(duration)` matches, every staging vessel exists in `dim_vessel` |
| Integrity | DQ-I01 to I11 | All pass: **0 fact rows without a matching dimension** for date, vessel, port and location, and 0 encounters missing the encountered vessel |
| Uniqueness | DQ-U01 to U05 | All pass: no duplicate `event_id`, `vessel_id` or `port_name` |

**Warnings (expected, to explain in the Data Quality Report):**
- **DQ-C04:** 29,799 port visits (about 10%) have no anchorage name, so they map to the Unknown port.
- **DQ-C06:** 8,009 vessels have no registry detail record (length, tonnage, power). About 96% do.
- **DQ-C07:** 50,874 vessels have no flag country.

---

## 5. Data findings that shaped the queries

These issues were found in the first query results and fixed before building the dashboard. They matter for how the numbers are reported.

| # | Finding | How it was handled |
|---|---|---|
| 1 | Port visits start as early as 2012. About 253,000 of 296,199 start between Dec 2024 and Mar 2025. The older rows are visits still open when the data was loaded, because the API returns events overlapping the date window. Average dwell time was about 25,000 hours and the maximum about 130,000 hours (roughly 15 years). | Port analysis uses `vw_port_visits_window`, which keeps visits starting on or after 2025-01-01. Dwell time is reported as **median** and **90th percentile**, not just average |
| 2 | Data covers only **Jan to Mar 2025** | No growth or seasonal claims. Page notes state the date range |
| 3 | The "dark fleet" top list was full of **AIS gear buoys** (names like `Float-2009-99%`, `Buoy-02 9v`, all with vessel type `GEAR`) | Excluded `primary_vessel_type = 'GEAR'` |
| 4 | The loitering top list contained gear buoys and oil platforms (FPSO, "Plataforma") | Excluded `GEAR`, platform name patterns, and events longer than 720 hours (30 days) |
| 5 | Encounters have one row per vessel (`.1`, `.2` ids), so they are double counted | Views include `distinct_events` using `event_base_id`. OLAP O3 counts vessel-event rows and says so |
| 6 | Some ports are unnamed anchorage codes (`CHN-1969`) | Flag `is_named_port`, and exclude them from rankings |
| 7 | Arrival hours are in **UTC**, not local port time | Stated in the reports |
| 8 | Singapore's average dwell is 340 hours but its median is 15.5 | Congestion is ranked by median and 90th percentile |
| 9 | Fishing, loitering and encounter totals (133,694) are very close to a 15,000 per month per type extraction cap (3 types × 3 months × 15,000 = 135,000) | Reports say these counts may be a **sample**. To confirm with the Bronze owner |

---

## 6. Reporting views (`07_reporting_views.sql`, schema `project_reporting`)

| View | Replaces PDF example | Content |
|---|---|---|
| `vw_port_visits_window` | helper | Port visits starting on or after 2025-01-01, joined to date, port and vessel; includes `is_named_port` |
| `vw_monthly_port_traffic` | `vw_monthly_sales` | Visits, average and median dwell by month and named port |
| `vw_vessel_analysis` | `vw_customer_analysis` | Vessel count and average length, tonnage and engine power by flag and vessel type (vessels with details only) |
| `vw_port_performance` | `vw_product_performance` | Visits, average, median and 90th percentile dwell, and % ended at dock, per named port |
| `vw_ais_gap_analysis` | extra | Gaps, intentional gaps and gap hours by month, flag and vessel (GEAR excluded) |
| `vw_vessel_events_analysis` | extra | Events by type, flag and protected-area flags (GEAR and events over 30 days excluded; `distinct_events` counts each encounter once) |

Row counts: `vw_monthly_port_traffic` 684 · `vw_vessel_analysis` 1,172 · `vw_port_performance` 327 · `vw_ais_gap_analysis` 2,531 · `vw_vessel_events_analysis` 1,581.

---

## 7. Analytical SQL (`08_analytical_queries.sql`)

Aggregations used: `SUM`, `AVG`, `COUNT`, `MAX`, `PERCENTILE_CONT` (median and 90th percentile), `CORR`, and `LAG()` for growth.

| # | Query | Answers business question |
|---|---|---|
| A1 | Monthly visits with growth % | Port traffic and monthly trend |
| A2 | Busiest named ports with average, median and p90 dwell | Dwell time and congestion |
| A3 | Arrivals by hour of day (UTC) | Peak arrival hours |
| A4 | Arrivals by day of week | Peak arrival days |
| A5 | Dwell time by vessel type | Fleet stay by vessel type |
| A6 | Top flag states by port visits | Flag state distribution |
| A7 | Correlation of length, tonnage and engine power by type | Fleet capacity profiling |
| A8 | Flag states by fishing hours | Flag state fishing effort |
| A9 | Events inside MPAs and no-take reserves | Protected waters intrusions |
| A10 | Loitering vessels (platforms, buoys and over-30-day events excluded) | Suspicious loitering |
| A11 | Intentional AIS disabling by vessel (GEAR excluded) | Dark fleet |

**Key results:**
- Monthly port visits (named and unnamed ports, window view): Jan 80,002 · Feb 69,438 · Mar 64,770. Median dwell stays near 27 hours.
- Busiest ports: Shanghai 37,353 visits, Busan 25,420, Singapore 15,305.
- Median dwell: Shanghai 53.7 h, Antwerp 40.3 h, Busan 34.2 h. 90th percentile: Busan 362 h, Hong Kong 312 h, Shanghai 252 h.
- Top flag states by visits: CHN 80,037 · KOR 29,270 · IDN 12,213 · NLD 12,195 · SGP 11,832.
- Fishing hours: CHN 238,288 (highest by far), then ESP 39,549 and KOR 37,796.
- Events in protected areas: fishing 1,480 of 44,971 (3.29%); in no-take reserves 47. Encounters 644 of 28,465 (2.26%); loitering 80 of 5,983 (1.34%).
- Arrivals peak between 03:00 and 08:00 UTC and are lowest around 17:00–19:00 UTC.
- Length correlates with engine power in every vessel type (0.51 to 0.87), and with tonnage strongly for cargo (0.87) but weakly for fishing vessels (0.26).

---

## 8. OLAP queries (`09_olap_queries.sql`)

| # | Operator | Hierarchy / dimensions | Output |
|---|---|---|---|
| O1 | **ROLLUP** | Flag country → vessel type | Type rows, flag totals, grand total |
| O2 | **ROLLUP** | Port country → port | Port rows, country totals, grand total |
| O3 | **CUBE** | Flag × event type | All combinations plus flag totals, type totals, grand total |
| O4 | **CUBE** | Port country × vessel type | All combinations plus totals |

Notes for the reports:
- Subtotals are labelled with `GROUPING()` (for example `FLAG TOTAL`, `COUNTRY TOTAL`, `GRAND TOTAL`, `ALL TYPES`).
- "Grand total" in O1 and "ALL FLAGS" in O3 cover only the **selected flags** in each query's filter, not the whole dataset.
- O3 counts vessel-event rows, so each encounter appears once per vessel.

---

## 9. Query optimization (`10_indexing_before_after.sql`)

Existing foreign-key indexes (`ix_fpv_*`, `ix_fag_*`, `ix_fve_*`) from `02_dw_schema.sql` do not help these filters, so three queries on unindexed columns were tested. Parallel workers were disabled for a fair comparison. Results come from the second run (warm cache) saved in `indexing_results.txt`.

| Test | Query | Index created | Plan | Cost | Execution time |
|---|---|---|---|---|---|
| 1 | Port visits in a one-week window, grouped by port | `ix_fpv_start_ts (start_ts)` | Seq Scan → Bitmap Index Scan | 12,581 → 8,748 (−31%) | 36.0 ms → 13.3 ms (**2.7× faster**) |
| 2 | Long intentional AIS gaps, grouped by vessel | `ix_fag_intentional_dur (intentional_disabling, duration_hours)` | Seq Scan → Bitmap Index Scan | 1,965 → 1,942 (−1%) | 6.7 ms → 7.1 ms (about 5% slower) |
| 3 | Vessels by flag and vessel type | `ix_dv_flag_type (flag_country, primary_vessel_type)` | Seq Scan → Bitmap Index Scan | 6,382 → 1,429 (−78%) | 14.5 ms → 2.8 ms (**5.3× faster**) |

**Conclusions**
- **Tests 1 and 3 benefit.** The filters select a small share of the table (about 6% and 1.5% of rows), so an index is worth using.
- **Test 2 does not benefit.** The filter matches 13,677 of 39,056 rows (35%), so the index saves almost nothing and adds a little overhead. The index `ix_fag_intentional_dur` was dropped.
- Test 1's "before" run read 412 disk blocks against 50 after, so part of that gain is cache warming.

---

## 10. Power BI dashboard

**Connection:** PostgreSQL database, server `localhost`, database `maritime_dwbi`, Import mode, using the five `project_reporting` views only.

**Measures:** `Port Visits`, `AIS Gaps`, `Intentional Gaps`, `Vessels With Details`, `Port Visits (ports)`, `Fishing Hours`, `Avg Length (m)`, `Fishing Events In MPA`, `Fishing Events In No-Take`, and optionally `Median Dwell Hrs`.

### Page 1 – Executive Overview
KPI cards (port visits, AIS gaps, vessels with registry details, and optionally intentional gaps and typical dwell time), a monthly port visits trend chart sorted by `month_number`, an optional top-10 ports chart and a port-country slicer. Notes: data covers Jan to Mar 2025, and vessel details exist for about 96% of vessels.

### Page 2 – Detailed Analysis
- **Port analysis:** top 10 ports by visits, busy-and-slow scatter chart (visits vs median dwell), port performance table.
- **Regional analysis:** visits by port country, top 10 flag states by fishing hours.
- **Vessel segmentation:** vessels by type (donut), average length by type.
- Note: dwell uses medians, event counts may be a sample, and vessel charts cover vessels with details only.

### Page 3 – Decision Page
Question: "Based on the dashboard, what decision should management take?"

> **Decision:** Port authorities should prioritise berth and anchorage capacity at **Shanghai, Busan and Hong Kong**. Shanghai's typical (median) stay is 53.7 hours, and Busan and Hong Kong have the longest slow stays (90th percentile of 362 and 312 hours). Separately, maritime enforcement should focus patrols on **no-take marine reserves**. Only **47 of 44,971 fishing events (0.10%)** occurred there, and **1,480 (3.29%)** occurred in any protected area, so a small, targeted patrol can cover the violations.

Evidence on the page: 90th percentile dwell chart for the top 10 ports, and cards for fishing events in MPAs and no-take reserves. A caveats box states that results are based on Jan to Mar 2025 only, that no traffic growth is claimed, that fishing counts may be a sample, and that buoys and events over 30 days are excluded.

**Why no growth recommendation:** the PDF's example uses a growth figure, but our data covers three months and the decline from January to March may partly reflect incomplete recent data, so a growth claim would not be supported.

---

## 11. Limitations (to include in the reports)

1. Only **Jan to Mar 2025** of event data. No seasonal analysis is possible.
2. Port visits starting before 2025 are excluded from port analysis because they are legacy open visits, not true visit durations.
3. Fishing, loitering and encounter counts may be capped by the extraction (15,000 per month per event type) and therefore are not guaranteed to be complete totals.
4. About 10% of port visits have no anchorage name. About 4% of vessels have no registry details. About 27% of vessels (50,874 of 188,714) have no flag country.
5. Arrival times are UTC.
6. The `dim_location` table holds only region flag combinations (high seas, MPA, no-take). EEZ and MPA names are not available in Silver.
7. Gear buoys (vessel type `GEAR`) are excluded from AIS gap and event analysis.

---

## 12. Phase 3 deliverables checklist

| Deliverable | Status | Location |
|---|---|---|
| Analytical SQL queries | ✅ Done | `sql/gold_postgres/08_analytical_queries.sql` |
| OLAP queries (ROLLUP, CUBE) | ✅ Done | `sql/gold_postgres/09_olap_queries.sql` |
| Reporting views | ✅ Done | `sql/gold_postgres/07_reporting_views.sql` |
| Index comparison evidence | ✅ Done | `10_indexing_before_after.sql`, `indexing_results.txt` |
| Indexing Report (PDF) | ⬜ To do | `reports/indexing_report.pdf` |
| Power BI dashboard (PBIX) | ⬜ Save file | `reports/maritime_dashboard.pbix` |
| Dashboard screenshots (PDF) | ⬜ To do | `reports/dashboard_screenshots.pdf` |
| Business Insights Report (PDF) | ⬜ To do | `reports/business_insights_report.pdf` |
| Final presentation (PPT) | ⬜ To do | `reports/final_presentation.pptx` |

### Still to do
- [ ] Add files `07` to `10` to `run_gold.sql`
- [ ] Add a comment in `09_olap_queries.sql` that O1 and O3 totals cover selected flags only
- [ ] Save the `.pbix` and take one screenshot per dashboard page
- [ ] Confirm with the Bronze owner whether the extraction cap applied
- [ ] Update the Azure README (Sections 3, 4, 21) to mark Gold and Phase 3 as in scope
- [ ] Write the Indexing Report, Business Insights Report and presentation
- [ ] Open a pull request with all files (never commit CSVs or credentials)

---

## 13. Team contributions (fill in)

| Member | Phase 3 work |
|---|---|
| Member 1 | |
| Member 2 | |
| Member 3 | |
| Member 4 | |
