# Silver Data Dictionary (`project_staging`)

Owner: Member 3. Covers all five Silver staging tables:
`stg_port_visits`, `stg_vessel_events`, `stg_rejects`,
`stg_ais_gaps`, and `stg_vessels`.

Schemas live in `schemas/silver_schemas.py`; this file is the
human-readable contract (C3).

Status: **v2 (final for Phase 2 runs)**. Verified against Bronze
2025-01 to 2025-03 by full Databricks runs where noted.
Items marked **(proposal)** still need team sign-off; remove the
tag once the team agrees.

## 1. Global rules

| Topic | Rule |
|---|---|
| Reading Bronze | Explicit schema only (never `inferSchema`). Scalars read as STRING, then cast. `_source_file` comes from the file path. |
| Timestamps | ISO 8601 UTC strings to `TIMESTAMP`. Unparseable becomes NULL and is counted; NULL `start_ts` or `end_ts` goes to `stg_rejects`. |
| Numbers | `DECIMAL(10,2)` measures, `DECIMAL(10,6)` coordinates. Unparseable becomes NULL and is counted. **A real `0` stays `0`.** |
| Missing measures | Stay `NULL`. Never replaced with `0`. |
| Text cleaning | Trim, collapse repeated spaces, empty string becomes NULL. |
| UPPER columns | `vessel_flag`, `vessel_type`, `event_type`, `encounter_type`, `end_anchorage_flag`, `encountered_vessel_flag`, `encountered_vessel_type`. |
| Vessel names | Trim and collapse spaces, then UPPER *(proposal)*. Names carry identifiers and symbols (`NO.8`, `YU7635`, `>`, `&`), so title case would damage them. NULL or empty becomes `'Unknown'`, and the count goes to the audit. |
| Anchorage names | Title case, except a token that contains a digit stays as is *(proposal)*: `ROTTERDAM MAASVLAKTE` becomes `Rotterdam Maasvlakte`, `CHN-1969` stays `CHN-1969`. A NULL name stays NULL (DQ-I02, warning). |
| Missing flag | `vessel_flag` stays `NULL` *(proposal)*; reported as a coverage warning. |
| Booleans | Missing becomes `FALSE` and is counted. Region flags are `size(array) > 0`; an empty array is `FALSE`. |
| De-duplication | One row per `event_id`. Keep, in order: (1) latest window month from the file name (`..._2025-03.jsonl`), (2) `load_type=incremental` over `initial`, (3) file path as a final tie-break. The number removed is recorded in the audit. `_bronze_ingest_ts` cannot decide because every file has `ingest_date=2026-11-12`. |
| Writes | Delta, MERGE on the key. A re-run gives the same counts. |
| Audit columns | On every row: `_source_file`, `_bronze_ingest_ts`, `_silver_load_ts`, `_run_id`. |
| No surrogate keys | Natural keys only. Gold-layer work is out of scope. |

## 2. Reject rules (rows go to `stg_rejects` with a reason)

| Reason | Condition |
|---|---|
| `DQ-C01` | `event_id` NULL or empty |
| `DQ-C02` | `vessel_id` NULL or empty |
| `DQ-C03` | `start_ts` or `end_ts` NULL or unparseable |
| `DQ-V01` | duration < 0 *(no upper cap in Silver *(proposal)*: about 21% of port visits exceed one year)* |
| `DQ-V02` | `end_ts` < `start_ts` |
| `DQ-V03` | lat outside [-90, 90] or lon outside [-180, 180] |
| `DQ-V04` | `event_type` not one of the expected values |

Result of the full runs: **0 rejects** for `stg_port_visits` and `stg_vessel_events`, so `stg_rejects`
is empty. The reject path is proven by unit tests with synthetic bad rows
(`tests/test_transforms.py`, `tests/test_vessel_events_logic.py`).

## 3. `stg_port_visits` (key `event_id`; 29 columns)

Grain: one port visit by one vessel. Source: `port_visit` events. Rows: 296,199 unique ids.

| Column | Type | Null | Source path | Rule | Example |
|---|---|---|---|---|---|
| event_id | STRING | no | `id` | key; reject if missing | `3622b6ae62ea0ad2412d92607d1f151c` |
| visit_id | STRING | yes | `port_visit.visitId` | trim | `2586176bd5d6cf746a731ea2de4b0718` |
| vessel_id | STRING | no | `vessel.id` | reject if missing | `09feaccf9-92d3-585c-56e3-422d12d53735` |
| vessel_name | STRING | yes | `vessel.name` | name rule; missing becomes `Unknown` | `PAGADDER` |
| ssvid | STRING | yes | `vessel.ssvid` | trim, keep as string | `205367990` |
| vessel_flag | STRING | yes | `vessel.flag` | UPPER; NULL if missing | `BEL` |
| vessel_type | STRING | yes | `vessel.type` | UPPER | `OTHER_NON_FISHING` |
| start_ts | TIMESTAMP | no | `start` | UTC | `2012-01-01 08:37:48` |
| end_ts | TIMESTAMP | no | `end` | UTC; must be >= start_ts | `2026-02-15 07:48:17` |
| start_date | DATE | yes | derived | `to_date(start_ts)` | `2012-01-01` |
| duration_hrs | DECIMAL(10,2) | yes | `port_visit.durationHrs` | >= 0; large values are valid | `123815.17` |
| confidence | INT | yes | `port_visit.confidence` | cast from string; values 2, 3, 4 | `3` |
| lat | DECIMAL(10,6) | yes | `position.lat` | range check | `51.360900` |
| lon | DECIMAL(10,6) | yes | `position.lon` | range check | `4.279700` |
| start_anchorage_name | STRING | yes | `port_visit.startAnchorage.name` | anchorage name rule | `Antwerp` |
| end_anchorage_name | STRING | yes | `port_visit.endAnchorage.name` | anchorage name rule; about 9% NULL | `Antwerp` |
| end_anchorage_flag | STRING | yes | `port_visit.endAnchorage.flag` | UPPER | `BEL` |
| end_anchorage_lat | DECIMAL(10,6) | yes | `port_visit.endAnchorage.lat` | range check | `51.360940` |
| end_anchorage_lon | DECIMAL(10,6) | yes | `port_visit.endAnchorage.lon` | range check | `4.279705` |
| end_at_dock | BOOLEAN | yes | `port_visit.endAnchorage.atDock` | missing becomes FALSE | `true` |
| start_dist_port_km | DECIMAL(10,2) | yes | `distances.startDistanceFromPortKm` | >= 0; `0` stays `0` | `0.00` |
| end_dist_port_km | DECIMAL(10,2) | yes | `distances.endDistanceFromPortKm` | >= 0 | `0.00` |
| start_dist_shore_km | DECIMAL(10,2) | yes | `distances.startDistanceFromShoreKm` | >= 0 | `0.00` |
| end_dist_shore_km | DECIMAL(10,2) | yes | `distances.endDistanceFromShoreKm` | >= 0 | `0.00` |
| high_seas | BOOLEAN | yes | `regions.highSeas` | non-empty array means TRUE | `false` |
| _source_file, _bronze_ingest_ts, _silver_load_ts, _run_id | see schema | | | audit columns | |

## 4. `stg_vessel_events` (key `event_id`; 36 columns)

Grain: one activity segment. Source: `fishing`, `loitering`, `encounter` events (`event_type` column).
Rows: 133,694 unique ids (44,995 fishing + 43,752 loitering + 44,947 encounter).

| Column | Type | Null | Source path | Rule | Example |
|---|---|---|---|---|---|
| event_id | STRING | no | `id` | key; encounter ids end in `.1` or `.2` and are kept whole | `cefb3506d1c7a41fd97c64d0ad8f7df5.1` |
| event_base_id | STRING | yes | derived | `id` without the `.1`/`.2` suffix; each encounter is stored once per vessel | `cefb3506d1c7a41fd97c64d0ad8f7df5` |
| event_type | STRING | no | `type` | UPPER; `FISHING`, `LOITERING`, `ENCOUNTER` | `FISHING` |
| vessel_id | STRING | no | `vessel.id` | reject if missing | `0ca293acd-d1d8-1658-0421-5fa99bd60bce` |
| vessel_name | STRING | yes | `vessel.name` | name rule; missing becomes `Unknown` | `TAIKO MARU NO.8` |
| ssvid | STRING | yes | `vessel.ssvid` | trim | `432971000` |
| vessel_flag | STRING | yes | `vessel.flag` | UPPER; NULL if missing (57% of loitering rows) | `JPN` |
| vessel_type | STRING | yes | `vessel.type` | UPPER | `FISHING` |
| start_ts | TIMESTAMP | no | `start` | UTC | `2025-01-28 07:55:18` |
| end_ts | TIMESTAMP | no | `end` | UTC; >= start_ts | `2025-03-03 07:11:00` |
| start_date | DATE | yes | derived | `to_date(start_ts)` | `2025-01-28` |
| lat | DECIMAL(10,6) | yes | `position.lat` | range check | `8.444800` |
| lon | DECIMAL(10,6) | yes | `position.lon` | range check | `-31.589300` |
| duration_hours | DECIMAL(10,2) | yes | loitering: `loitering.totalTimeHours`; fishing and encounter: derived | Fishing and encounter: `(end_ts - start_ts)` in hours, because `fishing.averageDurationHours` is NULL in all rows and encounters have no duration field. Loitering: source value. Never rejected for a missing source duration. | `815.26` |
| avg_speed_knots | DECIMAL(10,2) | yes | `fishing.averageSpeedKnots` / `loitering.averageSpeedKnots` | NULL for encounters; >= 0 | `5.14` |
| total_distance_km | DECIMAL(10,2) | yes | `fishing.totalDistanceKm` / `loitering.totalDistanceKm` | NULL for encounters | `8032.78` |
| avg_dist_shore_km | DECIMAL(10,2) | yes | `loitering.averageDistanceFromShoreKm` | loitering only | `90.00` |
| potential_risk | BOOLEAN | yes | `fishing.potentialRisk` / `encounter.potentialRisk` | missing becomes FALSE | `false` |
| encounter_type | STRING | yes | `encounter.type` | UPPER; encounters only | `FISHING-FISHING` |
| encounter_median_speed_knots | DECIMAL(10,2) | yes | `encounter.medianSpeedKnots` | encounters only | `1.22` |
| encounter_median_distance_km | DECIMAL(10,2) | yes | `encounter.medianDistanceKilometers` | distance **between** the two vessels, not distance travelled | `0.01` |
| encountered_vessel_id | STRING | yes | `encounter.vessel.id` | encounters only | `8b9349865-54f8-7e6d-0d89-d4426a9a681d` |
| encountered_vessel_name | STRING | yes | `encounter.vessel.name` | name rule | `FU YUAN YU7635` |
| encountered_vessel_flag | STRING | yes | `encounter.vessel.flag` | UPPER | `CHN` |
| encountered_vessel_type | STRING | yes | `encounter.vessel.type` | UPPER | `FISHING` |
| start_dist_shore_km | DECIMAL(10,2) | yes | `distances.startDistanceFromShoreKm` | >= 0 | `902.00` |
| end_dist_shore_km | DECIMAL(10,2) | yes | `distances.endDistanceFromShoreKm` | >= 0 | `1175.00` |
| start_dist_port_km | DECIMAL(10,2) | yes | `distances.startDistanceFromPortKm` | >= 0 | `928.09` |
| end_dist_port_km | DECIMAL(10,2) | yes | `distances.endDistanceFromPortKm` | >= 0 | `1204.42` |
| high_seas | BOOLEAN | yes | `regions.highSeas` | non-empty array means TRUE | `true` |
| mpa | BOOLEAN | yes | `regions.mpa` | non-empty array means TRUE | `false` |
| mpa_no_take | BOOLEAN | yes | `regions.mpaNoTake` | non-empty array means TRUE | `false` |
| _source_file, _bronze_ingest_ts, _silver_load_ts, _run_id | see schema | | | audit columns | |

### Decisions for `stg_vessel_events`

| # | Topic | Decision | Reason |
|---|---|---|---|
| D1 | Fishing and encounter duration | Derived as `end_ts - start_ts` (hours). Loitering uses `loitering.totalTimeHours`. | Source duration is always NULL for fishing and encounter. Rejecting them would drop every row. |
| D2 | Encounter `.1` / `.2` rows | **Both kept** as separate rows. `event_id` keeps the suffix; `event_base_id` links the pair. | The API stores one row per vessel in an encounter. The business questions need per-vessel flag and type. Gold can pair them through `event_base_id`. |
| D3 | Events starting before 2025 | **Kept**, not rejected. Reported as a DQ-V07 warning. | The API window matches overlap, not start date (about a third of rows start earlier). |
| D4 | Missing vessel flag | Stays `NULL` (34,484 vessel-event rows). Not rejected. | 57% of loitering rows have no flag. Reported as coverage. |
| D5 | Missing vessel name | Becomes `'Unknown'` (6,005 vessel-event rows, 91,500 port-visit rows). Counted in audit. | Team data dictionary rule. |
| D6 | Casing | Flags, types, event and encounter types: UPPER. Vessel names: trim and UPPER. Anchorage names: title case, tokens containing a digit unchanged. | Names carry identifiers and symbols; title case would damage them. |

## 5. `stg_rejects` (key `reject_id`; 8 columns)

| Column | Type | Null | Rule |
|---|---|---|---|
| reject_id | STRING | no | unique per rejected row (entity + event id + reason) |
| entity | STRING | no | `port_visit`, `fishing`, `loitering`, `encounter` |
| reject_reason | STRING | no | one of the codes in section 2 |
| raw_json | STRING | yes | the original Bronze line |
| audit columns | | | as in section 1 |

## 6. Known data facts (Bronze 2025-01..03)

- Port visits: 381,055 rows, 296,199 unique ids (84,856 duplicate rows). Copies come from different chunk files and have identical `end` and duration.
- Vessel events: 135,000 rows (45,000 per type), 133,694 unique ids (1,306 duplicate rows).
- The API window matches events that overlap it, not events that start in it: about a third of port visits start before 2025.
- The three worldwide event types are capped at 15,000 per month (45,000 rows each), so they are a sample.
- Missing vessel name: about 24% of port-visit rows. Missing vessel flag: 7% of port visits, 57% of loitering rows.
- Encounters appear twice (`.1`, `.2`), once per vessel.
- Open from README Appendix Q10: `Callsign` and `Max_Speed_KN` exist only in the proposal DDL; they are not in these two tables.

## 7. Run results (Databricks, `dry_run=false`)

| Metric | `stg_port_visits` | `stg_vessel_events` |
|---|---|---|
| Run id | ee6fc2a8-2532-4b09-9df1-9d53d457b120 | 826a191f-50bb-4c69-89cb-d990afc84313 |
| Rows in (Bronze) | 381,055 | 135,000 |
| Duplicates removed | 84,856 | 1,306 |
| Unique after dedupe | 296,199 | 133,694 |
| Rejects | 0 | 0 |
| Rows out (Silver) | 296,199 | 133,694 |
| Reconciled (Bronze ids = Silver + rejects) | True | True |
| Re-run row count | unchanged | 133,694 (unchanged) |
| `vessel_name` set to `'Unknown'` | 91,500 | 6,005 |
| Missing `vessel_flag` (kept NULL) | 28,003 | 34,484 |
| Missing `end_anchorage_name` (kept NULL) | 34,096 | n/a |
| Missing / unparseable durations | 0 / 0 | 0 / 0 |

These must match `project_audit.pipeline_runs` once Member 4's logger is wired in. Update this table after any re-run.
## 8. `stg_ais_gaps` (key `event_id`; 25 columns)

Grain: one AIS gap event. Source: `gap` events.

| Column | Type | Null | Source path | Rule |
|---|---|---|---|---|
| event_id | STRING | no | `id` | Natural merge key |
| vessel_id | STRING | no | `vessel.id` | Required |
| vessel_name | STRING | yes | `vessel.name` | Trim and UPPER; missing becomes `Unknown` |
| ssvid | STRING | yes | `vessel.ssvid` | Keep as string |
| vessel_flag | STRING | yes | `vessel.flag` | UPPER |
| vessel_type | STRING | yes | `vessel.type` | UPPER |
| start_ts | TIMESTAMP | no | `start` | UTC |
| end_ts | TIMESTAMP | no | `end` | UTC; must be >= start_ts |
| start_date | DATE | yes | derived | `to_date(start_ts)` |
| duration_hours | DECIMAL(10,2) | yes | `gap.durationHours` | Numeric conversion |
| distance_km | DECIMAL(10,2) | yes | `gap.distanceKm` | Bronze value read as STRING, then cast |
| implied_speed_knots | DECIMAL(10,2) | yes | `gap.impliedSpeedKnots` | Bronze value read as STRING, then cast |
| intentional_disabling | BOOLEAN | yes | `gap.intentionalDisabling` | Boolean conversion |
| off_lat | DECIMAL(10,6) | yes | `gap.offPosition.lat` | Latitude range check |
| off_lon | DECIMAL(10,6) | yes | `gap.offPosition.lon` | Longitude range check |
| on_lat | DECIMAL(10,6) | yes | `gap.onPosition.lat` | Latitude range check |
| on_lon | DECIMAL(10,6) | yes | `gap.onPosition.lon` | Longitude range check |
| positions_per_day_sat_reception | DECIMAL(10,2) | yes | `gap.positionsPerDaySatReception` | Numeric conversion |
| high_seas | BOOLEAN | yes | `regions.highSeas` | Non-empty array means TRUE |
| mpa | BOOLEAN | yes | `regions.mpa` | Non-empty array means TRUE |
| mpa_no_take | BOOLEAN | yes | `regions.mpaNoTake` | Non-empty array means TRUE |
| _source_file, _bronze_ingest_ts, _silver_load_ts, _run_id | see schema | | audit columns | As in section 1 |
## 9. `stg_vessels` (key `vessel_id`; 16 columns)

Grain: one row per vessel. Source: Bronze vessel records.

| Column | Type | Null | Source path | Rule |
|---|---|---|---|---|
| vessel_id | STRING | no | vessel identifier | Natural merge key; required |
| vessel_name | STRING | yes | vessel_name | Trim and clean text |
| ssvid | STRING | yes | ssvid | Keep as string |
| imo | STRING | yes | imo | Keep as string |
| callsign | STRING | yes | callsign | Trim and clean text |
| vessel_flag | STRING | yes | vessel_flag | UPPER |
| length_m | DECIMAL(10,2) | yes | length_m | Numeric conversion; metres |
| tonnage_gt | DECIMAL(10,2) | yes | tonnage_gt | Numeric conversion; gross tonnage |
| engine_power_kw | DECIMAL(10,2) | yes | engine_power_kw | Numeric conversion; kilowatts |
| max_speed_kn | DECIMAL(10,2) | yes | max_speed_kn | Numeric conversion; knots |
| ship_types | ARRAY<STRING> | yes | ship_types | Array of vessel types |
| gear_types | ARRAY<STRING> | yes | gear_types | Array of gear types |
| _source_file | STRING | yes | audit | Source file path |
| _bronze_ingest_ts | TIMESTAMP | yes | audit | Bronze ingestion timestamp |
| _silver_load_ts | TIMESTAMP | yes | Silver load timestamp |
| _run_id | STRING | yes | audit | Pipeline run identifier |

**Naming decision:** `max_speed_kn` is the canonical column name, matching the README and Member 3 schema. Member 2 should use `max_speed_kn` instead of `max_speed_knots` in their notebook and related SQL.