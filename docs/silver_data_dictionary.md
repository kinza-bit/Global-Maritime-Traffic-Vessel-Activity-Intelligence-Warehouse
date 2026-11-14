# Silver Data Dictionary (`project_staging`)

Owner: Member 3. Covers `stg_port_visits`, `stg_vessel_events`, `stg_rejects`.
`stg_ais_gaps` (Member 1) and `stg_vessels` (Member 2) are documented by their owners.
Schemas live in `schemas/silver_schemas.py`; this file is the human-readable contract (C3).

Status: **DRAFT v1**, based on Bronze 2025-01 to 2025-03. Items marked *(proposal)* need team confirmation.

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

Measured on Bronze 2025-01..03, none of these fire, so `stg_rejects` is expected to be empty. Unit tests prove the path with synthetic bad rows.

## 3. `stg_port_visits` (key `event_id`; 29 columns)

Grain: one port visit by one vessel. Source: `port_visit` events. Expected rows: 296,199 unique ids.

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

Grain: one activity segment. Source: `fishing`, `loitering`, `encounter` events (`event_type` column). Expected rows: 133,694 unique ids (44,995 + 43,752 + 44,947).

| Column | Type | Null | Source path | Rule | Example |
|---|---|---|---|---|---|
| event_id | STRING | no | `id` | key; encounter ids end in `.1` or `.2` and are kept whole | `cefb3506d1c7a41fd97c64d0ad8f7df5.1` |
| event_base_id | STRING | yes | derived | `id` without the `.1`/`.2` suffix; each encounter is stored once per vessel | `cefb3506d1c7a41fd97c64d0ad8f7df5` |
| event_type | STRING | no | `type` | UPPER; `FISHING`, `LOITERING`, `ENCOUNTER` | `FISHING` |
| vessel_id | STRING | no | `vessel.id` | reject if missing | `0ca293acd-d1d8-1658-0421-5fa99bd60bce` |
| vessel_name | STRING | yes | `vessel.name` | name rule; missing becomes `Unknown` | `TAIKO MARU NO.8` |
| ssvid | STRING | yes | `vessel.ssvid` | trim | `432971000` |
| vessel_flag | STRING | yes | `vessel.flag` | UPPER; up to 57% NULL for loitering | `JPN` |
| vessel_type | STRING | yes | `vessel.type` | UPPER | `FISHING` |
| start_ts | TIMESTAMP | no | `start` | UTC | `2025-01-28 07:55:18` |
| end_ts | TIMESTAMP | no | `end` | UTC; >= start_ts | `2025-03-03 07:11:00` |
| start_date | DATE | yes | derived | `to_date(start_ts)` | `2025-01-28` |
| lat | DECIMAL(10,6) | yes | `position.lat` | range check | `8.444800` |
| lon | DECIMAL(10,6) | yes | `position.lon` | range check | `-31.589300` |
| duration_hours | DECIMAL(10,2) | yes | loitering: `loitering.totalTimeHours`; fishing and encounter: derived | `(end_ts - start_ts)` in hours *(proposal)*, because `fishing.averageDurationHours` is NULL in all rows and encounters have no duration | `116525.37` |
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
- The API window matches events that overlap it, not events that start in it: about a third of port visits start before 2025.
- The three worldwide event types are capped at 15,000 per month (45,000 rows each), so they are a sample.
- Missing vessel name: about 24% of port-visit rows. Missing vessel flag: 7% of port visits, 57% of loitering rows.
- Encounters appear twice (`.1`, `.2`), once per vessel.
- Open from README Appendix Q10: `Callsign` and `Max_Speed_KN` exist only in the proposal DDL; they are not in these two tables.