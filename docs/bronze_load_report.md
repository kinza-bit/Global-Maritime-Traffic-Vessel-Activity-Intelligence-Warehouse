# Bronze Ingestion & `stg_vessels` Load Report

**Owner:** Member 2 – Syeda Faryal Zehra (`feature/bronze-ingestion`)  
**Storage Account:** `stmaritimeks` (ADLS Gen2, containers: `bronze`, `silver`)  
**Silver Staging Target:** `abfss://silver@stmaritimeks.dfs.core.windows.net/stg_vessels` (`dbw_maritime_ks.project_staging.stg_vessels`)

---

## 1. Storage Layout & State Control
- **Events (`bronze/gfw/events/<event_type>/load_type=<initial|incremental>/ingest_date=YYYY-MM-DD/*.jsonl`):**
  - Partitioned across all 5 event types: `port_visit`, `gap`, `fishing`, `loitering`, and `encounter`.
  - Initial backfill window: `2025-01-01` to `2025-02-28` (`load_type=initial`).
  - Incremental window: `2025-03-01` to `2025-03-31` (`load_type=incremental`, `ingest_date=2026-11-12`).
- **Vessels (`bronze/gfw/vessels/load_type=incremental/ingest_date=YYYY-MM-DD/batch_XXXX/*.json`):**
  - Enriched across all unique event vessel IDs (`MAX_ENRICH_VESSELS=None`, 50 IDs per batch across 3,615 batches).
- **Manifest & Watermark:**
  - `_manifest/ingestion_manifest/*.jsonl`: Chunk-level audit logs (`run_id`, `status`, `rows_received`, `retry_count`, `started_at_utc`, `finished_at_utc`).
  - `_state/watermark.json`: Verified at `last_loaded_month = "2025-03"`, `window_end = "2025-03-31"` (`run_id = incremental_20261112T191935Z`).

---

## 2. Verified Row Count Reconciliation
*(Audited directly from ADLS Gen2 `bronze` files and `silver` Delta transaction logs)*

| Dataset / Entity | Load Type / Operation | ADLS Path | Exact Row Count |
|---|---|---|---|
| `port_visit` events | `initial` / `incremental` | `bronze/gfw/events/port_visit/` | `269,548` / `111,507` (`381,055` total) |
| `gap` events | `initial` / `incremental` | `bronze/gfw/events/gap/` | `30,000` / `15,000` (`45,000` total) |
| `fishing` events | `initial` / `incremental` | `bronze/gfw/events/fishing/` | `30,000` / `15,000` (`45,000` total) |
| `loitering` events | `initial` / `incremental` | `bronze/gfw/events/loitering/` | `30,000` / `15,000` (`45,000` total) |
| `encounter` events | `initial` / `incremental` | `bronze/gfw/events/encounter/` | `30,000` / `15,000` (`45,000` total) |
| **Total Bronze Events** | **All 5 event types** | `bronze/gfw/events/` | **`561,055`** (`389,548` initial + `171,507` incremental) |
| **Bronze Vessels** | `load_type=incremental` | `bronze/gfw/vessels/load_type=incremental/` | **`181,058`** |
| **Silver `stg_vessels`** | **Idempotent Delta `MERGE`** | `silver/stg_vessels` | **`180,705`** (`777` duplicates removed) |
| Quarantine (`schema_drift`) | Delta `APPEND` | `silver/_quarantine/schema_drift` | **`0`** |

---

## 3. Data Quality & Audit Controls
- **All-String Bronze Schema-on-Write:** `12_bronze_vessels_enrichment.py` validates every batch against `BRONZE_VESSEL_ALL_STRING_SCHEMA` (`primitivesAsString=true`, `_rescued_data`) prior to writing to ADLS.
- **Quarantine & Reject Routing:** Records lacking both `selfReportedInfo` and `registryInfo` route to `silver/_quarantine/schema_drift`; records with null/blank `vessel_id` route to `silver/stg_rejects`.
- **Idempotency & Audit Logging:** `23_stg_vessels.py` deduplicates on `vessel_id` (ordered by `_bronze_ingest_ts DESC, transmission_to_ts DESC`), executes an idempotent Delta `MERGE` (`inserted=0, updated=180705` on re-run), and records run metrics in `silver/_audit/pipeline_run_log`.
- **Unity Catalog Registration Note:** Physical Delta data is written and verified at `abfss://silver@stmaritimeks.dfs.core.windows.net/stg_vessels`. Catalog registration (`CREATE TABLE IF NOT EXISTS project_staging.stg_vessels`) is wrapped non-blockingly awaiting Member 1 (`dbw_maritime_ks` owner) to execute `GRANT USE SCHEMA, CREATE TABLE ON SCHEMA dbw_maritime_ks.project_staging`.