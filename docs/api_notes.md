# Global Fishing Watch (GFW) API v3 – Integration & Schema Notes

**Owner:** Member 2 – Syeda Faryal Zehra (`feature/bronze-ingestion`)  
**Base URL:** `https://gateway.api.globalfishingwatch.org/v3`  
**Authentication:** `Authorization: Bearer <token>` retrieved at runtime via Databricks secret scope `maritime-kv` (`gfw-api-token`).

---

## 1. Endpoints & Pagination Behavior
- **Events (`POST /v3/events`):**
  - Request body accepts `datasets`, GeoJSON `geometry`, `startDate`, `endDate`, and `types`; query parameters control `limit` and `offset`.
  - Default page size is `limit=1000`, automatically falling back to `limit=100` if the API responds with HTTP `400` or `422`.
  - Pagination iterates via `nextOffset` until `entries` is empty or `nextOffset` is `null`.
- **Vessels (`GET /v3/vessels`):**
  - Queries `datasets[0]=public-global-vessel-identity:latest` with `ids[0..49]` (batches of up to 50 vessel IDs per request).

## 2. Rate Limits, Retries & Failure Semantics
- Transient HTTP status codes (`429`, `500`, `502`, `503`, `504`) and network timeouts retry with exponential backoff, honoring the `Retry-After` response header when returned.
- Non-retryable `4xx` responses halt the active chunk immediately, write a failure entry (with truncated response body) to `_manifest/ingestion_manifest/`, and leave `_state/watermark.json` unchanged so the failed window can be safely retried.

## 3. Verified JSON Payload & Schema Quirks
- **No root-level `id` on `/vessels` objects:** Verified top-level keys are strictly `registryInfoTotalRecords`, `registryInfo`, `registryOwners`, `registryPublicAuthorizations`, `combinedSourcesInfo`, `selfReportedInfo`, and `dataset`. Vessel IDs reside inside `selfReportedInfo[].id` and `registryInfo[].id`.
- **Nested metric arrays in `combinedSourcesInfo`:** Fields `bestLengthM`, `bestTonnageGt`, `bestEnginePowerKw`, and `maxSpeedKn` are returned as JSON arrays of historical objects (e.g., `[{"value": 36.18473727611401, "source": "SECONDARY_CLASSIFIER_MODEL", "yearFrom": 2024, "yearTo": 2024}]`) rather than flat scalars. Silver (`23_stg_vessels.py`) parses these arrays, sorts by `yearTo DESC`, extracts `value`, and casts to `DECIMAL(10, 2)` via SQL `try_cast`.
- **Mixed primitives & string encoding:** Numeric and boolean attributes (`gap.distanceKm`, `port_visit.confidence`, `registryInfoTotalRecords`, `latestVesselInfo`, `lengthM`) arrive as mixed JSON primitives or strings. Bronze enforces an explicit all-string `StructType` with `primitivesAsString=true` and `_rescued_data` (`mode="PERMISSIVE"`) to prevent ingestion failures.
- **Registry-only vessels & empty arrays:** Some vessel records contain an empty `selfReportedInfo` array (`[]`) and populate only `registryInfo`. Silver uses ANSI-safe `F.get(..., 0)` indexing and falls back to `registryInfo[0]` attributes when `selfReportedInfo` is empty.
- **Overlapping port bounding boxes:** Adjacent port polygons (such as Rotterdam and Antwerp) can return duplicate `PORT_VISIT` events across spatial queries; Silver deduplicates deterministically by primary key.
- **Date-window incremental filtering:** `/v3/events` filters by event occurrence (`startDate` / `endDate`) without a `modified_since` cursor, so incremental loads ingest an overlap window and rely on Silver `MERGE` deduplication.