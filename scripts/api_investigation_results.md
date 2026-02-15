# SAS API Investigation Results

## Test: ARN → BER (Berlin)

### direct=true vs direct=false

| Parameter | Outbound dates | Inbound dates |
|-----------|----------------|---------------|
| direct=FALSE | 214 | 249 |
| direct=TRUE  | 172 | 231 |

**Conclusion:** `direct=false` returns MORE data. We use `direct=false` – correct for maximum coverage.

### Storage verification

| Source | ARN-BER outbound | ARN-BER inbound |
|--------|------------------|-----------------|
| API (direct=false) | 214 | 249 |
| Our DB | 214 | 249 |

**Conclusion:** We store everything the API returns. No data is dropped.

### Date gaps in API response

The API does **not** return every calendar date. Example for ARN-BER outbound (Feb 2026):
- Has: 2026-02-15, 16, 17, 18, 19, then 24, 25, 26, 27, 28
- Missing: 2026-02-20, 21, 22, 23

The SAS website shows "Avresa 2026-02-22" with multiple flight times. That date is **not** in our API response. The website likely uses a different endpoint when you click a date (e.g. fetches per-date detail on demand).

### API structure

Each availability entry has:
- `date`, `availableSeatsTotal`, `AG`, `AP`, `AB`
- One entry per date – aggregated, not per flight time
- No nested "flights" array with 10:10, 12:20, etc.

The website’s per-departure-time view (10:10, 12:20, 14:20, 22:15) is almost certainly from another API or a different response structure we don’t have access to via the destinations/v1 endpoint.

### Summary

1. We are **not** filtering by direct-only – we use `direct=false`.
2. We **store all data** the API returns – counts match exactly.
3. The destinations/v1 API returns **aggregated availability per date**, not per flight time.
4. The API **omits some dates** (e.g. 2026-02-20–23). The SAS website may fetch those via a different call when a user selects a date.
