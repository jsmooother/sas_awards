# Finding the SAS Award Finder API (for per-date / per-flight detail)

The `destinations/v1` API returns aggregated availability per date. The SAS website shows multiple flight times (10:10, 12:20, etc.) when you click a date – that data likely comes from another endpoint.

## How to discover it (Browser DevTools)

1. **Open Chrome or Safari** and go to https://www.sas.se/award-finder

2. **Open DevTools** (Cmd+Option+I on Mac, F12 on Windows)

3. **Open the Network tab** and filter by "Fetch/XHR" or "All" to see API requests

4. **Perform a search:**
   - Select origin (e.g. Stockholm ARN)
   - Select destination (e.g. Berlin BER)
   - Click on a date in the calendar (e.g. 22 Feb 2026)

5. **Watch the Network tab** – when the modal with flight times (10:10, 12:20, etc.) appears, look for new requests. The URL of that request is the per-date/per-flight API.

6. **Inspect the request:**
   - Right-click → Copy → Copy as cURL
   - Or click the request and check Headers → Request URL, Query String Parameters

## What we've already tried (no success)

- `/bff/award-finder/availability/v1` – 404
- `/bff/award-finder/calendar/v1` – 404
- `/bff/award-finder/flights/v1` – 404
- Key-based or date-based detail endpoints – 404
- `www.flysas.com` has the same `destinations/v1` as `sas.se` – same structure

## Known API

| Endpoint | Purpose |
|----------|---------|
| `https://www.sas.se/bff/award-finder/destinations/v1` | List destinations + availability (one row per date, aggregated) |

Parameters: `market`, `origin`, `destinations`, `availability`, `direct`, `passengers`, `selectedFlightClass`, `selectedMonth`
