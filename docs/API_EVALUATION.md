# SAS Award Finder – API & Architecture Evaluation

**Date:** February 2025  
**New source:** https://www.sas.se/award-finder

---

## 1. API Discovery

The award finder at https://www.sas.se/award-finder uses the **same backend API** as the previous beta:

| Endpoint | URL | Purpose |
|----------|-----|---------|
| Destinations (list) | `https://www.sas.se/bff/award-finder/destinations/v1` | All destinations, no availability |
| Destinations (availability) | Same + `destinations=XXX&availability=true` | Per-destination availability |

### Filters & Parameters

| Parameter | Values | Notes |
|-----------|--------|-------|
| `market` | `se-sv` | Swedish market |
| `origin` | `ARN`, `CPH` | Stockholm Arlanda, Copenhagen |
| `destinations` | IATA code or empty | Empty = list all, code = get availability |
| `availability` | `true` / `false` | Must be `true` to get seat data |
| `passengers` | 1–9 | |
| `direct` | `true` / `false` | Direct flights only |
| `selectedMonth` | e.g. `2026-02` or empty | Optional filter |
| `selectedFlightClass` | `AG`, `AP`, `AB` or empty | AG=Economy, AP=Plus, AB=Business |

### JSON Response (per destination)

```json
{
  "airportCode": "BCN",
  "cityName": "Barcelona",
  "countryName": "Spanien",
  "flightClasses": ["AG", "AB"],
  "availability": {
    "outbound": [
      { "date": "2026-02-24", "availableSeatsTotal": 7, "AG": 7 },
      { "date": "2026-03-02", "availableSeatsTotal": 14, "AG": 10, "AB": 4 }
    ],
    "inbound": [ /* same structure */ ]
  }
}
```

**Conclusion:** The API structure is identical to the current code. Only the base URL needs to change from `beta.sas.se` to `www.sas.se`.

---

## 2. Required Change

| Current | New |
|---------|-----|
| `https://beta.sas.se/bff/award-finder/destinations/v1` | `https://www.sas.se/bff/award-finder/destinations/v1` |

No other changes to the API client are needed.

---

## 3. Data Flow & “New Flights” Report

**Current behaviour:**
- `update_sas_awards.py` fetches all destinations and replaces the `flights` table each run
- Prints a diff of added/removed flights for **that run** (before vs after in same execution)
- Does **not** persist history between runs

**Your goal:** Morning report of what new flights have been **added since yesterday**.

**What’s needed:**
- Persist yesterday’s snapshot before overwriting
- Compare today’s fetch with yesterday’s snapshot
- Report only newly available (date, direction, city, seats)

**Implementation options:**

| Option | How | Pros | Cons |
|--------|-----|------|------|
| A. `flight_history` table | Insert current `flights` into `flight_history` with `fetch_date` before each overwrite | Full history, can report “new since any date” | Slightly more storage |
| B. Separate “yesterday” snapshot table | Copy `flights` to `flights_yesterday` at end of run, compare next morning | Simple, minimal schema change | Only “yesterday” available |
| C. Diff files | Save JSON/CSV snapshots daily, diff externally | No schema change | Clunky, parsing logic |

**Recommendation:** **Option A** – add `flight_history` with `fetch_date`. One row per (fetch_date, airport_code, direction, date, ag, ap, ab). Enables “new since yesterday” and future “new this week” reports.

---

## 4. PostgreSQL vs SQLite

| Factor | SQLite | PostgreSQL |
|--------|--------|------------|
| **Data size** | ~100 destinations × ~200 dates × 2 directions × runs ≈ 40K rows/run; history adds ~40K/day | Same |
| **Single user** | Ideal | Overkill |
| **Local Mac mini** | No server, low RAM | Needs server, more RAM |
| **Setup** | None | Install, configure, run service |
| **Backup** | Copy `.sqlite` file | `pg_dump` or similar |
| **Concurrent writes** | One writer at a time (cron + bot read) | Multiple writers (not needed) |

**Conclusion:** **Keep SQLite.** It matches your setup (local, single machine, modest data volume). PostgreSQL would add complexity and resource use without clear benefit.

---

## 5. Summary

| Item | Recommendation |
|------|----------------|
| **API** | Switch base URL to `www.sas.se` |
| **Approach** | Keep: API fetch → SQLite storage → cron runs. Add: `flight_history` for “new since yesterday” reports |
| **Database** | Stay with SQLite |

---

## 6. Next Steps

1. Update `BASE_URL` in `update_sas_awards.py` to `https://www.sas.se/bff/award-finder/destinations/v1`
2. Add `flight_history` table and logic to snapshot before overwrite
3. Add a “new flights since yesterday” report script
4. Update morning cron to run the report after the fetch
