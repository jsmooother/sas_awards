# Partner Awards – Air France / KLM

Ingests and displays Air France / KLM (Flying Blue) award offers. Fully separate from SAS tables and logic.

## Setup

1. **Create data directory** (if not exists):
   ```bash
   mkdir -p ~/sas_awards
   ```

2. **Install dependencies** (including Playwright for live fetch):
   ```bash
   pip install -r requirements.txt
   python -m playwright install chromium
   ```

3. **Database:** Tables are created and migrated automatically. No manual step is required. The first time the app or worker uses the partner DB (e.g. opening Flying Blue Routes or running a job), `init_db(conn)` runs: it applies `schema.sql` (CREATE TABLE IF NOT EXISTS) and then runs in-code migrations (ALTER TABLE / table recreates) for existing DBs. So after a merge or deploy, existing installs get new tables and new columns without running any script. To create the file empty and let the app create tables, ensure `~/sas_awards` exists; the DB path is `~/sas_awards/partner_awards.sqlite` (or `$SAS_DB_PATH/partner_awards.sqlite`). Optional manual run:
   ```bash
   sqlite3 ~/sas_awards/partner_awards.sqlite < partner_awards/airfrance/schema.sql
   ```

4. **Start the Flask app**:
   ```bash
   source venv/bin/activate
   flask run --host=0.0.0.0 --port=5000
   # or: python app.py
   ```

## Test ingest (fixture)

Reads `fixtures/airfrance/SearchResultAvailableOffersQuery.json` and inserts offers into the DB.

```bash
curl -X POST http://127.0.0.1:5000/partner-awards/airfrance/test-ingest \
  -H "Content-Type: application/json" \
  -d '{"origin":"PAR","destination":"JNB","depart_date":"2026-02-27","cabin_requested":"ECONOMY"}'
```

Expected: `{"ok":true,"inserted_offer_count":N,"offers":[...]}` with N > 0.

Dev script (verify fixture parse + ingest):

```bash
python -m partner_awards.airfrance.dev_ingest_fixture
```

## Load offers

```bash
curl "http://127.0.0.1:5000/partner-awards/airfrance/offers?origin=PAR&destination=JNB&depart_date=2026-02-27&limit=50"
```

Returns offers with `segments` and `cabins` (Economy, Premium, Business).

## UI

Open **Partner Awards** in the sidebar → `/partner-awards`.

- **Fixture** – Run test ingest from fixture JSON, then Load offers
- **Live (Playwright)** – Live fetch via Chromium (recommended)
- **Live (httpx legacy)** – Direct HTTP; may hang or fail (unstable)
- **Manual Import** – Upload HAR or JSON response, then Load offers

## Live fetch (Playwright)

Uses Playwright (Chromium) to fetch GraphQL responses. Requires Playwright + Chromium installed (see Setup).

```bash
curl -X POST http://127.0.0.1:5000/partner-awards/airfrance/live-test-playwright \
  -H "Content-Type: application/json" \
  -d '{"origin":"PAR","destination":"JNB","depart_date":"2026-02-27","cabin":"ECONOMY"}'
```

Returns `homepage_status`, `create_context_status`, `offers_status`, timings, `inserted_offer_count`, `scan_run_id`, etc.

Health check:

```bash
curl http://127.0.0.1:5000/partner-awards/airfrance/playwright-health
```

## Manual import (HAR / JSON)

Upload a JSON response body or a HAR file exported from Chrome DevTools.

### Export HAR

1. Open Chrome DevTools (F12) → **Network** tab
2. Load the Air France award search page and trigger a search
3. Right‑click in the Network list → **Save all as HAR with content**
4. Save the `.har` file

### Import via UI

On the Partner Awards page, use **Manual Import (HAR/JSON)**:

- Choose file (`.json` or `.har`)
- Enter Origin, Destination, Depart Date
- Click **Import & Ingest**

Offers load automatically after import.

### Import via curl

```bash
curl -X POST http://127.0.0.1:5000/partner-awards/airfrance/import \
  -F "file=@/path/to/SearchResultAvailableOffersQuery.json" \
  -F "origin=PAR" \
  -F "destination=JNB" \
  -F "depart_date=2026-02-27" \
  -F "cabin_requested=ECONOMY"
```

Or with a HAR file:

```bash
curl -X POST http://127.0.0.1:5000/partner-awards/airfrance/import \
  -F "file=@/path/to/network.har" \
  -F "origin=PAR" \
  -F "destination=JNB" \
  -F "depart_date=2026-02-27"
```

Returns `ok`, `operation_name`, `inserted_offer_count`, `scan_run_id`, `warnings`.

## Remote Fetch Runner (VPS)

When Playwright is blocked from local networks, deploy `partner_awards_remote_runner/` on a VPS. Copy outputs back and ingest:

```bash
# Open dates month (AMS→JNB, March 2026, Business)
cd partner_awards_remote_runner && python runner.py open-dates-month --origin AMS --destination JNB --month 2026-03 --cabins BUSINESS

# Import (from sas_awards root)
python -m partner_awards.airfrance.import_folder --path partner_awards_remote_runner/outputs/AF

# Calendar UI
# http://127.0.0.1:5000/partner-awards/calendar?origin=AMS&destination=JNB&month=2026-03&cabin=BUSINESS

# Verify against KLM screenshot
python -m partner_awards.airfrance.verify_month --origin AMS --destination JNB --month 2026-03 --cabin BUSINESS
# Or: http://127.0.0.1:5000/partner-awards/calendar/verify?origin=AMS&destination=JNB&month=2026-03&cabin=BUSINESS
```

See `partner_awards_remote_runner/README.md` for VPS setup and cron.

## Calendar via KLM.se without login

On **KLM.se**, `SharedSearchLowestFareOffersForSearchQuery` can be tried **without login** (no CreateSearchContext, no Playwright). This path is **disabled by default** (it often does not work in practice). To enable for testing:

```bash
export PARTNER_AWARDS_KLM_NO_LOGIN=1
curl -X POST http://127.0.0.1:5000/partner-awards/airfrance/calendar-scan-klm-no-login \
  -H "Content-Type: application/json" \
  -d '{"origin":"AMS","destination":"BKK","date_interval":"2026-03-01/2027-02-28","cabins":["BUSINESS"]}'
```

When disabled, the endpoint returns `ok: false` with `"KLM no-login is disabled"`. When enabled, returns `ok`, `inserted_calendar_fares`, `scan_run_id`, `host`: `"klm.se"`, `timing`. Same persisted query hash as Air France: `3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243`.

**Daily scanner:** If no-login works for you, it is enough for calendar-only runs. If a daily job later needs full offer drilldown (e.g. SearchResultAvailableOffersQuery), next steps can be done via manual cookie collection (export cookies from a logged-in session and reuse in the runner) rather than automating login.

## Data we collect and store

The batch script (worker + remote runner) is slow because it does **one API call per (route × month × cabin)** with cooldowns. The API we call is **LowestFareOffers**, which returns the **lowest miles per day** for a given month – it is already a calendar summary, not per-flight detail.

**What we store:**

| Where | What |
|-------|------|
| **partner_award_calendar_fares** | One row per (origin, destination, cabin_class, depart_date, host): **miles** and **tax**. So we have both **Business** and **Premium Economy** for every day we scanned. The 365-day calendar view shows this data (one cabin at a time; use the Cabin dropdown). |
| **partner_award_raw_responses** | Full JSON of each LowestFareOffers response (when importing from runner output). Useful for debugging or re-parsing. |

So the **overview you see (365-day calendar) is the main product of the data we collect**. We do not get “more detailed” data in this flow: the API returns one lowest price per day per cabin, and we store that. For full itineraries (flights, times, multiple options per date) you’d need the **AvailableOffers** call for a specific date, which the batch does not run.

**Return routes:** Each watchlist route can have **Include returns** turned on. When you run a batch job, the worker then queues both the outbound leg (e.g. AMS→BKK) and the return leg (BKK→AMS). New routes default to include returns on; use the “Include returns” toggle per route to turn it off. Note: the API sometimes returns empty prices for the return direction (e.g. BKK→AMS may have `lowestOffers` with dates but no price/connections). When that happens, the import stores 0 rows for that file; outbound data is unchanged.

**Quick check of what’s in the DB:**

```bash
sqlite3 ~/sas_awards/partner_awards.sqlite "
SELECT 'calendar_fares' AS tbl, COUNT(*) AS n FROM partner_award_calendar_fares
UNION ALL
SELECT 'raw_responses', COUNT(*) FROM partner_award_raw_responses;
SELECT source, origin, destination, cabin_class, COUNT(*), MIN(depart_date), MAX(depart_date)
FROM partner_award_calendar_fares GROUP BY source, origin, destination, cabin_class;
"
```

## API reference

See `docs/airfrance-awards-api.md` for:

- **3 GraphQL calls**: CreateSearchContext → LowestFareOffers → AvailableOffers (Air France / full flow)
- **searchStateUuid** required for reward queries on Air France; **KLM.se** allows LowestFareOffers only with a client-generated UUID (no login)
- **Headers**: `afkl-travel-country`, `afkl-travel-language`, `afkl-travel-market`, `afkl-travel-host`
- **SearchResultAvailableOffersQuery** sha256Hash: `6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a`
