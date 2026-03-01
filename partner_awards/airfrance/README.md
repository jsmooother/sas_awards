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

3. **Run schema** (tables are created automatically on first ingest; or run manually):
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

## API reference

See `docs/airfrance-awards-api.md` for:

- **3 GraphQL calls**: CreateSearchContext → LowestFareOffers → AvailableOffers
- **searchStateUuid** required for all reward queries
- **Headers**: `afkl-travel-country`, `afkl-travel-language`, `afkl-travel-market`, `afkl-travel-host`
- **SearchResultAvailableOffersQuery** sha256Hash: `6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a`
