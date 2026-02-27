# Partner Awards – Air France / KLM

Ingests and displays Air France / KLM (Flying Blue) award offers. Fully separate from SAS tables and logic.

## Setup

1. **Create data directory** (if not exists):
   ```bash
   mkdir -p ~/sas_awards
   ```

2. **Run schema** (tables are created automatically on first ingest; or run manually):
   ```bash
   sqlite3 ~/sas_awards/partner_awards.sqlite < partner_awards/airfrance/schema.sql
   ```

3. **Start the Flask app**:
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

## Load offers

```bash
curl "http://127.0.0.1:5000/partner-awards/airfrance/offers?origin=PAR&destination=JNB&depart_date=2026-02-27&limit=50"
```

Returns offers with `segments` and `cabins` (Economy, Premium, Business).

## UI

Open **Partner Awards** in the sidebar → `/partner-awards`. Use "Run test ingest (fixture)" then "Load offers" to see data.

## Live fetch (TODO)

Placeholder in `service.py` for future live GraphQL fetch. See `docs/airfrance-awards-api.md` for:

- **3 GraphQL calls**: CreateSearchContext → LowestFareOffers → AvailableOffers
- **searchStateUuid** required for all reward queries
- **Headers**: `afkl-travel-country`, `afkl-travel-language`, `afkl-travel-market`, `afkl-travel-host`
- **SearchResultAvailableOffersQuery** sha256Hash: `6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a`
