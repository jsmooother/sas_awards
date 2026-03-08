# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

SAS Awards is a Python toolset for tracking SAS EuroBonus award flight availability. See `README.md` for full documentation.

### Services

| Service | Command | Port | Notes |
|---------|---------|------|-------|
| Flask web dashboard | `source venv/bin/activate && python app.py` | 5000 | Main UI; requires SQLite DB to be populated first |
| Data fetcher | `source venv/bin/activate && python update_sas_awards.py` | N/A | Populates `~/sas_awards/sas_awards.sqlite` from SAS API; takes ~3 minutes |
| Telegram bot | `source venv/bin/activate && python weekend_bot.py` | N/A | Requires `TELEGRAM_BOT_TOKEN` in `.env`; optional |

### Navigation (2-page architecture)

The app has exactly 2 pages + API endpoints:

| URL | Purpose |
|---|---|
| `GET /` | Dashboard — unified search with region/cabin/city filters, paginated results, detail modal |
| `GET /reports` | Reports — 5 tabs (Region, City, Business, Weekend, New Today) with charts + tables |
| `GET /api/detail` | Route detail JSON (origin, dest, date) |
| `GET /api/routes` | SAS routes/v1 proxy (origin, dest, date) |

Old URLs (`/all`, `/business`, `/plus`, `/weekend`, `/new`, `/search`, `/flow`, `/reports/*`) are 301 redirects.

### Code structure

- `app.py` — Flask routes only (~200 lines), no SQL
- `queries.py` — All database queries as testable functions
- `regions.py` — Country→region mapping (Swedish names from SAS API)
- `report_config.py` — Shared constants (MIN_SEATS, TRIP_DAYS)
- `tests/test_app.py` — 24 pytest tests (pages, APIs, redirects)

### Key caveats

- The database lives at `~/sas_awards/sas_awards.sqlite`. Create with `mkdir -p ~/sas_awards`.
- Run `update_sas_awards.py` at least once to populate data.
- Tests: `python -m pytest tests/test_app.py -v`
- `daily_business_by_date.py` and `daily_plus_europe.py` are bash scripts despite `.py` extension.
- Telegram bot and morning report are optional (need `TELEGRAM_BOT_TOKEN` in `.env`).
