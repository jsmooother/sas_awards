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

### Key caveats

- The database lives at `~/sas_awards/sas_awards.sqlite` (not in the project directory). The directory must be created with `mkdir -p ~/sas_awards` before running anything.
- The data fetcher (`update_sas_awards.py`) must be run at least once before the Flask dashboard will show meaningful data.
- There are no automated tests or lint configuration in this project. Syntax checking can be done with `python -m py_compile <file>`.
- `daily_business_by_date.py` and `daily_plus_europe.py` are actually bash scripts despite their `.py` extension.
- The Telegram bot and morning report script are optional and require `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` secrets in `.env`.
- Standard dev commands are documented in `README.md` under "Quick start".
- **Partner Awards** (Air France/KLM) is a separate module: `partner_awards/airfrance/`. Uses `~/sas_awards/partner_awards.sqlite`. See `partner_awards/airfrance/README.md`.
