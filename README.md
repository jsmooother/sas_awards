# SAS Awards

A Python toolset for tracking SAS (Scandinavian Airlines) EuroBonus award flight availability from **Stockholm Arlanda (ARN)** and **Copenhagen (CPH)**. Includes a Telegram bot for querying weekend trip pairings and scripts for generating daily reports.

## Features

- **Data fetcher** – Fetches award availability from SAS API and stores in SQLite
- **Telegram bot** – Query weekend flight pairings with `/CityName` (e.g. `/Barcelona`, `/Oslo`)
- **Reports** – Daily CSV reports for business-class, Plus, and weekend trips
- **Web dashboard** – Browse All Europe, Business, Plus & Business, Weekend pairs; live filters

## Requirements

- Python 3.10+
- SQLite3
- Network access to SAS API (`https://www.sas.se`)

## Quick start

### 1. Clone and enter the project

```bash
git clone https://github.com/jsmooother/sas_awards.git
cd sas_awards
```

### 2. Create virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Set up the data directory

The database is stored in `~/sas_awards/`. Create it and run from there:

```bash
mkdir -p ~/sas_awards
cp -r /path/to/sas_awards/* ~/sas_awards/   # if project lives elsewhere
cd ~/sas_awards
```

### 4. Fetch initial data

```bash
source venv/bin/activate
python update_sas_awards.py
```

This creates `~/sas_awards/sas_awards.sqlite` and populates the `flights` table.

### 5. Run the Telegram bot (optional)

Create a bot via [@BotFather](https://t.me/BotFather) and set the token:

```bash
export TELEGRAM_BOT_TOKEN="your_bot_token_here"
python weekend_bot.py
```

Or create a `.env` file (see [Configuration](#configuration)).

## Project layout

| File | Description |
|------|-------------|
| `update_sas_awards.py` | Fetches availability from SAS API → SQLite `flights` table |
| `weekend_bot.py` | Telegram bot for `/CityName` weekend pair queries |
| `split_weekend_trips.sh` | Exports weekend trip CSVs to `reports/weekend_trips/` |
| `daily_new_business_report.py` | New business-class flights (uses `flight_history`) |
| `daily_business_by_date.sh` | Business seats by date (uses `flights`) |
| `scripts/morning_report.py` | Morning summary → Telegram |
| `app.py` | Web dashboard (Flask) |
| `daily_plus_europe.sh` | Plus Europe availability by city |
| `daily_new_plus_europe2.sh` | New Plus Europe flights (today vs yesterday) |
| `daily_new_business_by_date.sh` | New business by date (uses `flight_history`) |

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `TELEGRAM_BOT_TOKEN` | Bot token from BotFather | (required for bot) |
| `DB_PATH` | SQLite database path | `~/sas_awards/sas_awards.sqlite` |

Create a `.env` file in the project root and add:

```
TELEGRAM_BOT_TOKEN=your_token_here
```

**Important:** Never commit `.env` or real tokens to git.

## Database schema

The main `flights` table:

| Column | Type | Description |
|--------|------|-------------|
| origin | TEXT | Departure airport (ARN, CPH) |
| airport_code | TEXT | IATA code (e.g. CPH, BCN) |
| city_name | TEXT | City name |
| country_name | TEXT | Country |
| direction | TEXT | `outbound` or `inbound` |
| date | TEXT | Flight date (YYYY-MM-DD) |
| total | INTEGER | Total available seats |
| ag | INTEGER | Economy (Go) seats |
| ap | INTEGER | Economy (Plus) seats |
| ab | INTEGER | Business seats |

Some reports use a `flight_history` table for historical snapshots; see [docs/SETUP.md](docs/SETUP.md) for details.

## Report filters

Reports only show flights that are realistically bookable:

| Filter | Value | Purpose |
|--------|-------|---------|
| **Min seats** | 2 | Reward flights are only useful when 2+ seats are available (couples/friends) |
| **Weekend trip length** | 3–4 days | Outbound to inbound: 3–4 days (typical long weekend) |

Configure in `report_config.py` if you need different thresholds.

## Output directories

- **Reports:** `~/OneDrive/SASReports/` (business, Plus Europe CSVs)
- **Weekend trips:** `reports/weekend_trips/` (per-city CSVs)

Adjust paths in the scripts if your setup differs. Set `SAS_DB_PATH` to point at your DB when running from a different location.

## Scheduling

The project uses **system cron** (no APScheduler). Run the data updater periodically so the bot and reports have fresh data.

### Add a cron job (macOS)

```bash
crontab -e
```

Add one of these lines (adjust paths to your setup):

```bash
# Daily at 06:00
0 6 * * * cd /Users/jeppe/sas-awards && /Users/jeppe/sas-awards/venv/bin/python update_sas_awards.py >> /Users/jeppe/sas-awards/run.log 2>&1

# Every 6 hours
0 */6 * * * cd /Users/jeppe/sas-awards && /Users/jeppe/sas-awards/venv/bin/python update_sas_awards.py >> /Users/jeppe/sas-awards/run.log 2>&1
```

If the project lives in `~/sas_awards`:

```bash
0 6 * * * cd ~/sas_awards && ~/sas_awards/venv/bin/python update_sas_awards.py >> ~/sas_awards/run.log 2>&1
```

### Morning report to Telegram

After the updater and report scripts, send a summary to Telegram:

```bash
# 06:20 – requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
20 6 * * * cd ~/sas_awards && TELEGRAM_CHAT_ID=your_chat_id ~/sas_awards/venv/bin/python scripts/morning_report.py >> ~/sas_awards/run.log 2>&1
```

Get your chat ID: message the bot, then visit `https://api.telegram.org/bot<TOKEN>/getUpdates` and find `"chat":{"id":123456789}`.

The bot (`weekend_bot.py`) runs separately and stays in the foreground – use `launchd` or `tmux`/`screen` to keep it running.

## License

MIT (or as you prefer)
