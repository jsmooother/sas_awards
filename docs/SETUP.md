# Detailed Setup Guide

## Prerequisites

- **Python 3.10+** – Check with `python3 --version`
- **SQLite3** – Usually included; check with `sqlite3 --version`
- **Bash** – For shell scripts (macOS/Linux)

## Step-by-step setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/sas_awards.git
cd sas_awards
```

Replace `YOUR_USERNAME` with your GitHub username or org.

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
# On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Data directory

Scripts expect:

- **Project/code:** Can live anywhere (e.g. `~/sas_awards` or `/path/to/sas_awards`)
- **Database:** `~/sas_awards/sas_awards.sqlite`
- **Reports:** `~/OneDrive/SASReports/`

Create the directories:

```bash
mkdir -p ~/sas_awards
mkdir -p ~/OneDrive/SASReports
```

If the project is not in `~/sas_awards`, either:

- Symlink: `ln -s /path/to/sas_awards ~/sas_awards`
- Or edit `DB_PATH` in the Python scripts to point to your DB

### 5. First data fetch

```bash
cd ~/sas_awards   # or your project dir
source venv/bin/activate
python update_sas_awards.py
```

You should see output like:

```
Run at 2025-02-15T12:00:00.000000
🆕 Added flights:
• 2025-03-15 outbound  BCN | tot=2 AG=0 AP=2 AB=0
...
```

### 6. Telegram bot (optional)

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow prompts
3. Copy the token (e.g. `7552061103:AAH...`)

Create `.env` in the project root:

```
TELEGRAM_BOT_TOKEN=your_token_here
```

Update `weekend_bot.py` to load from env:

```python
import os
from dotenv import load_dotenv
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
```

Or set before running:

```bash
export TELEGRAM_BOT_TOKEN="your_token"
python weekend_bot.py
```

### 7. Run reports

After `flights` is populated:

```bash
./split_weekend_trips.sh          # Weekend CSVs → reports/weekend_trips/
./daily_business_by_date.sh       # Business by date → OneDrive/SASReports
./daily_plus_europe.sh            # Plus Europe → OneDrive/SASReports
```

## Flight history (advanced)

Some reports (`daily_new_business_report.py`, `daily_new_business_by_date.sh`) use a `flight_history` table that stores daily snapshots. The main `update_sas_awards.py` only populates `flights` (current snapshot).

To support history-based reports, you would need to:

1. Add a `flight_history` table with columns like: `fetch_date`, `airport_code`, `city_name`, `direction`, `flight_date`, `ab`, etc.
2. Modify `update_sas_awards.py` (or add a wrapper) to INSERT into `flight_history` after each run instead of (or in addition to) replacing `flights`.

## Path summary

| Path | Purpose |
|------|---------|
| `~/sas_awards/` | Project + DB directory |
| `~/sas_awards/sas_awards.sqlite` | SQLite database |
| `~/OneDrive/SASReports/` | Daily report CSVs |
| `reports/weekend_trips/` | Per-city weekend CSVs |

## Troubleshooting

| Issue | Possible fix |
|-------|--------------|
| `ModuleNotFoundError: No module named 'telegram'` | `pip install python-telegram-bot` |
| `sqlite3.OperationalError: no such table: flights` | Run `python update_sas_awards.py` first |
| `No weekend-pairings found for X` | City not in DB; check spelling (case-insensitive) |
| Shell scripts fail with `cd ~/sas_awards` | Ensure project/database is at `~/sas_awards` or edit script paths |
