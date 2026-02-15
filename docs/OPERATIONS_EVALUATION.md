# SAS Awards – Operations & Testing Evaluation

**Running locally on Mac mini – testing, scheduling, reporting, and frontend options**

---

## 1. How to Test

### Quick test (no Telegram)

```bash
cd /Users/jeppe/sas-awards
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create data dir and DB location
mkdir -p ~/sas_awards
# Symlink or copy so DB ends up at ~/sas_awards (or set DB_PATH)
# If project is at /Users/jeppe/sas-awards, either:
ln -sf /Users/jeppe/sas-awards ~/sas_awards   # project as data dir
# Or: DB_PATH=./sas_awards.sqlite python update_sas_awards.py

# Test 1: Fetch data (will take a few minutes – ~100 destinations × 2 origins)
python update_sas_awards.py

# Test 2: Check DB
sqlite3 ~/sas_awards/sas_awards.sqlite "SELECT origin, COUNT(*) FROM flights GROUP BY origin;"

# Test 3: Reports (need ~/OneDrive/SASReports for some)
mkdir -p ~/OneDrive/SASReports
./daily_business_by_date.sh
./split_weekend_trips.sh
```

### Test with Telegram bot

```bash
export TELEGRAM_BOT_TOKEN="your_token_from_botfather"
python weekend_bot.py
# In Telegram: send /Barcelona or /Oslo
```

### Minimal “dry” test (API only)

```bash
python3 -c "
import requests
r = requests.get('https://www.sas.se/bff/award-finder/destinations/v1',
  params={'market':'se-sv','origin':'CPH','destinations':'BCN','availability':'true'})
print('CPH->BCN:', len(r.json()[0]['availability']['outbound']), 'outbound dates')
"
```

---

## 2. Keeping It Running on Mac mini

| Option | Pros | Cons | Best for |
|--------|------|------|----------|
| **cron** | Simple, standard, reliable | Limited to scheduled jobs | Data updater, report scripts |
| **launchd** | Mac-native, runs on login/reboot, survives restarts | More config, XML plist | Long-running bot |
| **Mac app (.app)** | Looks like “an app” | Build/maintain overhead, overkill | Not recommended |
| **Docker** | Portable, isolated | Heavy on old Mac mini, extra complexity | Not recommended |

### Recommendation

- **cron** for: `update_sas_awards.py`, `daily_business_by_date.sh`, `daily_plus_europe.sh`, `scripts/morning_report.py`, etc.
- **launchd** for: `weekend_bot.py` (keep running 24/7)
- **launchd** (optional) for: `app.py` web dashboard

---

## 3. Morning Report Flow

Target: every morning, fetch fresh data, generate reports, then notify you.

### Sequence

1. **05:55** – Run `update_sas_awards.py` (fetches ARN + CPH, ~5–10 min)
2. **06:10** – Run report scripts → CSVs to `~/OneDrive/SASReports/`
3. **06:15** – Send summary to Telegram (optional)

### Cron example

```bash
# crontab -e
55 5 * * * cd ~/sas_awards && ./venv/bin/python update_sas_awards.py >> ~/sas_awards/run.log 2>&1
15 6 * * * cd ~/sas_awards && ./daily_business_by_date.sh >> ~/sas_awards/run.log 2>&1
15 6 * * * cd ~/sas_awards && ./daily_plus_europe.sh >> ~/sas_awards/run.log 2>&1
20 6 * * * cd ~/sas_awards && TELEGRAM_CHAT_ID=your_id ./venv/bin/python scripts/morning_report.py >> ~/sas_awards/run.log 2>&1
```

`scripts/morning_report.py` formats a summary and sends it via Telegram Bot API.

---

## 4. Report Delivery: Telegram vs Web vs Both

| Channel | Pros | Cons |
|---------|------|------|
| **Telegram** | Push to phone, works anywhere, bot already in place | Limited length, no charts |
| **Web dashboard** | Nice tables, filters, charts, full reports | Needs server, more setup |
| **CSV in OneDrive** | Syncs to cloud, Excel/Sheets | No push notification |
| **All three** | Flexible, redundant | More moving parts |

### Recommendation: Start with Telegram, add web later

1. **Phase 1 – Telegram morning report**  
   - Script generates short text summary (e.g. top new business flights, counts).  
   - Sends via Telegram Bot API to your chat.

2. **Phase 2 – Web dashboard (optional)**  
   - Flask/FastAPI app, reads from SQLite.  
   - Run on Mac mini, access from `http://macmini.local:5000` on your LAN.

---

## 5. Sending Morning Report to Telegram

The bot can post to a **specific chat** using the Bot API. You need your **chat ID**.

### Get your Telegram chat ID

1. Message your bot with `/start`.
2. Visit:  
   `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`
3. In the JSON, find `"chat":{"id":123456789}` – that’s your chat ID.

### Flow

1. Run `update_sas_awards.py`.
2. Query SQLite for “new” or “today’s best” flights.
3. Format a short message (e.g. top 10 business flights, new cities).
4. POST to `https://api.telegram.org/bot<TOKEN>/sendMessage` with `chat_id` and `text`.

Example script concept:

```python
# morning_report.py (concept)
# - Query flights for today’s highlights
# - Format as text
# - requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
#                 json={"chat_id": CHAT_ID, "text": report})
```

---

## 6. Web Dashboard Options

| Approach | Effort | What you get |
|----------|--------|--------------|
| **Simple HTML + table** | Low | Static page, refresh to see new data |
| **Flask + Jinja** | Medium | Dynamic page, simple filters, no real-time |
| **Flask + auto-refresh** | Medium | Same plus periodic reload |
| **React/Vue SPA + API** | High | Modern UI, more work to maintain |

### Simple dashboard

- One Flask route that reads from SQLite.
- Returns HTML with a table of flights (e.g. business, weekend pairs).
- Run with `flask run --host=0.0.0.0` to reach from other devices on the LAN.

---

## 7. Suggested Roadmap

| Phase | Deliverable | Effort |
|-------|-------------|--------|
| **1** | Test script + run full pipeline once | 1–2 h |
| **2** | Cron for updater + reports | ~30 min |
| **3** | launchd for Telegram bot | ~30 min |
| **4** | Morning report script → Telegram | 1–2 h |
| **5** | Web dashboard (optional) | 2–4 h |

---

## 8. Paths & Layout Assumptions

| Path | Purpose |
|------|---------|
| `~/sas_awards/` | Project root (code + venv) |
| `~/sas_awards/sas_awards.sqlite` | DB |
| `~/OneDrive/SASReports/` | Business/Plus reports |
| `~/sas_awards/reports/weekend_trips/` | Weekend CSVs |
| `~/sas_awards/run.log` | Cron logs |

If the project lives elsewhere (e.g. `/Users/jeppe/sas-awards`), use symlinks or set `DB_PATH` so the DB is where the scripts expect it.

---

## 9. Summary

| Question | Recommendation |
|----------|----------------|
| **How to test?** | Run `update_sas_awards.py`, check DB, run report scripts |
| **Run daily how?** | Cron for updater + reports + morning_report; launchd for bot |
| **Mac app?** | No – cron + launchd is enough |
| **Web frontend?** | Flask app (`app.py`) – run with `flask run --host=0.0.0.0` |
| **Morning report?** | `scripts/morning_report.py` → format summary → send via Telegram Bot API |
| **Dashboard?** | Flask app: /all, /business, /plus, /weekend, /new, /search – auto-updating filters. See docs/WEB_DASHBOARD.md |
