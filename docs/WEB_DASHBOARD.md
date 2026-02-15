# SAS Awards – Web Dashboard

Flask web app to browse SAS EuroBonus award availability from ARN and CPH. Filter by origin, city, date, and min seats. Filters auto-update while typing.

## Run

```bash
cd ~/sas_awards   # or your project dir
source venv/bin/activate
flask run --host=0.0.0.0 --port=5000
```

Or:

```bash
python app.py
```

## Access

- Local: http://127.0.0.1:5000
- LAN: http://macmini.local:5000 or http://&lt;macmini-ip&gt;:5000

## Pages

| Route | Description |
|-------|-------------|
| `/` | Dashboard: flight counts, quick links |
| `/all` | All Europe: Economy, Plus, Business (≥2 seats in any cabin) |
| `/business` | Business class only (≥2 seats) |
| `/plus` | Plus & Business Europe (≥2 seats in Plus or Business) |
| `/weekend` | Weekend pairs: Outbound → Inbound, 3–4 day trip |
| `/new` | New business flights since yesterday |
| `/search?q=Barcelona` | Search by city or airport code |

## Filters

On each table page:

- **Origin** – ARN, CPH, or all
- **City / code** – Partial match; updates automatically as you type
- **From date / To date** – Date range
- **Min seats** – Minimum seats (default 2)

Changing any filter (including typing in city) updates results automatically. No need to click Apply.

## Run on boot (launchd)

Create `~/Library/LaunchAgents/com.sasawards.web.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.sasawards.web</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/jeppe/sas-awards/venv/bin/python</string>
    <string>/Users/jeppe/sas-awards/app.py</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/jeppe/sas-awards</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
</dict>
</plist>
```

Then:

```bash
launchctl load ~/Library/LaunchAgents/com.sasawards.web.plist
```
