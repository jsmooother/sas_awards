#!/usr/bin/env python3
import os, sqlite3, csv

from datetime import date
from report_config import MIN_SEATS

# ─── CONFIG ────────────────────────────────────────────────────────────────
DB_FILE = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))
HOME    = os.path.expanduser("~")
OUT_DIR = os.path.join(HOME, "OneDrive", "SASReports")
OUT_F   = os.path.join(
    OUT_DIR,
    f"daily_new_business_us_{date.today().isoformat()}.csv"
)
# ─────────────────────────────────────────────────────────────────────────────

os.makedirs(OUT_DIR, exist_ok=True)
conn = sqlite3.connect(DB_FILE)
cur  = conn.cursor()

# 1) grab the two most recent fetch_dates from the history table
cur.execute("""
  SELECT DISTINCT fetch_date
    FROM flight_history
   ORDER BY fetch_date DESC
   LIMIT 2
""")
dates = [row[0] for row in cur.fetchall()]
if len(dates) < 2:
    print("⚠️  Not enough history yet (need 2 distinct fetch_date rows).")
    conn.close()
    exit(1)

latest, prev = dates[0], dates[1]

# 2) a templated SQL that returns “new” outbound AB-class flights
NEW_OUT_SQL = """
WITH
  t AS (
    SELECT origin, airport_code, city_name, date, ab AS business_seats
      FROM flight_history
     WHERE fetch_date = :latest
       AND direction   = 'outbound'
       AND ab >= :min_seats
  ),
  p AS (
    SELECT origin, airport_code, date
      FROM flight_history
     WHERE fetch_date = :prev
       AND direction   = 'outbound'
       AND ab >= :min_seats
  )
SELECT t.origin, t.city_name, t.airport_code, t.date, t.business_seats
  FROM t
  LEFT JOIN p
    ON p.origin = t.origin AND p.airport_code = t.airport_code AND p.date = t.date
 WHERE p.airport_code IS NULL
 ORDER BY t.origin, t.city_name COLLATE NOCASE, t.date;
"""

# 3) same for inbound
NEW_IN_SQL = NEW_OUT_SQL.replace("direction   = 'outbound'",
                                  "direction   = 'inbound'")

params = {"latest": latest, "prev": prev, "min_seats": MIN_SEATS}

# 4) run and write CSV
with open(OUT_F, "w", newline="") as f:
    w = csv.writer(f)

    w.writerow(["Outbound Business (NEW since", prev, "->", latest, f", >={MIN_SEATS} seats)"])
    w.writerow(["origin", "city_name", "airport_code", "date", "business_seats"])
    cur.execute(NEW_OUT_SQL, params)
    for row in cur.fetchall():
        w.writerow(row)

    # blank line
    w.writerow([])

    w.writerow(["Inbound Business (NEW since", prev, "->", latest, f", >={MIN_SEATS} seats)"])
    w.writerow(["origin", "city_name", "airport_code", "date", "business_seats"])
    cur.execute(NEW_IN_SQL, params)
    for row in cur.fetchall():
        w.writerow(row)

print(f"✓ Written new-business report: {OUT_F}")

conn.close()
