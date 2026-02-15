#!/usr/bin/env python3
import os, sqlite3, csv

from datetime import date

# ─── CONFIG ────────────────────────────────────────────────────────────────
HOME    = os.path.expanduser("~")
DB_FILE = os.path.join(HOME, "sas_awards", "sas_awards.sqlite")
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
    SELECT airport_code, city_name, flight_date, ab AS business_seats
      FROM flight_history
     WHERE fetch_date = :latest
       AND direction   = 'outbound'
       AND ab > 0
  ),
  p AS (
    SELECT airport_code, flight_date
      FROM flight_history
     WHERE fetch_date = :prev
       AND direction   = 'outbound'
       AND ab > 0
  )
SELECT t.city_name, t.airport_code, t.flight_date, t.business_seats
  FROM t
  LEFT JOIN p
    ON p.airport_code = t.airport_code
   AND p.flight_date  = t.flight_date
 WHERE p.airport_code IS NULL
 ORDER BY t.city_name COLLATE NOCASE, t.flight_date;
"""

# 3) same for inbound
NEW_IN_SQL = NEW_OUT_SQL.replace("direction   = 'outbound'",
                                  "direction   = 'inbound'")

# 4) run and write CSV
with open(OUT_F, "w", newline="") as f:
    w = csv.writer(f)

    # Outbound block
    w.writerow(["Outbound Business (NEW since", prev, "→", latest, ")"])
    w.writerow(["city_name", "airport_code", "flight_date", "business_seats"])
    cur.execute(NEW_OUT_SQL, {"latest": latest, "prev": prev})
    for row in cur.fetchall():
        w.writerow(row)

    # blank line
    w.writerow([])

    # Inbound block
    w.writerow(["Inbound Business (NEW since", prev, "→", latest, ")"])
    w.writerow(["city_name", "airport_code", "flight_date", "business_seats"])
    cur.execute(NEW_IN_SQL, {"latest": latest, "prev": prev})
    for row in cur.fetchall():
        w.writerow(row)

print(f"✓ Written new-business report: {OUT_F}")

conn.close()
