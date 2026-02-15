#!/usr/bin/env python3
import requests
import sqlite3
import datetime
import os
import sys

# ——— CONFIGURATION —————————————————————————————————————————————
DB_PATH      = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))
BASE_URL     = "https://www.sas.se/bff/award-finder/destinations/v1"
MARKET       = "se-sv"
ORIGINS      = ["ARN", "CPH"]  # Stockholm Arlanda, Copenhagen
PASSENGERS   = 1
# leave SELECT_CLASS = "" for all cabins; set to "AG", "AP" or "AB" to restrict
SELECT_CLASS = ""
# ——————————————————————————————————————————————————————————————

def get_all_destinations(origin):
    """Fetch list of all destination codes from given origin."""
    params = {
        "market": MARKET,
        "origin": origin,
        "destinations": "",
        "selectedMonth": "",
        "passengers": PASSENGERS,
        "direct":  str(False).lower(),   # yields "false"
        "availability": str(False).lower(),
        "selectedFlightClass": SELECT_CLASS,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_availability(origin, dest_code):
    """Fetch outbound/inbound availability for one origin-destination pair."""
    params = {
        "market": MARKET,
        "origin": origin,
        "destinations": dest_code,
        "selectedMonth": "",
        "passengers": PASSENGERS,
        "direct":  str(False).lower(),
        "availability": str(True).lower(),
        "selectedFlightClass": SELECT_CLASS,
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data[0] if data else None

def connect_db():
    """Ensure flights table exists (with origin) and return connection."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Check if old schema (no origin) exists – migrate
    c.execute("PRAGMA table_info(flights)")
    cols = [row[1] for row in c.fetchall()]
    if cols and "origin" not in cols:
        c.execute("""
          CREATE TABLE flights_new (
            origin       TEXT,
            airport_code TEXT,
            city_name    TEXT,
            country_name TEXT,
            direction    TEXT,
            date         TEXT,
            total        INTEGER,
            ag           INTEGER,
            ap           INTEGER,
            ab           INTEGER,
            PRIMARY KEY (origin, airport_code, direction, date)
          )
        """)
        c.execute("""
          INSERT INTO flights_new (origin, airport_code, city_name, country_name, direction, date, total, ag, ap, ab)
          SELECT 'ARN', airport_code, city_name, country_name, direction, date, total, ag, ap, ab FROM flights
        """)
        c.execute("DROP TABLE flights")
        c.execute("ALTER TABLE flights_new RENAME TO flights")
        conn.commit()
    c.execute("""
      CREATE TABLE IF NOT EXISTS flights (
        origin       TEXT,
        airport_code TEXT,
        city_name    TEXT,
        country_name TEXT,
        direction    TEXT,
        date         TEXT,
        total        INTEGER,
        ag           INTEGER,
        ap           INTEGER,
        ab           INTEGER,
        PRIMARY KEY (origin, airport_code, direction, date)
      )
    """)
    conn.commit()
    return conn

def snapshot(conn):
    """Return a set of tuples for existing flights."""
    cur = conn.cursor()
    cur.execute("""
      SELECT origin, airport_code, direction, date, total, ag, ap, ab
        FROM flights
    """)
    return set(cur.fetchall())

def ensure_flight_history(conn):
    """Create flight_history table if not exists."""
    conn.cursor().execute("""
      CREATE TABLE IF NOT EXISTS flight_history (
        fetch_date   TEXT,
        origin       TEXT,
        airport_code TEXT,
        city_name    TEXT,
        country_name TEXT,
        direction    TEXT,
        date         TEXT,
        total        INTEGER,
        ag           INTEGER,
        ap           INTEGER,
        ab           INTEGER,
        PRIMARY KEY (fetch_date, origin, airport_code, direction, date)
      )
    """)
    conn.commit()

def snapshot_to_history(conn, fetch_date):
    """Copy current flights into flight_history, then prune to last 7 days."""
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO flight_history (fetch_date, origin, airport_code, city_name, country_name, direction, date, total, ag, ap, ab)
      SELECT ?, origin, airport_code, city_name, country_name, direction, date, total, ag, ap, ab FROM flights
    """, (fetch_date,))
    cur.execute("""
      DELETE FROM flight_history
      WHERE fetch_date < date('now', '-7 days')
    """)
    conn.commit()

def rewrite_all(conn, rows):
    """Delete all old flights and insert these new rows."""
    cur = conn.cursor()
    cur.execute("DELETE FROM flights")
    cur.executemany("""
      INSERT OR REPLACE INTO flights
        (origin, airport_code, city_name, country_name, direction, date, total, ag, ap, ab)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

def main():
    conn = connect_db()
    ensure_flight_history(conn)
    before = snapshot(conn)
    fetch_date = datetime.datetime.now().strftime("%Y-%m-%d")

    rows = []
    for origin in ORIGINS:
        # 1) Fetch master list of destinations from this origin
        all_dests = get_all_destinations(origin)

        # 2) Fetch availability for each destination
        for d in all_dests:
            code    = d["airportCode"]
            city    = d["cityName"]
            country = d.get("countryName", "")
            rec     = fetch_availability(origin, code)
            if not rec:
                continue
            for direction in ("outbound", "inbound"):
                for x in rec.get("availability", {}).get(direction, []):
                    rows.append((
                        origin,
                        code,
                        city,
                        country,
                        direction,
                        x["date"],
                        x.get("availableSeatsTotal", 0),
                        x.get("AG", 0),
                        x.get("AP", 0),
                        x.get("AB", 0),
                    ))

    # 3) Store to DB
    rewrite_all(conn, rows)

    # 4) Snapshot new state to flight_history (for "new since yesterday" reports)
    snapshot_to_history(conn, fetch_date)

    after = snapshot(conn)
    added   = after - before
    removed = before - after

    # 6) Print only today's changes
    now = datetime.datetime.now().isoformat()
    print(f"Run at {now}")
    if added:
        print("🆕 Added flights:")
        for origin, code, direction, date, total, ag, ap, ab in sorted(added):
            print(f"• {date} {origin} {direction:<8} {code} | tot={total} AG={ag} AP={ap} AB={ab}")
    if removed:
        print("❌ Removed flights:")
        for origin, code, direction, date, total, ag, ap, ab in sorted(removed):
            print(f"• {date} {origin} {direction:<8} {code} | tot={total} AG={ag} AP={ap} AB={ab}")
    if not added and not removed:
        print("No new flights today!")

    conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
