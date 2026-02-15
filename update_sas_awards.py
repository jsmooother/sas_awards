#!/usr/bin/env python3
import requests
import sqlite3
import datetime
import os
import sys

# ——— CONFIGURATION —————————————————————————————————————————————
DB_PATH      = os.path.expanduser("~/sas_awards/sas_awards.sqlite")
BASE_URL     = "https://beta.sas.se/bff/award-finder/destinations/v1"
MARKET       = "se-sv"
ORIGIN       = "ARN"
PASSENGERS   = 1
# leave SELECT_CLASS = "" for all cabins; set to "AG", "AP" or "AB" to restrict
SELECT_CLASS = ""
# ——————————————————————————————————————————————————————————————

def get_all_destinations():
    """Fetch list of all destination codes."""
    params = {
        "market": MARKET,
        "origin": ORIGIN,
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

def fetch_availability(dest_code):
    """Fetch outbound/inbound availability for one code."""
    params = {
        "market": MARKET,
        "origin": ORIGIN,
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
    """Ensure flights table exists and return connection."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
      CREATE TABLE IF NOT EXISTS flights (
        airport_code TEXT,
        city_name    TEXT,
        country_name TEXT,
        direction    TEXT,
        date         TEXT,
        total        INTEGER,
        ag           INTEGER,
        ap           INTEGER,
        ab           INTEGER,
        PRIMARY KEY (airport_code, direction, date)
      )
    """)
    conn.commit()
    return conn

def snapshot(conn):
    """Return a set of tuples for existing flights."""
    cur = conn.cursor()
    cur.execute("""
      SELECT airport_code, direction, date, total, ag, ap, ab
        FROM flights
    """)
    return set(cur.fetchall())

def rewrite_all(conn, rows):
    """Delete all old flights and insert these new rows."""
    cur = conn.cursor()
    cur.execute("DELETE FROM flights")
    cur.executemany("""
      INSERT OR REPLACE INTO flights
        (airport_code, city_name, country_name, direction, date, total, ag, ap, ab)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

def main():
    conn = connect_db()
    before = snapshot(conn)

    # 1) Fetch master list of destinations
    all_dests = get_all_destinations()

    # 2) Fetch availability for each
    rows = []
    for d in all_dests:
        code    = d["airportCode"]
        city    = d["cityName"]
        country = d.get("countryName","")
        rec     = fetch_availability(code)
        if not rec:
            continue
        for direction in ("outbound","inbound"):
            for x in rec.get("availability",{}).get(direction,[]):
                rows.append((
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

    # 4) Diff old vs new
    after = snapshot(conn)
    added   = after - before
    removed = before - after

    # 5) Print only today's changes
    now = datetime.datetime.now().isoformat()
    print(f"Run at {now}")
    if added:
        print("🆕 Added flights:")
        for code, direction, date, total, ag, ap, ab in sorted(added):
            print(f"• {date} {direction:<8} {code} | tot={total} AG={ag} AP={ap} AB={ab}")
    if removed:
        print("❌ Removed flights:")
        for code, direction, date, total, ag, ap, ab in sorted(removed):
            print(f"• {date} {direction:<8} {code} | tot={total} AG={ag} AP={ap} AB={ab}")
    if not added and not removed:
        print("No new flights today!")

    conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        sys.exit(1)
