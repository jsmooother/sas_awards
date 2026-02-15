#!/usr/bin/env python3
"""
Morning report: formats a summary of flights and sends to Telegram.
Requires: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
Run after update_sas_awards.py and report scripts (e.g. via cron 06:20).
"""
import os
import sys

# Load .env when run from cron (WorkingDirectory = project root)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import sqlite3
import requests

DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MAX_LEN = 4000  # Telegram limit 4096, leave margin

MIN_SEATS = 2
TRIP_DAYS_MIN, TRIP_DAYS_MAX = 3, 4

# Long-haul = Asia + North America (Swedish country names from SAS API)
LONG_HAUL_COUNTRIES = ("USA", "Kanada", "Japan", "Korea", "Indien", "Thailand", "Förenade arabemiraten")


def get_conn():
    return sqlite3.connect(DB_PATH)


def summary_counts(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT origin, direction, COUNT(*) FROM flights
        WHERE ab >= ? OR ap >= ? OR ag >= ?
        GROUP BY origin, direction
    """, (MIN_SEATS, MIN_SEATS, MIN_SEATS))
    rows = cur.fetchall()
    return rows


def top_business(conn, n=10):
    cur = conn.cursor()
    cur.execute("""
        SELECT origin, city_name, airport_code, date, direction, ab
        FROM flights
        WHERE ab >= ?
        ORDER BY ab DESC, date
        LIMIT ?
    """, (MIN_SEATS, n))
    return cur.fetchall()


def new_since_yesterday(conn, n=10):
    """New business flights since previous fetch_date (if flight_history exists)."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flight_history'")
    if not cur.fetchone():
        return []
    cur.execute("""
        SELECT DISTINCT fetch_date FROM flight_history ORDER BY fetch_date DESC LIMIT 2
    """)
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        return []
    latest, prev = dates[0], dates[1]
    cur.execute("""
        WITH t AS (
            SELECT origin, airport_code, city_name, date, direction, ab FROM flight_history
            WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
        ),
        p AS (
            SELECT origin, airport_code, date FROM flight_history
            WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
        )
        SELECT t.origin, t.city_name, t.airport_code, t.date, t.ab FROM t
        LEFT JOIN p ON p.origin = t.origin AND p.airport_code = t.airport_code AND p.date = t.date
        WHERE p.airport_code IS NULL
        ORDER BY t.ab DESC, t.date LIMIT ?
    """, (latest, MIN_SEATS, prev, MIN_SEATS, n))
    return cur.fetchall()


def new_longhaul_business_weekend_pairs(conn):
    """New long-haul (Asia/N America) weekend pairs in Business (≥2 seats) since prev fetch."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flight_history'")
    if not cur.fetchone():
        return []
    cur.execute("SELECT DISTINCT fetch_date FROM flight_history ORDER BY fetch_date DESC LIMIT 2")
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        return []
    latest, prev = dates[0], dates[1]
    ph = ", ".join("?" * len(LONG_HAUL_COUNTRIES))
    # Current pairs from flights
    cur.execute(f"""
        SELECT inb.origin, inb.airport_code, inb.city_name, outb.date, inb.date, outb.ab, inb.ab
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND inb.ab >= ? AND outb.ab >= ?
          AND inb.country_name IN ({ph})
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
    """, (MIN_SEATS, MIN_SEATS) + LONG_HAUL_COUNTRIES + (TRIP_DAYS_MIN, TRIP_DAYS_MAX))
    current = {(r[0], r[1], r[3], r[4]): (r[2], r[5], r[6]) for r in cur.fetchall()}
    # Previous pairs from flight_history
    cur.execute(f"""
        SELECT o.origin, o.airport_code, o.date, i.date
        FROM flight_history o
        JOIN flight_history i ON o.origin = i.origin AND o.airport_code = i.airport_code
        WHERE o.fetch_date = ? AND i.fetch_date = ?
          AND o.direction = 'outbound' AND i.direction = 'inbound'
          AND o.ab >= ? AND i.ab >= ?
          AND o.country_name IN ({ph}) AND i.country_name IN ({ph})
          AND strftime('%w', i.date) IN ('6','0','1')
          AND strftime('%w', o.date) IN ('3','4','5')
          AND (julianday(i.date) - julianday(o.date)) BETWEEN ? AND ?
          AND date(i.date) BETWEEN date('now') AND date('now','+1 year')
          AND date(o.date) BETWEEN date(i.date, '-7 days') AND date(i.date, '-1 days')
    """, (prev, prev, MIN_SEATS, MIN_SEATS) + LONG_HAUL_COUNTRIES + LONG_HAUL_COUNTRIES + (TRIP_DAYS_MIN, TRIP_DAYS_MAX))
    prev_set = {(r[0], r[1], r[2], r[3]) for r in cur.fetchall()}
    new = [(k[0], k[1], k[2], k[3], current[k]) for k in current if k not in prev_set]
    return sorted(new, key=lambda x: (x[0], x[4][0], x[3]))


def top_weekend_cities(conn, n=8):
    """Cities with most weekend pairs (min 2 seats, 3–4 day trip)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT inb.origin, inb.city_name, COUNT(*) AS pairs
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND (inb.ag>=? OR inb.ap>=?) AND (outb.ag>=? OR outb.ap>=?)
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
        GROUP BY inb.origin, inb.city_name
        ORDER BY pairs DESC
        LIMIT ?
    """, (MIN_SEATS, MIN_SEATS, MIN_SEATS, MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX, n))
    return cur.fetchall()


def format_report(conn):
    parts = ["*SAS Awards – Morning Report*", ""]

    # Summary counts
    counts = summary_counts(conn)
    if counts:
        parts.append("*Summary (≥2 seats)*")
        for origin, direction, cnt in counts:
            parts.append(f"  {origin} {direction}: {cnt} flight-dates")
        parts.append("")

    # New since yesterday
    new_rows = new_since_yesterday(conn)
    if new_rows:
        parts.append("*New business (since yesterday)*")
        for origin, city, code, date, ab in new_rows[:8]:
            parts.append(f"  {origin} {date} {city} ({code}) {ab}B")
        if len(new_rows) > 8:
            parts.append(f"  ... +{len(new_rows)-8} more")
        parts.append("")

    # Top weekend cities
    weekend = top_weekend_cities(conn)
    if weekend:
        parts.append("*Top weekend cities*")
        for origin, city, pairs in weekend:
            parts.append(f"  {origin} {city}: {pairs} pairs")
        parts.append("")

    # Top business
    business = top_business(conn, 5)
    if business:
        parts.append("*Top business (sample)*")
        for origin, city, code, date, direction, ab in business:
            parts.append(f"  {origin} {date} {city} {direction} {ab}B")
        parts.append("")

    msg = "\n".join(parts)
    if len(msg) > MAX_LEN:
        msg = msg[:MAX_LEN - 50] + "\n…(truncated)"
    return msg.strip()


def send_telegram(text):
    if not TOKEN or not CHAT_ID:
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID", file=sys.stderr)
        return False
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
    if not r.ok:
        print(f"Telegram API error: {r.status_code} {r.text}", file=sys.stderr)
        return False
    return True


def main():
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = get_conn()
    try:
        report = format_report(conn)
        print(report)
        if TOKEN and CHAT_ID:
            if send_telegram(report):
                print("Sent to Telegram.", file=sys.stderr)
            else:
                sys.exit(1)
        else:
            print("(Skipped Telegram – no token/chat_id)", file=sys.stderr)

        # Separate ping: new long-haul (Asia/N America) business weekend pairs
        new_pairs = new_longhaul_business_weekend_pairs(conn)
        if new_pairs and TOKEN and CHAT_ID:
            lines = ["*🛫 New long-haul Business weekend pairs* (≥2 seats, 3–4 days)", ""]
            for origin, code, outb, inb, (city, ab_out, ab_in) in new_pairs:
                lines.append(f"  {origin} {city} ({code}) {outb} → {inb}  (B: {ab_out}/{ab_in})")
            ping = "\n".join(lines)
            if send_telegram(ping):
                print("Sent long-haul alert to Telegram.", file=sys.stderr)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
