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
    finally:
        conn.close()


if __name__ == "__main__":
    main()
