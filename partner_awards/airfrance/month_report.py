#!/usr/bin/env python3
"""
CLI: compute month delta and print Telegram-ready text.
Usage: python -m partner_awards.airfrance.month_report --origin AMS --destination JNB --month 2026-03 --cabin BUSINESS
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from partner_awards.airfrance.routes import PARTNER_DB_DIR, PARTNER_DB_PATH
from partner_awards.airfrance.adapter import init_db
from partner_awards.airfrance.calendar_delta import (
    get_scan_runs_for_month,
    get_month_fares_by_scan_run,
    compute_month_delta,
    build_telegram_month_text,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Partner Awards month report (delta + Telegram text)")
    parser.add_argument("--origin", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--cabin", default="BUSINESS")
    args = parser.parse_args()

    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(PARTNER_DB_PATH):
        print("Error: DB not found:", PARTNER_DB_PATH, file=sys.stderr)
        return 1

    conn = sqlite3.connect(PARTNER_DB_PATH)
    init_db(conn)
    runs = get_scan_runs_for_month(conn, args.origin, args.destination, args.cabin, args.month)
    if not runs:
        print("No scan data for this route/month/cabin", file=sys.stderr)
        conn.close()
        return 1

    latest = runs[0]
    prev = runs[1] if len(runs) > 1 else None
    latest_map = get_month_fares_by_scan_run(
        conn, latest["scan_run_id"], args.origin, args.destination, args.cabin, args.month
    )
    prev_map = get_month_fares_by_scan_run(
        conn, prev["scan_run_id"], args.origin, args.destination, args.cabin, args.month
    ) if prev else {}
    conn.close()

    delta = compute_month_delta(latest_map, prev_map)
    text = build_telegram_month_text(
        args.origin, args.destination, args.month, args.cabin,
        delta, latest,
        prev_missing=prev is None,
    )
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
