#!/usr/bin/env python3
"""
CLI verification: compare DB values for a route/month/cabin against expected (KLM screenshot).
Usage: python -m partner_awards.airfrance.verify_month --origin AMS --destination JNB --month 2026-03 --cabin BUSINESS
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from partner_awards.airfrance.routes import PARTNER_DB_DIR, PARTNER_DB_PATH

# Reference from KLM screenshot
EXPECTED_AMS_JNB_2026_03_BUSINESS = {
    2: 111000, 3: 85000, 4: 85000, 5: 85000, 6: 85000, 7: 222000, 8: 222000,
    9: 85000, 10: 85000, 11: 85000, 12: 85000, 13: 85000, 14: 111000, 15: 222000,
    16: 85000, 17: 85000, 18: 85000, 19: 85000, 20: 114000, 21: 114000, 22: 222000,
    23: 222000, 24: 85000, 25: 85000, 26: 114000, 27: 222000, 28: 222000, 29: 199500,
    30: 85000, 31: 85000,
}

# Amsterdam → Bangkok, Business, March 2026. We parse first connection (outbound) per date.
# API returns connections as [outbound, return]; calendar shows "lowest for departing flight".
# Days 1–2: API has no price; day 3: 563k (KLM may show 0.6m rounded).
EXPECTED_AMS_BKK_2026_03_BUSINESS = {
    3: 563000, 5: 85500, 6: 85500, 7: 85500, 8: 85500,
    9: 402500, 10: 85500, 11: 85500, 12: 85500, 13: 85500, 14: 85500, 15: 95500,
    16: 85500, 17: 85000, 18: 85000, 19: 130500, 20: 209500, 21: 281500,
    22: 209500, 23: 281500, 24: 209500, 25: 130500, 26: 209500, 27: 281500,
    28: 209500, 29: 281500, 30: 209500, 31: 209500,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify calendar fares against expected values")
    parser.add_argument("--origin", default="AMS")
    parser.add_argument("--destination", default="JNB")
    parser.add_argument("--month", default="2026-03")
    parser.add_argument("--cabin", default="BUSINESS")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()

    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    if not os.path.exists(PARTNER_DB_PATH):
        print("FAIL: DB not found:", PARTNER_DB_PATH)
        return 1

    if args.origin == "AMS" and args.destination == "JNB" and args.month == "2026-03" and args.cabin == "BUSINESS":
        expected = EXPECTED_AMS_JNB_2026_03_BUSINESS
    elif args.origin == "AMS" and args.destination == "BKK" and args.month == "2026-03" and args.cabin == "BUSINESS":
        expected = EXPECTED_AMS_BKK_2026_03_BUSINESS
    else:
        expected = {}

    conn = sqlite3.connect(PARTNER_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT depart_date, miles FROM partner_award_calendar_fares
           WHERE source='AF' AND origin=? AND destination=? AND cabin_class=?
           AND depart_date >= ? AND depart_date <= ?""",
        (args.origin, args.destination, args.cabin, f"{args.month}-01", f"{args.month}-31"),
    )
    rows = {int(r["depart_date"][8:10]): r["miles"] for r in cur.fetchall()}
    conn.close()

    found_count = len(rows)
    min_miles = min((m for m in rows.values() if m is not None), default=None)
    mismatches = []
    if expected:
        for day, exp in expected.items():
            got = rows.get(day)
            if got != exp:
                mismatches.append({"day": day, "expected": exp, "got": got})
    ok = len(mismatches) == 0 and (min_miles == 85000 if expected else found_count > 0)

    if args.json:
        print(json.dumps({"ok": ok, "mismatches": mismatches, "found_count": found_count, "min_miles": min_miles}))
    else:
        if ok:
            print(f"PASS: min_miles={min_miles}, found_count={found_count}")
        else:
            print(f"FAIL: min_miles={min_miles}, mismatches={len(mismatches)}")
            for m in mismatches[:10]:
                print(f"  Day {m['day']}: expected {m['expected']}, got {m['got']}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
