#!/usr/bin/env python3
"""
Non-network dev test for calendar fare ingestion.
Verifies: ingest twice -> no duplicates, upsert works, best miles correct.
Run: python -m partner_awards.airfrance.dev_test_calendar_ingest
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

# Project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from partner_awards.airfrance.adapter import init_db, create_scan_run, ingest_lowest_fares


def _count_calendar_fares(conn: sqlite3.Connection, origin: str = "TEST", destination: str = "JNB") -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM partner_award_calendar_fares WHERE origin=? AND destination=?",
        (origin, destination),
    )
    return cur.fetchone()[0]


def _get_best_miles(conn: sqlite3.Connection, origin: str = "TEST", destination: str = "JNB") -> int | None:
    cur = conn.execute(
        """SELECT MIN(miles) FROM partner_award_calendar_fares
           WHERE origin=? AND destination=? AND miles IS NOT NULL""",
        (origin, destination),
    )
    row = cur.fetchone()
    return row[0] if row else None


def main() -> int:
    fixture_path = Path(__file__).resolve().parent.parent.parent / "fixtures" / "airfrance" / "LowestFareOffers_sample.json"
    if not fixture_path.exists():
        print("FAIL: fixture not found:", fixture_path)
        return 1

    with open(fixture_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    conn = sqlite3.connect(":memory:")
    init_db(conn)

    origin, destination = "TEST", "JNB"
    cabins = ["ECONOMY"]

    # First ingest
    scan_run_id = create_scan_run(
        conn,
        source="AF",
        ingest_type="dev_test",
        origin=origin,
        destination=destination,
        cabin_requested=",".join(cabins),
        depart_date=None,
        host_used="KLM-SE",
    )
    n1 = ingest_lowest_fares(
        conn,
        scan_run_id=scan_run_id,
        payload=payload,
        origin=origin,
        destination=destination,
        cabins=cabins,
        host_used="KLM-SE",
    )
    count1 = _count_calendar_fares(conn, origin, destination)
    best1 = _get_best_miles(conn, origin, destination)

    # Second ingest (same data - should upsert, no new rows)
    scan_run_id2 = create_scan_run(
        conn,
        source="AF",
        ingest_type="dev_test",
        origin=origin,
        destination=destination,
        cabin_requested=",".join(cabins),
        depart_date=None,
        host_used="KLM-SE",
    )
    n2 = ingest_lowest_fares(
        conn,
        scan_run_id=scan_run_id2,
        payload=payload,
        origin=origin,
        destination=destination,
        cabins=cabins,
        host_used="KLM-SE",
    )
    count2 = _count_calendar_fares(conn, origin, destination)
    best2 = _get_best_miles(conn, origin, destination)

    conn.close()

    ok = True
    if count1 != 5:
        print(f"FAIL: first ingest expected 5 rows, got {count1}")
        ok = False
    if count2 != 5:
        print(f"FAIL: second ingest expected 5 rows (no duplicates), got {count2}")
        ok = False
    if best1 != 48000 or best2 != 48000:
        print(f"FAIL: best miles expected 48000, got {best1} / {best2}")
        ok = False
    if n1 != 5 or n2 != 5:
        print(f"FAIL: ingest_lowest_fares returned {n1}, {n2} (expected 5, 5)")
        ok = False

    if ok:
        print("PASS: ingest twice -> no duplicates, best miles=48000")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
