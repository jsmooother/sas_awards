#!/usr/bin/env python3
"""
Remove a route (origin, destination) from the partner awards DB.
Usage: python scripts/remove_partner_route.py [ORIGIN] [DEST]
Default: TEST JNB (removes test data).

Also prints AMS-BKK 2026-04-29 Business rows so you can verify stored prices.
"""
import os
import sqlite3
import sys

PARTNER_DB_DIR = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards"))
DB_PATH = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(PARTNER_DB_DIR, "partner_awards.sqlite")


def main():
    origin = (sys.argv[1] if len(sys.argv) > 1 else "TEST").strip().upper()
    dest = (sys.argv[2] if len(sys.argv) > 2 else "JNB").strip().upper()

    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    # Delete in order (referenced tables first where applicable)
    tables_route = [
        ("partner_award_calendar_fares", "origin=? AND destination=?"),
        ("partner_award_watch_routes", "origin=? AND destination=?"),
        ("partner_award_job_tasks", "origin=? AND destination=?"),
    ]
    for table, where in tables_route:
        cur = conn.execute(f"DELETE FROM {table} WHERE {where}", (origin, dest))
        print(f"Deleted {cur.rowcount} row(s) from {table} ({origin}→{dest})")

    # Offers: delete cabins then offers
    cur = conn.execute(
        """DELETE FROM partner_award_offer_cabins WHERE offer_id IN (
           SELECT id FROM partner_award_offers WHERE source='AF' AND origin=? AND destination=?
        )""",
        (origin, dest),
    )
    print(f"Deleted {cur.rowcount} row(s) from partner_award_offer_cabins (via offers)")
    cur = conn.execute(
        "DELETE FROM partner_award_offers WHERE source='AF' AND origin=? AND destination=?",
        (origin, dest),
    )
    print(f"Deleted {cur.rowcount} row(s) from partner_award_offers")
    cur = conn.execute(
        "DELETE FROM partner_award_best_offers WHERE source='AF' AND origin=? AND destination=?",
        (origin, dest),
    )
    print(f"Deleted {cur.rowcount} row(s) from partner_award_best_offers")

    # Scan runs for this route (raw_responses reference scan_run_id)
    cur = conn.execute(
        "SELECT id FROM partner_award_scan_runs WHERE origin=? AND destination=?",
        (origin, dest),
    )
    run_ids = [r[0] for r in cur.fetchall()]
    if run_ids:
        placeholders = ",".join("?" * len(run_ids))
        conn.execute(f"DELETE FROM partner_award_raw_responses WHERE scan_run_id IN ({placeholders})", run_ids)
        conn.execute(f"DELETE FROM partner_award_scan_runs WHERE id IN ({placeholders})", run_ids)
        print(f"Deleted scan runs and raw responses for {len(run_ids)} run(s)")

    conn.commit()
    print(f"\nDone. Removed {origin}→{dest} from DB.")

    # Show AMS-BKK 29 Apr 2026 Business rows (to verify 50k issue)
    print("\n--- AMS→BKK 2026-04-29 BUSINESS (all rows in DB) ---")
    cur = conn.execute(
        """SELECT depart_date, cabin_class, miles, host_used, updated_at
           FROM partner_award_calendar_fares
           WHERE source='AF' AND origin='AMS' AND destination='BKK'
             AND depart_date='2026-04-29' AND cabin_class='BUSINESS'
           ORDER BY miles ASC""",
    )
    rows = cur.fetchall()
    if not rows:
        print("No rows found.")
    else:
        for r in rows:
            print(f"  {r[0]} {r[1]} miles={r[2]} host={r[3]!r} updated={r[4]}")
        print(f"  → Displayed value is MIN(miles) = {min(r[2] for r in rows if r[2] is not None)}")
    conn.close()


if __name__ == "__main__":
    main()
