#!/usr/bin/env python3
"""
Smoke tests for Partner Awards. DB-only, no network.
Run: python -m partner_awards.tests.smoke
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _tmp_db():
    return sqlite3.connect(":memory:")


def test_init_and_watchlist():
    """Watchlist add/toggle works."""
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import list_watch_routes, upsert_watch_route, set_watch_route_enabled

    conn = _tmp_db()
    init_db(conn)
    routes_before = list_watch_routes(conn, "flyingblue")
    rid = upsert_watch_route(conn, "flyingblue", "AMS", "JNB", enabled=1)
    assert rid > 0
    routes = list_watch_routes(conn, "flyingblue")
    assert any(r["origin"] == "AMS" and r["destination"] == "JNB" for r in routes)
    set_watch_route_enabled(conn, rid, 0)
    routes = list_watch_routes(conn, "flyingblue")
    r = next(x for x in routes if x["id"] == rid)
    assert r["enabled"] is False
    set_watch_route_enabled(conn, rid, 1)
    conn.close()
    print("  watchlist add/toggle: OK")


def test_top_deals_ordering():
    """Top deals returns min ordering."""
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.top_deals import get_top_deals_for_month

    conn = _tmp_db()
    init_db(conn)
    conn.execute(
        """INSERT INTO partner_award_calendar_fares (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
           VALUES (1, '', 'AF', 'AMS', 'JNB', 'BUSINESS', '2026-03-15', 85000, 100, datetime('now'), datetime('now'))"""
    )
    conn.execute(
        """INSERT INTO partner_award_calendar_fares (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
           VALUES (1, '', 'AF', 'AMS', 'JNB', 'BUSINESS', '2026-03-20', 95000, 100, datetime('now'), datetime('now'))"""
    )
    conn.execute(
        """INSERT INTO partner_award_calendar_fares (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
           VALUES (1, '', 'AF', 'AMS', 'JNB', 'BUSINESS', '2026-03-10', 82000, 100, datetime('now'), datetime('now'))"""
    )
    conn.commit()
    rows, stats = get_top_deals_for_month(conn, "2026-03", "BUSINESS", [("AMS", "JNB")], limit=10)
    assert len(rows) == 3
    miles = [r["miles"] for r in rows]
    assert miles == sorted(miles)
    assert stats["global_min_miles"] == 82000
    conn.close()
    print("  top_deals ordering: OK")


def test_heatmap_structure():
    """Heatmap returns correct day keys and hm-min marking."""
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.heatmap import build_heatmap

    conn = _tmp_db()
    init_db(conn)
    for day in (10, 15, 20):
        conn.execute(
            """INSERT INTO partner_award_calendar_fares (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
               VALUES (1, '', 'AF', 'PAR', 'JNB', 'BUSINESS', '2026-03-' || ?, ?, 100, datetime('now'), datetime('now'))""",
            (f"{day:02d}", 75000 + day * 1000),
        )
    conn.commit()
    hm = build_heatmap(conn, "2026-03", "BUSINESS", [("PAR", "JNB")])
    assert "days" in hm and "rows" in hm
    assert 1 in hm["days"] and 31 in hm["days"]
    assert len(hm["rows"]) == 1
    row = hm["rows"][0]
    assert "values" in row and "month_min" in row and "css_classes" in row
    assert row["month_min"] == 85000  # day 10: 75000+10*1000
    # Day 10 should be hm-min
    assert row["css_classes"].get(10) == "hm-min"
    conn.close()
    print("  heatmap structure: OK")


def test_route_discovery_sorting():
    """Route discovery returns destination stats sorted correctly."""
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.route_discovery import discovery_multi_origin

    conn = _tmp_db()
    init_db(conn)
    # Two destinations: CPT cheaper, JNB more green days
    for dest, miles_list in [("CPT", [75000, 76000]), ("JNB", [80000, 80000])]:
        for d, m in [(10, miles_list[0]), (15, miles_list[1])]:
            conn.execute(
                """INSERT INTO partner_award_calendar_fares (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
                   VALUES (1, '', 'AF', 'AMS', ?, 'BUSINESS', '2026-03-' || ?, ?, 100, datetime('now'), datetime('now'))""",
                (dest, f"{d:02d}", m),
            )
    conn.commit()
    result = discovery_multi_origin(conn, ["AMS"], "BUSINESS", ["2026-03"], limit_per_origin=5)
    assert "AMS" in result
    rows = result["AMS"]
    assert len(rows) == 2
    # CPT has lower best_miles (75000) so should be first
    assert rows[0]["destination"] == "CPT"
    assert rows[0]["best_miles"] == 75000
    assert rows[1]["destination"] == "JNB"
    assert rows[1]["green_days_count"] == 2  # both days at min 80000
    conn.close()
    print("  route_discovery sorting: OK")


def test_watchlist_validation():
    """Watchlist rejects invalid inputs."""
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import upsert_watch_route

    conn = _tmp_db()
    init_db(conn)
    for bad_origin, bad_dest in [("A", "JNB"), ("XY1", "JNB"), ("AMS", "AMS")]:
        try:
            upsert_watch_route(conn, "flyingblue", bad_origin, bad_dest)
            assert False, f"Expected ValueError for {bad_origin}/{bad_dest}"
        except ValueError:
            pass
    conn.close()
    print("  watchlist validation: OK")


def main():
    print("Partner Awards smoke tests")
    test_init_and_watchlist()
    test_watchlist_validation()
    test_top_deals_ordering()
    test_heatmap_structure()
    test_route_discovery_sorting()
    print("All passed.")


if __name__ == "__main__":
    main()
    sys.exit(0)
