"""
Top deals: cheapest days across watchlist routes for a month+cabin.
Uses partner_award_calendar_fares with best-overall (MIN miles across hosts).
"""

from __future__ import annotations

import calendar
import sqlite3
from typing import Any


def _month_range(month: str) -> tuple[str, str]:
    start = f"{month}-01"
    try:
        y, m = int(month[:4]), int(month[5:7])
        last_d = calendar.monthrange(y, m)[1]
        end = f"{month}-{last_d:02d}"
    except (ValueError, IndexError):
        end = f"{month}-31"
    return start, end


def get_top_deals_for_month(
    conn: sqlite3.Connection,
    month: str,
    cabin: str,
    routes: list[tuple[str, str]],
    limit: int = 50,
    mode: str = "best_overall",
) -> tuple[list[dict], dict[str, Any]]:
    """
    Returns (rows, stats).
    rows: origin, destination, depart_date, miles, host_used, scan_run_id, updated_at
    stats: global_min_miles, days_at_min, total_days
    """
    if not routes:
        return [], {"global_min_miles": None, "days_at_min": 0, "total_days": 0}

    start, end = _month_range(month)

    route_conds = " OR ".join("(origin=? AND destination=?)" for _ in routes)
    route_params = [p for pair in routes for p in pair]

    sql = f"""
    SELECT b.origin, b.destination, b.depart_date, b.min_m as miles,
           (SELECT tax FROM partner_award_calendar_fares c
            WHERE c.source='AF' AND c.origin=b.origin AND c.destination=b.destination
              AND c.depart_date=b.depart_date AND c.cabin_class=? AND c.miles=b.min_m
            ORDER BY COALESCE(c.tax, 999999) ASC LIMIT 1),
           (SELECT host_used FROM partner_award_calendar_fares c
            WHERE c.source='AF' AND c.origin=b.origin AND c.destination=b.destination
              AND c.depart_date=b.depart_date AND c.cabin_class=? AND c.miles=b.min_m LIMIT 1),
           (SELECT scan_run_id FROM partner_award_calendar_fares c
            WHERE c.source='AF' AND c.origin=b.origin AND c.destination=b.destination
              AND c.depart_date=b.depart_date AND c.cabin_class=? AND c.miles=b.min_m LIMIT 1),
           (SELECT updated_at FROM partner_award_calendar_fares c
            WHERE c.source='AF' AND c.origin=b.origin AND c.destination=b.destination
              AND c.depart_date=b.depart_date AND c.cabin_class=? AND c.miles=b.min_m LIMIT 1)
    FROM (
      SELECT origin, destination, depart_date, MIN(miles) as min_m
      FROM partner_award_calendar_fares
      WHERE source='AF' AND cabin_class=? AND depart_date >= ? AND depart_date <= ?
        AND ({route_conds}) AND miles IS NOT NULL
      GROUP BY origin, destination, depart_date
    ) b
    ORDER BY b.min_m ASC, b.depart_date ASC
    LIMIT ?
    """
    sql_params = [cabin, cabin, cabin, cabin, cabin, start, end] + route_params + [limit]

    cur = conn.execute(sql, sql_params)
    rows = [
        {
            "origin": r[0],
            "destination": r[1],
            "depart_date": r[2],
            "miles": r[3],
            "tax": r[4],
            "host_used": r[5] or "",
            "scan_run_id": r[6],
            "updated_at": r[7] or "",
        }
        for r in cur.fetchall()
    ]

    miles_list = [r["miles"] for r in rows if r["miles"] is not None]
    global_min = min(miles_list, default=None)
    days_at_min = sum(1 for r in rows if r["miles"] == global_min) if global_min else 0

    cur2 = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
          SELECT origin, destination, depart_date, MIN(miles) as m
          FROM partner_award_calendar_fares
          WHERE source='AF' AND cabin_class=? AND depart_date >= ? AND depart_date <= ?
            AND ({route_conds}) AND miles IS NOT NULL
          GROUP BY origin, destination, depart_date
        )
        """,
        [cabin, start, end] + route_params,
    )
    total_days = cur2.fetchone()[0] if cur2 else 0

    stats = {
        "global_min_miles": global_min,
        "days_at_min": days_at_min,
        "total_days": total_days,
    }
    return rows, stats
