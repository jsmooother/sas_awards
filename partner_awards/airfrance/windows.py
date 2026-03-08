"""
Round-trip window search: find outbound + inbound pairs within trip length constraints.
Uses partner_award_calendar_fares. Returns pairs with Excellent/Good flags.
"""

from __future__ import annotations

import calendar
import sqlite3
from datetime import datetime
from typing import Any


def get_round_trip_pairs(
    conn: sqlite3.Connection,
    origin: str,
    destination: str,
    cabin: str,
    month: str,
    trip_nights_min: int,
    trip_nights_max: int,
) -> list[dict[str, Any]]:
    """
    Find round-trip pairs (outbound origin→destination, inbound destination→origin)
    within [trip_nights_min, trip_nights_max] nights for the given month.

    Returns list of:
      out_date, in_date, nights, out_miles, in_miles, total_miles, is_excellent, is_good

    - is_excellent: both legs <= 1.15× route min (per direction)
    - is_good: at least one leg <= route min
    - Route min = MIN(miles) for that route in the month (per direction)
    """
    if trip_nights_min > trip_nights_max:
        return []

    start_date = f"{month}-01"
    try:
        y, m = int(month[:4]), int(month[5:7])
        last_day = calendar.monthrange(y, m)[1]
    except (ValueError, IndexError):
        return []
    end_date = f"{month}-{last_day:02d}"

    # Fetch outbound (origin → destination) and inbound (destination → origin)
    cur = conn.execute(
        """
        SELECT origin, destination, depart_date, MIN(miles) as miles
        FROM partner_award_calendar_fares
        WHERE source='AF' AND cabin_class=? AND miles IS NOT NULL
          AND depart_date >= ? AND depart_date <= ?
          AND ((origin=? AND destination=?) OR (origin=? AND destination=?))
        GROUP BY origin, destination, depart_date
        """,
        (cabin, start_date, end_date, origin, destination, destination, origin),
    )
    rows = cur.fetchall()

    outbound: dict[str, int] = {}  # date -> miles
    inbound: dict[str, int] = {}   # date -> miles
    for o, d, dep, miles in rows:
        if o == origin and d == destination:
            outbound[dep] = miles
        elif o == destination and d == origin:
            inbound[dep] = miles

    min_out = min(outbound.values(), default=None)
    min_in = min(inbound.values(), default=None)

    pairs: list[dict[str, Any]] = []
    for out_date_str, out_miles in outbound.items():
        try:
            out_dt = datetime.strptime(out_date_str, "%Y-%m-%d")
        except ValueError:
            continue
        for in_date_str, in_miles in inbound.items():
            try:
                in_dt = datetime.strptime(in_date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if in_dt <= out_dt:
                continue
            nights = (in_dt - out_dt).days
            if trip_nights_min <= nights <= trip_nights_max:
                total = out_miles + in_miles
                is_excellent = False
                is_good = False
                if min_out and min_in:
                    out_ratio = out_miles / min_out if min_out else 0
                    in_ratio = in_miles / min_in if min_in else 0
                    is_excellent = out_ratio <= 1.15 and in_ratio <= 1.15
                    is_good = out_ratio <= 1.0 or in_ratio <= 1.0
                pairs.append({
                    "out_date": out_date_str,
                    "in_date": in_date_str,
                    "nights": nights,
                    "out_miles": out_miles,
                    "in_miles": in_miles,
                    "total_miles": total,
                    "is_excellent": is_excellent,
                    "is_good": is_good,
                })
    pairs.sort(key=lambda p: (p["out_date"], p["in_date"]))
    return pairs


def get_routes_with_data(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Return distinct (origin, destination) from partner_award_calendar_fares for AF."""
    cur = conn.execute(
        """SELECT DISTINCT origin, destination
           FROM partner_award_calendar_fares
           WHERE source='AF'
           ORDER BY origin, destination""",
    )
    return [(r[0], r[1]) for r in cur.fetchall()]
