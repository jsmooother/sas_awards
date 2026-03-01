"""
Heatmap: route × day-of-month grid for selected month+cabin.
DB-only, uses partner_award_calendar_fares with best_overall (min miles across hosts).
"""

from __future__ import annotations

import calendar
import sqlite3
from typing import Any


def _month_range(month: str) -> tuple[int, int, list[int]]:
    """Return (year, last_day, days_list [1..last_day])."""
    try:
        y, m = int(month[:4]), int(month[5:7])
        last_d = calendar.monthrange(y, m)[1]
        return y, last_d, list(range(1, last_d + 1))
    except (ValueError, IndexError):
        return 0, 0, []


def _format_miles_k(miles: int | None) -> str:
    """Format miles as 85k, 111k, 199.5k."""
    if miles is None:
        return "—"
    if miles >= 1000:
        k = miles / 1000
        return f"{k:.1f}k"
    return str(miles)


def build_heatmap(
    conn: sqlite3.Connection,
    month: str,
    cabin_class: str,
    routes: list[tuple[str, str]],
    mode: str = "best_overall",
    max_routes: int = 30,
) -> dict[str, Any]:
    """
    Build route × day heatmap for given month and cabin.
    routes: list of (origin, destination)
    Returns:
      days: [1..last_day]
      rows: [{origin, destination, values: {day->miles}, month_min, display_values: {day->str}, css_classes: {day->str}}]
    """
    _, last_day, days = _month_range(month)
    if not days or not routes:
        return {"days": days, "rows": [], "month": month, "cabin_class": cabin_class}

    start_date = f"{month}-01"
    end_date = f"{month}-{last_day:02d}"

    route_conds = " OR ".join("(origin=? AND destination=?)" for _ in routes)
    route_params = [p for pair in routes[:max_routes] for p in pair]
    params: list[Any] = [cabin_class, start_date, end_date] + route_params

    cur = conn.execute(
        f"""
        SELECT origin, destination, depart_date, MIN(miles) as miles
        FROM partner_award_calendar_fares
        WHERE source='AF' AND cabin_class=? AND depart_date >= ? AND depart_date <= ?
          AND ({route_conds}) AND miles IS NOT NULL
        GROUP BY origin, destination, depart_date
        """,
        params,
    )
    raw = cur.fetchall()

    by_route: dict[tuple[str, str], dict[int, int]] = {}
    for origin, dest, depart_date, miles in raw:
        try:
            day = int(depart_date[8:10])
        except (ValueError, IndexError):
            continue
        key = (origin, dest)
        if key not in by_route:
            by_route[key] = {}
        if day not in by_route[key] or (miles is not None and miles < (by_route[key].get(day) or 999999)):
            by_route[key][day] = miles

    rows = []
    for origin, dest in routes[:max_routes]:
        values = by_route.get((origin, dest), {})
        miles_list = [v for v in values.values() if v is not None]
        month_min = min(miles_list, default=None)

        display_values = {}
        css_classes = {}
        for d in days:
            miles = values.get(d)
            display_values[d] = _format_miles_k(miles)
            if miles is None:
                css_classes[d] = "hm-empty"
            elif month_min and miles == month_min:
                css_classes[d] = "hm-min"
            elif month_min and month_min > 0:
                ratio = miles / month_min
                if ratio <= 1.1:
                    css_classes[d] = "hm-low"
                elif ratio <= 1.3:
                    css_classes[d] = "hm-mid"
                else:
                    css_classes[d] = "hm-high"
            else:
                css_classes[d] = "hm-mid"

        rows.append({
            "origin": origin,
            "destination": dest,
            "values": values,
            "month_min": month_min,
            "display_values": display_values,
            "css_classes": css_classes,
        })

    return {
        "days": days,
        "rows": rows,
        "month": month,
        "cabin_class": cabin_class,
    }
