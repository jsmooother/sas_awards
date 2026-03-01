"""
Heatmap: route × day-of-month grid for selected month+cabin.
DB-only, uses partner_award_calendar_fares with best_overall (min miles across hosts).
"""

from __future__ import annotations

import calendar
import datetime as _dt
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


def build_year_grid(
    conn: sqlite3.Connection,
    start_month: str,
    cabin_class: str,
    routes: list[tuple[str, str]],
) -> dict[str, Any]:
    """
    Build 12-month grid for routes (365-day view). One row per route, 12 month columns.
    Each cell shows min miles for that month (or —).
    start_month: YYYY-MM for first month (e.g. 2026-04).
    """
    try:
        y, m = int(start_month[:4]), int(start_month[5:7])
    except (ValueError, IndexError):
        return {"months": [], "rows": [], "cabin_class": cabin_class}

    months: list[str] = []
    for _ in range(12):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    if not routes:
        return {"months": months, "rows": [], "cabin_class": cabin_class}

    route_conds = " OR ".join("(origin=? AND destination=?)" for _ in routes)
    route_params = [p for pair in routes for p in pair]

    cur = conn.execute(
        f"""
        SELECT origin, destination, substr(depart_date, 1, 7) as ym, MIN(miles) as miles
        FROM partner_award_calendar_fares
        WHERE source='AF' AND cabin_class=? AND miles IS NOT NULL
          AND ({route_conds})
        GROUP BY origin, destination, substr(depart_date, 1, 7)
        """,
        [cabin_class] + route_params,
    )
    raw = cur.fetchall()

    by_route: dict[tuple[str, str], dict[str, int]] = {}
    for origin, dest, ym, miles in raw:
        key = (origin, dest)
        if key not in by_route:
            by_route[key] = {}
        if ym not in by_route[key] or (miles is not None and miles < (by_route[key].get(ym) or 999999)):
            by_route[key][ym] = miles

    rows = []
    for origin, dest in routes:
        values = by_route.get((origin, dest), {})
        month_mins = {}
        display_values = {}
        for ym in months:
            m = values.get(ym)
            month_mins[ym] = m
            display_values[ym] = _format_miles_k(m)
        rows.append({
            "origin": origin,
            "destination": dest,
            "values": month_mins,
            "display_values": display_values,
        })
    return {"months": months, "rows": rows, "cabin_class": cabin_class}


def build_daily_year_calendar(
    conn: sqlite3.Connection,
    cabin_class: str,
    origin: str,
    destination: str,
    days: int = 365,
) -> list[dict[str, Any]]:
    """
    Build day-by-day 365 calendar for a single route (Plus Europe style).
    Starts from today and covers the next 365 days.
    Returns list of months, each with label and days.
    Each day: iso, day, miles, display (e.g. 85k), has_data, is_today.
    """
    cur = conn.execute(
        """
        SELECT depart_date, MIN(miles) as miles
        FROM partner_award_calendar_fares
        WHERE source='AF' AND cabin_class=? AND origin=? AND destination=? AND miles IS NOT NULL
        GROUP BY depart_date
        """,
        (cabin_class, origin, destination),
    )
    by_date: dict[str, int] = {r[0]: r[1] for r in cur.fetchall()}
    route_min = min(by_date.values(), default=None)

    start = _dt.date.today()
    months: dict[str, dict[str, Any]] = {}
    today = start

    for i in range(days):
        d = start + _dt.timedelta(days=i)
        iso = d.isoformat()
        key = d.strftime("%Y-%m")
        if key not in months:
            months[key] = {"label": d.strftime("%b %Y"), "days": []}
        miles = by_date.get(iso)
        display = _format_miles_k(miles)
        if miles is None:
            css_class = "hm-empty"
        elif route_min and miles == route_min:
            css_class = "hm-min"
        elif route_min and route_min > 0:
            ratio = miles / route_min
            if ratio <= 1.2:
                css_class = "hm-low"
            elif ratio <= 2.5:
                css_class = "hm-mid"
            else:
                css_class = "hm-high"
        else:
            css_class = "hm-mid"
        day_info = {
            "iso": iso,
            "day": d.strftime("%d"),
            "miles": miles,
            "display": display,
            "has_data": miles is not None,
            "is_today": d == today,
            "css_class": css_class,
        }
        months[key]["days"].append(day_info)

    # Build calendar grid: weekday headers (Mon=0) and weeks (rows of 7 cells)
    weekday_headers = ["M", "T", "W", "T", "F", "S", "S"]
    result = []
    for key in sorted(months.keys()):
        month_data = months[key]
        days_list = month_data["days"]
        if not days_list:
            result.append({
                "label": month_data["label"],
                "weekday_headers": weekday_headers,
                "weeks": [],
            })
            continue
        # First day in list may be mid-month; its weekday (0=Mon..6=Sun) = leading blanks
        first_date = _dt.date.fromisoformat(days_list[0]["iso"])
        leading = first_date.weekday()
        cells = [None] * leading + days_list
        while len(cells) % 7 != 0:
            cells.append(None)
        weeks = [cells[i : i + 7] for i in range(0, len(cells), 7)]
        result.append({
            "label": month_data["label"],
            "weekday_headers": weekday_headers,
            "weeks": weeks,
        })
    return result
