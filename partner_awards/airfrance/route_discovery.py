"""
Route discovery: rank destinations by best miles and green days.
DB-only, no live fetch. Helps decide which routes to add to the watchlist.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any


def months_present(
    conn: sqlite3.Connection,
    cabin_class: str,
    origins: list[str] | None = None,
) -> list[str]:
    """
    Return list of YYYY-MM available in calendar_fares (newest first).
    Optionally filter by cabin_class and origins.
    """
    conditions = ["source='AF'", "miles IS NOT NULL", "cabin_class=?"]
    params: list[Any] = [cabin_class]
    if origins:
        placeholders = ", ".join("?" for _ in origins)
        conditions.append(f"origin IN ({placeholders})")
        params.extend(origins)
    cur = conn.execute(
        f"""SELECT DISTINCT substr(depart_date, 1, 7) as ym
           FROM partner_award_calendar_fares
           WHERE {" AND ".join(conditions)}
           ORDER BY ym DESC""",
        params,
    )
    return [r[0] for r in cur.fetchall() if r[0]]


def discovery_for_origin(
    conn: sqlite3.Connection,
    origin: str,
    cabin_class: str,
    months: list[str],
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Return ranked rows for a single origin.
    Each row: {origin, destination, best_miles, green_days_count, coverage_days, months_count}
    """
    result = discovery_multi_origin(conn, [origin], cabin_class, months, limit)
    return result.get(origin, [])


def discovery_multi_origin(
    conn: sqlite3.Connection,
    origins: list[str],
    cabin_class: str,
    months: list[str],
    limit_per_origin: int = 20,
) -> dict[str, list[dict[str, Any]]]:
    """
    Return dict origin -> rows, each row ranked by best_miles, green_days_count, coverage_days, destination.
    If months is empty, default to last 3 months present in DB for those origins/cabin.
    """
    if not origins:
        return {}

    if not months:
        months = months_present(conn, cabin_class, origins)[:3]

    placeholders = ", ".join("?" for _ in origins)
    like_conds = " OR ".join("depart_date LIKE ?" for _ in months)
    params: list[Any] = [cabin_class] + list(origins) + [f"{m}%" for m in months]
    cur = conn.execute(
        f"""
        SELECT origin, destination, depart_date, MIN(miles) as miles
        FROM partner_award_calendar_fares
        WHERE source='AF' AND cabin_class=? AND miles IS NOT NULL
          AND origin IN ({placeholders})
          AND ({like_conds})
        GROUP BY origin, destination, depart_date
        """,
        params,
    )
    rows = cur.fetchall()

    route_data: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for origin, dest, depart_date, miles in rows:
        route_data[(origin, dest)].append((depart_date, miles))

    results: dict[str, list[dict[str, Any]]] = {o: [] for o in origins}
    for (origin, dest), date_miles in route_data.items():
        best_miles = min(m for _, m in date_miles if m is not None)
        coverage_days = len(date_miles)

        by_month: dict[str, list[int]] = defaultdict(list)
        for d, m in date_miles:
            ym = d[:7]
            if ym in months:
                by_month[ym].append(m)

        green_days_count = 0
        months_with_data = 0
        for miles_list in by_month.values():
            if not miles_list:
                continue
            months_with_data += 1
            month_min = min(miles_list)
            green_days_count += sum(1 for m in miles_list if m == month_min)

        results[origin].append({
            "origin": origin,
            "destination": dest,
            "best_miles": best_miles,
            "green_days_count": green_days_count,
            "coverage_days": coverage_days,
            "months_count": months_with_data,
        })

    for origin in origins:
        results[origin].sort(
            key=lambda r: (
                r["best_miles"] or 999999,
                -r["green_days_count"],
                -r["coverage_days"],
                r["destination"],
            )
        )
        results[origin] = results[origin][:limit_per_origin]

    return results


# Legacy aliases for backward compatibility
def compute_months_present(conn: sqlite3.Connection) -> list[str]:
    """Return all months (no cabin/origin filter). Use months_present() for filtered."""
    return months_present(conn, "BUSINESS", None)


def compute_route_discovery(
    conn: sqlite3.Connection,
    origins: list[str],
    cabin: str,
    months: list[str],
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Legacy: flat list. Use discovery_multi_origin for dict."""
    by_origin = discovery_multi_origin(conn, origins, cabin, months, limit)
    out = []
    for o in origins:
        out.extend(by_origin.get(o, []))
    return out
