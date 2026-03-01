"""
Month delta: compare latest vs previous scan for a route/month/cabin.
Uses raw_responses (historical) since calendar_fares are upserted (overwritten).
Produces Telegram-ready text for daily report.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .adapter import _parse_lowest_fare_entries


def _month_range(month: str) -> tuple[str, str]:
    start = f"{month}-01"
    try:
        y, m = int(month[:4]), int(month[5:7])
        import calendar
        last_d = calendar.monthrange(y, m)[1]
        end = f"{month}-{last_d:02d}"
    except (ValueError, IndexError):
        end = f"{month}-31"
    return start, end


def get_scan_runs_for_month(conn, origin: str, destination: str, cabin: str, month: str) -> list[dict]:
    """
    Returns scan runs that have LowestFareOffers raw response for this origin/destination.
    Ordered newest-first. Only includes runs whose parsed body has data for the month.
    """
    cur = conn.execute(
        """SELECT s.id, s.started_at, s.host_used, s.ingest_type, r.body
           FROM partner_award_scan_runs s
           INNER JOIN partner_award_raw_responses r ON r.scan_run_id = s.id
           WHERE r.operation_name = 'SharedSearchLowestFareOffersForSearchQuery'
             AND r.origin = ? AND r.destination = ?
             AND s.source = 'AF'
           ORDER BY s.started_at DESC, s.id DESC
           LIMIT 20""",
        (origin, destination),
    )
    start, end = _month_range(month)
    result = []
    for row in cur.fetchall():
        scan_run_id, created_at, host_used, ingest_type, body_json = row
        try:
            body = json.loads(body_json) if isinstance(body_json, str) else body_json
        except Exception:
            continue
        payload = body.get("body", body) if isinstance(body, dict) and "body" in body else body
        cabins = [cabin]
        entries = _parse_lowest_fare_entries(payload, cabins)
        month_entries = [(d, cab, mi, tx) for d, cab, mi, tx in entries if start <= d <= end and cab == cabin]
        if month_entries:
            result.append({
                "scan_run_id": scan_run_id,
                "created_at": created_at,
                "host_used": host_used or "",
                "ingest_type": ingest_type or "",
            })
    return result


def get_month_fares_by_scan_run(conn, scan_run_id: int, origin: str, destination: str, cabin: str, month: str) -> dict[str, dict]:
    """
    Returns dict[date_str] -> {miles, tax, host_used} from raw_response body.
    """
    cur = conn.execute(
        """SELECT body FROM partner_award_raw_responses
           WHERE scan_run_id = ? AND operation_name = 'SharedSearchLowestFareOffersForSearchQuery'
           ORDER BY id DESC LIMIT 1""",
        (scan_run_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    try:
        body = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    except Exception:
        return {}
    payload = body.get("body", body) if isinstance(body, dict) and "body" in body else body
    host_cur = conn.execute("SELECT host_used FROM partner_award_scan_runs WHERE id = ?", (scan_run_id,))
    host_row = host_cur.fetchone()
    host_used = (host_row[0] or "") if host_row else ""
    entries = _parse_lowest_fare_entries(payload, [cabin])
    start, end = _month_range(month)
    return {
        d: {"miles": mi, "tax": tx, "host_used": host_used}
        for d, cab, mi, tx in entries
        if start <= d <= end and cab == cabin
    }


def compute_month_delta(latest_map: dict, prev_map: dict) -> dict[str, Any]:
    """
    Compare latest vs prev. Returns delta result dict.
    """
    changed = []
    for d, v in latest_map.items():
        old = prev_map.get(d)
        if old is None:
            continue
        om = old.get("miles")
        nm = v.get("miles")
        if om is not None and nm is not None and om != nm:
            delta = nm - om
            changed.append({"date": d, "old_miles": om, "new_miles": nm, "delta_miles": delta})

    new_dates = sorted(k for k in latest_map if k not in prev_map)
    removed_dates = sorted(k for k in prev_map if k not in latest_map)

    miles_list = [v.get("miles") for v in latest_map.values() if v.get("miles") is not None]
    min_miles = min(miles_list, default=None)
    min_dates = sorted(d for d, v in latest_map.items() if v.get("miles") == min_miles)
    count_days = len(miles_list)

    # Top 6 highest miles days
    by_miles = [(d, v.get("miles")) for d, v in latest_map.items() if v.get("miles") is not None]
    by_miles.sort(key=lambda x: (-(x[1] or 0), x[0]))
    expensive_days = [(d, m) for d, m in by_miles[:6]]

    changed.sort(key=lambda x: x["delta_miles"])
    biggest_drops = [c for c in changed if c["delta_miles"] < 0][:5]
    biggest_increases = [c for c in changed if c["delta_miles"] > 0][-5:]
    biggest_increases.reverse()

    return {
        "changed_dates": changed,
        "new_dates": new_dates,
        "removed_dates": removed_dates,
        "min_miles_latest": min_miles,
        "min_dates_latest": min_dates,
        "count_days_with_data_latest": count_days,
        "biggest_drops": biggest_drops,
        "biggest_increases": biggest_increases,
        "expensive_days": expensive_days,
    }


def _compress_day_ranges(days: list[int]) -> str:
    """Compress sorted day numbers into ranges: 3–6, 9–13, 16–19, 24–25, 30–31"""
    if not days:
        return ""
    days = sorted(set(days))
    ranges = []
    start = days[0]
    prev = days[0]
    for d in days[1:]:
        if d == prev + 1:
            prev = d
        else:
            ranges.append(f"{start}" if start == prev else f"{start}–{prev}")
            start = prev = d
    ranges.append(f"{start}" if start == prev else f"{start}–{prev}")
    return ", ".join(ranges)


def build_telegram_month_text(
    origin: str,
    destination: str,
    month: str,
    cabin: str,
    delta_result: dict,
    scan_meta_latest: dict,
    prev_missing: bool = False,
) -> str:
    """
    Format compact Telegram-ready message.
    """
    parts = []
    try:
        y, m = int(month[:4]), int(month[5:7])
        month_name = datetime(y, m, 1).strftime("%B %Y")
    except (ValueError, IndexError):
        month_name = month

    parts.append(f"Flying Blue — {origin}→{destination} — {cabin} — {month_name}")

    min_miles = delta_result.get("min_miles_latest")
    min_dates = delta_result.get("min_dates_latest", [])
    count_days = delta_result.get("count_days_with_data_latest", 0)

    if min_miles is not None and min_dates:
        day_nums = [int(d[8:10]) for d in min_dates]
        daylist = _compress_day_ranges(day_nums)
        parts.append(f"🟩 Cheapest: {min_miles:,} miles on {len(min_dates)} days ({daylist})")
    elif count_days > 0:
        parts.append(f"🟩 {count_days} days with data")

    if not prev_missing:
        drops = delta_result.get("biggest_drops", [])
        if drops:
            drop_strs = [f"{int(c['date'][8:10])}: {c['old_miles']:,}→{c['new_miles']:,}" for c in drops]
            parts.append(f"🔻 Drops: {', '.join(drop_strs)}")

        increases = delta_result.get("biggest_increases", [])
        if increases:
            inc_strs = [f"{int(c['date'][8:10])}: {c['old_miles']:,}→{c['new_miles']:,}" for c in increases]
            parts.append(f"🔺 Increases: {', '.join(inc_strs)}")

        expensive = delta_result.get("expensive_days", [])
        if expensive and len(expensive) >= 2:
            exp_strs = [f"{int(d[8:10])}: {m:,}" for d, m in expensive if d and m is not None]
            if exp_strs:
                parts.append(f"⬆️ Expensive: {', '.join(exp_strs)}")
    else:
        parts.append("(First scan for this month)")

    host = scan_meta_latest.get("host_used") or "unknown"
    parts.append(f"Source: {host}")

    return "\n".join(parts)
