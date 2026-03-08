"""
Shared database query helpers for the SAS Awards dashboard and reports.
All SQL lives here — app.py only orchestrates routes and templates.
"""
import os
import sqlite3

from report_config import MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX
import regions as _regions

DB_PATH = os.path.expanduser(
    os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite")
)


def get_conn():
    return sqlite3.connect(DB_PATH)


# ═══════════════════════════════════════════════════════════════════════════
# Dashboard
# ═══════════════════════════════════════════════════════════════════════════

def dashboard_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT origin, COUNT(*) FROM flights GROUP BY origin")
    counts = dict(cur.fetchall())
    total = sum(counts.values())
    cur.execute(
        "SELECT COUNT(*) FROM sqlite_master "
        "WHERE type='table' AND name='flight_history'"
    )
    has_history = cur.fetchone()[0] > 0
    conn.close()
    return {"counts": counts, "total": total, "has_history": has_history}


def region_counts():
    """Live destination/seat counts for every region."""
    out = []
    conn = get_conn()
    cur = conn.cursor()
    for key in _regions.all_region_keys():
        r = _regions.REGIONS[key]
        countries = r["countries"]
        ph = ",".join("?" * len(countries))
        cur.execute(f"""
            SELECT COUNT(DISTINCT airport_code),
                   SUM(CASE WHEN ab >= 2 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN ap >= 2 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN ag >= 2 THEN 1 ELSE 0 END)
            FROM flights
            WHERE country_name IN ({ph}) AND date >= date('now')
        """, countries)
        row = cur.fetchone()
        out.append({
            "key": key, "label": r["label"], "icon": r["icon"],
            "destinations": row[0] or 0,
            "biz": row[1] or 0, "plus": row[2] or 0, "eco": row[3] or 0,
        })
    conn.close()
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Unified flight query (replaces /all, /business, /plus, /flow)
# ═══════════════════════════════════════════════════════════════════════════

def query_flights(
    countries=None, cabin="all", origin="", min_seats=MIN_SEATS,
    from_date="", to_date="", city="", page=1, per_page=50,
):
    """
    Filtered, ranked, paginated flight query.
    Returns {"rows": [dict, ...], "total": int, "page": int, "per_page": int}.
    """
    conditions = ["date >= date('now')"]
    params = []

    if countries:
        ph = ",".join("?" * len(countries))
        conditions.append(f"country_name IN ({ph})")
        params.extend(countries)

    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if city:
        conditions.append("(city_name LIKE ? OR airport_code LIKE ?)")
        params.extend([f"%{city}%", f"%{city}%"])
    if from_date:
        conditions.append("date >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("date <= ?")
        params.append(to_date)

    seat_cond, order_expr = _cabin_clause(cabin, min_seats)
    if seat_cond:
        conditions.append(seat_cond)

    where = " AND ".join(conditions)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM flights WHERE {where}", params)
    total = cur.fetchone()[0]

    offset = (page - 1) * per_page
    cur.execute(f"""
        SELECT origin, city_name, airport_code, country_name,
               direction, date, ag, ap, ab
        FROM flights WHERE {where}
        ORDER BY {order_expr}
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    cols = ["origin", "city_name", "airport_code", "country_name",
            "direction", "date", "ag", "ap", "ab"]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return {"rows": rows, "total": total, "page": page, "per_page": per_page}


# ═══════════════════════════════════════════════════════════════════════════
# Weekend pairs (outbound+inbound round-trip combos)
# ═══════════════════════════════════════════════════════════════════════════

def query_weekend_pairs(
    countries=None, cabin="all", origin="", min_seats=MIN_SEATS,
    city="", page=1, per_page=50,
):
    """
    Weekend round-trip pairs: outbound Wed/Thu/Fri, inbound Sat/Sun/Mon, 3-4 days.
    Cabin filter:
      - "business"      → both legs must have ab >= min_seats
      - "business_plus"  → both legs must have (ab >= min_seats OR ap >= min_seats)
      - "all"            → both legs must have (ag >= min_seats OR ap >= min_seats OR ab >= min_seats)
    Returns {"rows": [...], "total": int, "page": int, "per_page": int}.
    """
    seat_out, seat_in = _weekend_cabin_clause(cabin, min_seats)

    conditions = [
        "inb.direction = 'inbound'",
        "outb.direction = 'outbound'",
        seat_out,
        seat_in,
        "strftime('%w', inb.date) IN ('6','0','1')",
        "strftime('%w', outb.date) IN ('3','4','5')",
        f"(julianday(inb.date) - julianday(outb.date)) BETWEEN {TRIP_DAYS_MIN} AND {TRIP_DAYS_MAX}",
        "date(inb.date) BETWEEN date('now') AND date('now','+1 year')",
    ]
    params = []

    if countries:
        ph = ",".join("?" * len(countries))
        conditions.append(f"inb.country_name IN ({ph})")
        params.extend(countries)
    if origin:
        conditions.append("inb.origin = ?")
        params.append(origin)
    if city:
        conditions.append(
            "(inb.city_name LIKE ? OR inb.airport_code LIKE ?)"
        )
        params.extend([f"%{city}%", f"%{city}%"])

    where = " AND ".join(conditions)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT COUNT(*) FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE {where}
    """, params)
    total = cur.fetchone()[0]

    order = _weekend_order(cabin)
    offset = (page - 1) * per_page
    cur.execute(f"""
        SELECT inb.origin, inb.city_name, inb.airport_code, inb.country_name,
               outb.date AS outbound, inb.date AS inbound,
               outb.ag AS ag_out, outb.ap AS ap_out, outb.ab AS ab_out,
               inb.ag AS ag_in, inb.ap AS ap_in, inb.ab AS ab_in
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE {where}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """, params + [per_page, offset])

    rows = []
    for r in cur.fetchall():
        rows.append({
            "origin": r[0], "city_name": r[1], "airport_code": r[2],
            "country_name": r[3], "outbound": r[4], "inbound": r[5],
            "ag_out": r[6], "ap_out": r[7], "ab_out": r[8],
            "ag_in": r[9], "ap_in": r[10], "ab_in": r[11],
        })
    conn.close()
    return {"rows": rows, "total": total, "page": page, "per_page": per_page}


def _weekend_cabin_clause(cabin, min_seats):
    """Return (outbound_condition, inbound_condition) for weekend pair cabin filter."""
    if cabin == "business":
        return (f"outb.ab >= {min_seats}", f"inb.ab >= {min_seats}")
    if cabin == "business_plus":
        return (
            f"(outb.ab >= {min_seats} OR outb.ap >= {min_seats})",
            f"(inb.ab >= {min_seats} OR inb.ap >= {min_seats})",
        )
    # "all" — any cabin
    return (
        f"(outb.ag >= {min_seats} OR outb.ap >= {min_seats} OR outb.ab >= {min_seats})",
        f"(inb.ag >= {min_seats} OR inb.ap >= {min_seats} OR inb.ab >= {min_seats})",
    )


def _weekend_order(cabin):
    """Primary: outbound date (chronological). Tiebreak: inbound, then cabin score, then city."""
    base = "outb.date ASC, inb.date ASC"
    if cabin == "business":
        return f"{base}, outb.ab + inb.ab DESC, inb.city_name COLLATE NOCASE"
    if cabin == "business_plus":
        return f"{base}, (outb.ab + outb.ap + inb.ab + inb.ap) DESC, inb.city_name COLLATE NOCASE"
    return f"{base}, (outb.ab*3+outb.ap*2+outb.ag + inb.ab*3+inb.ap*2+inb.ag) DESC, inb.city_name COLLATE NOCASE"


def _cabin_clause(cabin, min_seats):
    if cabin == "business":
        return (f"ab >= {min_seats}",
                "ab DESC, date, origin, city_name COLLATE NOCASE")
    if cabin == "plus":
        return (f"(ap >= {min_seats} OR ab >= {min_seats})",
                "ap + ab DESC, date, origin, city_name COLLATE NOCASE")
    if cabin == "economy":
        return (f"ag >= {min_seats}",
                "ag DESC, date, origin, city_name COLLATE NOCASE")
    return (f"(ag >= {min_seats} OR ap >= {min_seats} OR ab >= {min_seats})",
            "(ab*3 + ap*2 + ag) DESC, date, origin, city_name COLLATE NOCASE")


# ═══════════════════════════════════════════════════════════════════════════
# Route detail (generalized — works for any origin+dest+date)
# ═══════════════════════════════════════════════════════════════════════════

def route_detail(origin, dest, date):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT origin, airport_code, city_name, country_name,
               direction, date, ag, ap, ab
        FROM flights
        WHERE origin = ? AND airport_code = ? AND date = ?
    """, (origin, dest, date))
    cols = ["origin", "airport_code", "city_name", "country_name",
            "direction", "date", "ag", "ap", "ab"]
    result = {}
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        result[d["direction"]] = d
    conn.close()
    return result or None


# ═══════════════════════════════════════════════════════════════════════════
# Reports
# ═══════════════════════════════════════════════════════════════════════════

def report_region(cabin="all", origin="", country="", min_seats=MIN_SEATS):
    """Countries aggregated with seat counts, grouped into regions."""
    conn = get_conn()
    cur = conn.cursor()
    seat_cond, _ = _cabin_clause(cabin, min_seats)
    origin_cond = f"AND origin = '{origin}'" if origin else ""
    country_cond = "AND country_name = ?" if country else ""
    params = [country] if country else []

    cur.execute(f"""
        SELECT country_name,
               COUNT(DISTINCT airport_code) AS dests,
               COUNT(*) AS flights,
               SUM(CASE WHEN ab >= {min_seats} THEN 1 ELSE 0 END) AS biz,
               SUM(CASE WHEN ap >= {min_seats} THEN 1 ELSE 0 END) AS plus,
               SUM(CASE WHEN ag >= {min_seats} THEN 1 ELSE 0 END) AS eco
        FROM flights
        WHERE date >= date('now') AND {seat_cond} {origin_cond} {country_cond}
        GROUP BY country_name
        ORDER BY flights DESC
    """, params if params else [])
    rows = cur.fetchall()
    conn.close()

    table = []
    for country, dests, flights, biz, plus_, eco in rows:
        region_key = _regions.country_to_region(country) or "other"
        region_label = _regions.REGIONS.get(region_key, {}).get("label", "Other")
        table.append({
            "country": country, "region": region_label,
            "dests": dests, "flights": flights,
            "biz": biz, "plus": plus_, "eco": eco,
        })

    region_agg = {}
    for r in table:
        rg = r["region"]
        if rg not in region_agg:
            region_agg[rg] = {"flights": 0, "biz": 0, "plus": 0, "eco": 0}
        region_agg[rg]["flights"] += r["flights"]
        region_agg[rg]["biz"] += r["biz"]
        region_agg[rg]["plus"] += r["plus"]
        region_agg[rg]["eco"] += r["eco"]

    chart = {
        "labels": list(region_agg.keys()),
        "datasets": [
            {"label": "Business", "data": [v["biz"] for v in region_agg.values()]},
            {"label": "Plus", "data": [v["plus"] for v in region_agg.values()]},
            {"label": "Economy", "data": [v["eco"] for v in region_agg.values()]},
        ],
    }
    return {"chart": chart, "table": table}


def report_cities(countries=None, cabin="all", origin="", min_seats=MIN_SEATS):
    """Destinations ranked by total seat availability."""
    conn = get_conn()
    cur = conn.cursor()
    seat_cond, _ = _cabin_clause(cabin, min_seats)
    conditions = [f"date >= date('now')", seat_cond]
    params = []
    if countries:
        ph = ",".join("?" * len(countries))
        conditions.append(f"country_name IN ({ph})")
        params.extend(countries)
    if origin:
        conditions.append("origin = ?")
        params.append(origin)

    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT city_name, airport_code, country_name,
               COUNT(*) AS flights,
               SUM(ab) AS total_biz, SUM(ap) AS total_plus, SUM(ag) AS total_eco
        FROM flights WHERE {where}
        GROUP BY city_name, airport_code, country_name
        ORDER BY (SUM(ab)*3 + SUM(ap)*2 + SUM(ag)) DESC
        LIMIT 50
    """, params)
    cols = ["city", "code", "country", "flights", "biz", "plus", "eco"]
    table = [dict(zip(cols, r)) for r in cur.fetchall()]

    cur.execute(f"""
        SELECT country_name,
               SUM(ab) AS total_biz, SUM(ap) AS total_plus, SUM(ag) AS total_eco
        FROM flights WHERE {where}
        GROUP BY country_name
        ORDER BY (SUM(ab)*3 + SUM(ap)*2 + SUM(ag)) DESC
        LIMIT 25
    """, params)
    country_rows = cur.fetchall()
    conn.close()

    chart = {
        "labels": [r[0] for r in country_rows],
        "datasets": [{
            "label": "Total weighted seats",
            "data": [r[1] * 3 + r[2] * 2 + r[3] for r in country_rows],
        }],
    }
    return {"chart": chart, "table": table}


def report_business(origin="", country="", min_seats=MIN_SEATS):
    """Business seats aggregated by date, split by origin."""
    conn = get_conn()
    cur = conn.cursor()
    origin_cond = "AND origin = ?" if origin else ""
    country_cond = "AND country_name = ?" if country else ""
    params = [min_seats] + ([origin] if origin else []) + ([country] if country else [])

    cur.execute(f"""
        SELECT date, origin, SUM(ab) AS total_biz, COUNT(*) AS routes
        FROM flights
        WHERE ab >= ? AND date >= date('now') {origin_cond} {country_cond}
        GROUP BY date, origin ORDER BY date, origin
    """, params)
    rows = cur.fetchall()

    dates = sorted(set(r[0] for r in rows))
    origins = sorted(set(r[1] for r in rows))
    by_origin = {}
    for d, o, total, _ in rows:
        by_origin.setdefault(o, {})[d] = total

    chart = {
        "labels": dates,
        "datasets": [
            {"label": o, "data": [by_origin.get(o, {}).get(d, 0) for d in dates]}
            for o in origins
        ],
    }

    cur.execute(f"""
        SELECT origin, city_name, airport_code, date, ab
        FROM flights
        WHERE ab >= ? AND date >= date('now') {origin_cond} {country_cond}
        ORDER BY date, origin, city_name COLLATE NOCASE LIMIT 200
    """, params)
    table = [
        {"origin": r[0], "city": r[1], "code": r[2], "date": r[3], "ab": r[4]}
        for r in cur.fetchall()
    ]
    conn.close()
    return {"chart": chart, "table": table}


def countries_with_weekend_pairs(origin="", cabin="all", min_seats=MIN_SEATS):
    """Country names that have at least one weekend pair for the given cabin/origin (for dropdown)."""
    seat_out, seat_in = _weekend_cabin_clause(cabin, min_seats)
    conn = get_conn()
    cur = conn.cursor()
    origin_cond = "AND inb.origin = ?" if origin else ""
    params = [TRIP_DAYS_MIN, TRIP_DAYS_MAX]
    if origin:
        params.append(origin)
    cur.execute(f"""
        SELECT DISTINCT inb.country_name
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND {seat_out} AND {seat_in}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
          {origin_cond}
        ORDER BY inb.country_name
    """, params)
    out = [r[0] for r in cur.fetchall()]
    conn.close()
    return out


def report_weekend(origin="", country="", min_seats=MIN_SEATS, cabin="all"):
    """Weekend pairs aggregated by city. cabin: all, business_plus, business."""
    seat_out, seat_in = _weekend_cabin_clause(cabin, min_seats)
    conn = get_conn()
    cur = conn.cursor()
    origin_cond = "AND inb.origin = ?" if origin else ""
    country_cond = "AND inb.country_name = ?" if country else ""
    params = [TRIP_DAYS_MIN, TRIP_DAYS_MAX]
    if origin:
        params.append(origin)
    if country:
        params.append(country)

    cur.execute(f"""
        SELECT inb.origin, inb.city_name, inb.airport_code, inb.country_name,
               COUNT(*) AS pairs,
               MIN(outb.date) AS earliest, MAX(inb.date) AS latest
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND {seat_out} AND {seat_in}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
          {origin_cond} {country_cond}
        GROUP BY inb.origin, inb.city_name, inb.airport_code, inb.country_name
        ORDER BY pairs DESC
    """, params)
    table = [
        {"origin": r[0], "city": r[1], "code": r[2], "country": r[3],
         "pairs": r[4], "earliest": r[5], "latest": r[6]}
        for r in cur.fetchall()
    ]

    summary = {}
    for r in table:
        summary.setdefault(r["origin"], {"cities": 0, "pairs": 0})
        summary[r["origin"]]["cities"] += 1
        summary[r["origin"]]["pairs"] += r["pairs"]

    by_country = {}
    for r in table:
        c = r["country"]
        by_country[c] = by_country.get(c, 0) + r["pairs"]
    country_sorted = sorted(by_country.items(), key=lambda x: -x[1])[:25]
    chart = {
        "labels": [c for c, _ in country_sorted],
        "datasets": [{"label": "Pairs", "data": [n for _, n in country_sorted]}],
    }
    conn.close()
    return {"chart": chart, "table": table, "summary": summary}


def report_new():
    """New business flights since yesterday (from flight_history)."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='flight_history'"
    )
    if not cur.fetchone():
        conn.close()
        return None

    cur.execute(
        "SELECT DISTINCT fetch_date FROM flight_history "
        "ORDER BY fetch_date DESC LIMIT 2"
    )
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        conn.close()
        return None

    latest, prev = dates[0], dates[1]
    cur.execute("""
        WITH t AS (
            SELECT origin, airport_code, city_name, date, direction, ab
            FROM flight_history
            WHERE fetch_date = ? AND ab >= ?
        ),
        p AS (
            SELECT origin, airport_code, date, direction
            FROM flight_history
            WHERE fetch_date = ? AND ab >= ?
        )
        SELECT t.origin, t.city_name, t.airport_code, t.date, t.direction, t.ab
        FROM t LEFT JOIN p
          ON p.origin = t.origin AND p.airport_code = t.airport_code
             AND p.date = t.date AND p.direction = t.direction
        WHERE p.airport_code IS NULL
        ORDER BY t.ab DESC, t.date LIMIT 200
    """, (latest, MIN_SEATS, prev, MIN_SEATS))
    table = [
        {"origin": r[0], "city": r[1], "code": r[2],
         "date": r[3], "direction": r[4], "ab": r[5]}
        for r in cur.fetchall()
    ]

    city_counts = {}
    for r in table:
        k = f"{r['city']} ({r['code']})"
        city_counts[k] = city_counts.get(k, 0) + r["ab"]
    sorted_cities = sorted(city_counts.items(), key=lambda x: -x[1])[:25]

    chart = {
        "labels": [c[0] for c in sorted_cities],
        "datasets": [{"label": "New Biz Seats", "data": [c[1] for c in sorted_cities]}],
    }
    conn.close()
    return {"chart": chart, "table": table, "latest": latest, "prev": prev}


# ═══════════════════════════════════════════════════════════════════════════
# Reports drill-down and calendar
# ═══════════════════════════════════════════════════════════════════════════

def cities_for_country(country, cabin="all", origin="", min_seats=MIN_SEATS):
    """
    Cities in a single country for region-tab drill-down.
    Always includes origin in each row (one row per origin when origin filter is empty).
    """
    conn = get_conn()
    cur = conn.cursor()
    seat_cond, _ = _cabin_clause(cabin, min_seats)
    conditions = [
        "date >= date('now')",
        seat_cond,
        "country_name = ?",
    ]
    params = [country]
    if origin:
        conditions.append("origin = ?")
        params.append(origin)

    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT origin, city_name, airport_code, country_name,
               COUNT(*) AS flights,
               SUM(ab) AS total_biz, SUM(ap) AS total_plus, SUM(ag) AS total_eco
        FROM flights WHERE {where}
        GROUP BY origin, city_name, airport_code, country_name
        ORDER BY (SUM(ab)*3 + SUM(ap)*2 + SUM(ag)) DESC
        LIMIT 100
    """, params)
    cols = ["origin", "city", "code", "country", "flights", "biz", "plus", "eco"]
    table = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return {"table": table}


def calendar_availability(origin, airport_code, min_seats=MIN_SEATS):
    """
    Daily availability for origin → airport_code from today to +365 days.
    Returns dict: date_str -> {"outbound": {"ab", "ap", "ag"}, "inbound": {...}}.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT date, direction, SUM(ab) AS ab, SUM(ap) AS ap, SUM(ag) AS ag
        FROM flights
        WHERE origin = ? AND airport_code = ?
          AND date >= date('now')
          AND date <= date('now', '+365 days')
        GROUP BY date, direction
    """, (origin, airport_code))
    by_date = {}
    for date_str, direction, ab, ap, ag in cur.fetchall():
        by_date.setdefault(date_str, {})[direction] = {
            "ab": ab or 0, "ap": ap or 0, "ag": ag or 0,
        }
    conn.close()
    return by_date


def weekend_pairs_for_route(origin, airport_code, min_seats=MIN_SEATS, cabin="all"):
    """
    Weekend pairs for a single route (origin → airport_code): outbound Wed/Thu/Fri,
    return Sat/Sun/Mon, 3–4 days. cabin: all, business_plus, business.
    """
    seat_out, seat_in = _weekend_cabin_clause(cabin, min_seats)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT outb.date AS outbound, inb.date AS inbound,
               outb.ab AS ab_out, outb.ap AS ap_out, outb.ag AS ag_out,
               inb.ab AS ab_in, inb.ap AS ap_in, inb.ag AS ag_in
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.origin = ? AND inb.airport_code = ?
          AND inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND {seat_out} AND {seat_in}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+365 days')
        ORDER BY outb.date, inb.date
    """, (origin, airport_code, TRIP_DAYS_MIN, TRIP_DAYS_MAX))
    rows = [
        {
            "outbound": r[0], "inbound": r[1],
            "ab_out": r[2], "ap_out": r[3], "ag_out": r[4],
            "ab_in": r[5], "ap_in": r[6], "ag_in": r[7],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows
