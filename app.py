#!/usr/bin/env python3
"""
SAS Awards web dashboard. Run: flask run --host=0.0.0.0 --port=5000
Access from LAN: http://<macmini-ip>:5000
"""
import os
import datetime as _dt
import requests
from flask import Flask, render_template, request, jsonify
import sqlite3

ROUTES_API = "https://www.sas.se/bff/award-finder/routes/v1"

app = Flask(__name__)
DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))

from report_config import MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX

from partner_awards.airfrance.routes import bp as partner_awards_airfrance_bp

app.register_blueprint(partner_awards_airfrance_bp)

EUROPE_COUNTRIES = (
    "Österrike", "Belgien", "Danmark", "Frankrike", "Tyskland",
    "Irland", "Italien", "Nederländerna", "Norge",
    "Portugal", "Spanien", "Sverige", "Schweiz", "Storbritannien"
)


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_destination_options(origin="", direction="", min_seats=None, seat_column="", countries=None):
    """Return destination options as (city_name, airport_code) with optional filters."""
    if seat_column and seat_column not in {"ag", "ap", "ab"}:
        raise ValueError("Invalid seat column")

    conditions = []
    params = []

    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if direction in {"outbound", "inbound"}:
        conditions.append("direction = ?")
        params.append(direction)
    if seat_column and min_seats is not None:
        conditions.append(f"{seat_column} >= ?")
        params.append(min_seats)
    if countries:
        placeholders = ", ".join("?" for _ in countries)
        conditions.append(f"country_name IN ({placeholders})")
        params.extend(countries)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    conn = get_conn()
    cur = conn.execute(
        f"""
        SELECT city_name, airport_code
        FROM flights
        {where_clause}
        GROUP BY airport_code, city_name
        ORDER BY city_name COLLATE NOCASE
        """,
        params,
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def is_checked(value):
    return str(value).strip().lower() in {"1", "true", "on", "yes"}


def weekend_cabin_columns(include_plus=False, include_business=False):
    if include_plus or include_business:
        cols = []
        if include_plus:
            cols.append("ap")
        if include_business:
            cols.append("ab")
        return cols
    return ["ag", "ap", "ab"]


def weekend_leg_clause(alias, cabin_cols):
    return "(" + " OR ".join(f"{alias}.{col} >= ?" for col in cabin_cols) + ")"


def build_year_calendar_data(daily_counts, days=365):
    """Build month/day buckets for a compact year calendar view."""
    start = _dt.date.today()
    months = {}
    for i in range(days):
        d = start + _dt.timedelta(days=i)
        iso = d.isoformat()
        key = d.strftime("%Y-%m")
        if key not in months:
            months[key] = {
                "label": d.strftime("%b %Y"),
                "days": [],
            }
        months[key]["days"].append(
            {
                "iso": iso,
                "day": d.strftime("%d"),
                "value": int(daily_counts.get(iso, 0)),
                "is_today": i == 0,
            }
        )
    return [months[k] for k in sorted(months.keys())]


def build_dual_year_calendar_data(outbound_counts, inbound_counts, days=365, min_flights=1):
    """Build month/day buckets with separate outbound and inbound counts."""
    start = _dt.date.today()
    months = {}
    for i in range(days):
        d = start + _dt.timedelta(days=i)
        iso = d.isoformat()
        key = d.strftime("%Y-%m")
        if key not in months:
            months[key] = {
                "label": d.strftime("%b %Y"),
                "days": [],
            }
        out_raw = int(outbound_counts.get(iso, 0))
        in_raw = int(inbound_counts.get(iso, 0))
        out_val = out_raw if out_raw >= min_flights else 0
        in_val = in_raw if in_raw >= min_flights else 0
        months[key]["days"].append(
            {
                "iso": iso,
                "day": d.strftime("%d"),
                "outbound": out_val,
                "inbound": in_val,
                "has_data": (out_val > 0 or in_val > 0),
                "is_today": i == 0,
            }
        )
    return [months[k] for k in sorted(months.keys())]


def get_weekend_country_options(origin="", include_plus=False, include_business=False):
    """Return countries that currently have valid weekend pairs for selected origin."""
    cabin_cols = weekend_cabin_columns(include_plus=include_plus, include_business=include_business)
    conditions = [
        "inb.direction = 'inbound'",
        "outb.direction = 'outbound'",
        weekend_leg_clause("inb", cabin_cols),
        weekend_leg_clause("outb", cabin_cols),
        "strftime('%w', inb.date) IN ('6','0','1')",
        "strftime('%w', outb.date) IN ('3','4','5')",
        "(julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?",
        "date(inb.date) BETWEEN date('now') AND date('now','+1 year')",
    ]
    params = [MIN_SEATS] * (len(cabin_cols) * 2) + [TRIP_DAYS_MIN, TRIP_DAYS_MAX]
    if origin:
        conditions.append("inb.origin = ?")
        params.append(origin)

    where_clause = " AND ".join(conditions)
    conn = get_conn()
    cur = conn.execute(
        f"""
        SELECT DISTINCT inb.country_name
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE {where_clause}
        ORDER BY inb.country_name COLLATE NOCASE
        """,
        params,
    )
    rows = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    return rows


def apply_filters(query, params, args, include_origin=True):
    """Add WHERE clauses from request args."""
    conditions = []
    new_params = list(params)
    if include_origin and args.get("origin"):
        conditions.append("origin = ?")
        new_params.append(args["origin"])
    if args.get("city") or args.get("q"):
        q = (args.get("city") or args.get("q", "")).strip()
        if q:
            conditions.append("(city_name LIKE ? OR airport_code LIKE ?)")
            pat = f"%{q}%"
            new_params.extend([pat, pat])
    if args.get("from_date"):
        conditions.append("date >= ?")
        new_params.append(args["from_date"])
    if args.get("to_date"):
        conditions.append("date <= ?")
        new_params.append(args["to_date"])
    min_seats = int(args.get("min_seats", MIN_SEATS))
    if conditions:
        query = query.replace("{{FILTERS}}", " AND " + " AND ".join(conditions))
    else:
        query = query.replace("{{FILTERS}}", "")
    return query, new_params, min_seats


@app.route("/")
def index():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT origin, COUNT(*) FROM flights GROUP BY origin")
    counts = dict(cur.fetchall())
    total = sum(counts.values())

    cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='flight_history'")
    has_history = cur.fetchone()[0] > 0

    conn.close()
    return render_template("index.html", counts=counts, total=total, has_history=has_history)


@app.route("/partner-awards")
def partner_awards():
    """Partner Awards page (Air France / KLM)."""
    return render_template("partner_awards.html")


@app.route("/business")
def business():
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
    destination = args.get("destination", "").strip().upper()
    city = args.get("city") or args.get("q", "").strip()
    from_date = args.get("from_date", "")
    to_date = args.get("to_date", "")

    query = """
        SELECT origin, city_name, airport_code, date, direction, ab
        FROM flights
        WHERE ab >= ? {{FILTERS}}
        ORDER BY date, origin, city_name COLLATE NOCASE
        LIMIT 500
    """
    params = [min_seats]
    conditions = []
    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if destination:
        conditions.append("airport_code = ?")
        params.append(destination)
    if city:
        conditions.append("(city_name LIKE ? OR airport_code LIKE ?)")
        params.append(f"%{city}%")
        params.append(f"%{city}%")
    if from_date:
        conditions.append("date >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("date <= ?")
        params.append(to_date)
    if conditions:
        query = query.replace("{{FILTERS}}", " AND " + " AND ".join(conditions))
    else:
        query = query.replace("{{FILTERS}}", "")

    conn = get_conn()
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return render_template(
        "table.html",
        title="Business (≥{m} seats)".format(m=min_seats),
        rows=rows,
        columns=["Origin", "City", "Code", "Date", "Direction", "Business"],
        filters={
            "origin": origin,
            "destination": destination,
            "city": city,
            "from_date": from_date,
            "to_date": to_date,
            "min_seats": min_seats,
        },
        destination_options=get_destination_options(origin),
    )


@app.route("/all")
def all_flights():
    """European flights – all cabins (Economy, Plus, Business). Mostly Economy."""
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
    destination = args.get("destination", "").strip().upper()
    city = args.get("city") or args.get("q", "").strip()
    from_date = args.get("from_date", "")
    to_date = args.get("to_date", "")

    placeholders = ", ".join("?" * len(EUROPE_COUNTRIES))
    query = f"""
        SELECT origin, city_name, airport_code, date, direction, ag, ap, ab
        FROM flights
        WHERE (ag >= ? OR ap >= ? OR ab >= ?) AND country_name IN ({placeholders}) __FILTERS__
        ORDER BY ag DESC, date, origin, city_name COLLATE NOCASE
        LIMIT 500
    """
    params = [min_seats, min_seats, min_seats] + list(EUROPE_COUNTRIES)
    conditions = []
    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if destination:
        conditions.append("airport_code = ?")
        params.append(destination)
    if city:
        conditions.append("(city_name LIKE ? OR airport_code LIKE ?)")
        params.append(f"%{city}%")
        params.append(f"%{city}%")
    if from_date:
        conditions.append("date >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("date <= ?")
        params.append(to_date)
    query = query.replace("__FILTERS__", " AND " + " AND ".join(conditions) if conditions else "")

    conn = get_conn()
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return render_template(
        "table.html",
        title="All Europe (≥{m} seats, any cabin)".format(m=min_seats),
        rows=rows,
        columns=["Origin", "City", "Code", "Date", "Direction", "Economy", "Plus", "Business"],
        filters={
            "origin": origin,
            "destination": destination,
            "city": city,
            "from_date": from_date,
            "to_date": to_date,
            "min_seats": min_seats,
        },
        destination_options=get_destination_options(origin),
    )


@app.route("/plus")
def plus():
    """European flights with Plus or Business (≥2 seats)."""
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
    destination = args.get("destination", "").strip().upper()
    city = args.get("city") or args.get("q", "").strip()
    from_date = args.get("from_date", "")
    to_date = args.get("to_date", "")

    placeholders = ", ".join("?" for _ in EUROPE_COUNTRIES)
    conditions = ["(ap >= ? OR ab >= ?)", "country_name IN ({})".format(placeholders)]
    params = [min_seats, min_seats] + list(EUROPE_COUNTRIES)

    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if destination:
        conditions.append("airport_code = ?")
        params.append(destination)
    if city:
        conditions.append("(city_name LIKE ? OR airport_code LIKE ?)")
        params.append("%{}%".format(city))
        params.append("%{}%".format(city))
    if from_date:
        conditions.append("date >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("date <= ?")
        params.append(to_date)

    where_clause = " AND ".join(conditions)
    query = """
        SELECT origin, city_name, airport_code, date, direction, ap, ab
        FROM flights
        WHERE {}
        ORDER BY date, origin, city_name COLLATE NOCASE
        LIMIT 500
    """.format(where_clause)

    conn = get_conn()
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return render_template(
        "table.html",
        title="Plus & Business Europe (≥{m} seats)".format(m=min_seats),
        rows=rows,
        columns=["Origin", "City", "Code", "Date", "Direction", "Plus", "Business"],
        filters={
            "origin": origin,
            "destination": destination,
            "city": city,
            "from_date": from_date,
            "to_date": to_date,
            "min_seats": min_seats,
        },
        destination_options=get_destination_options(origin),
    )


@app.route("/weekend")
def weekend():
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
    destination = args.get("destination", "").strip().upper()
    city = args.get("city") or args.get("q", "").strip()

    query = """
        SELECT inb.origin, inb.city_name, inb.airport_code, outb.date AS outbound, inb.date AS inbound,
               CASE WHEN outb.ag>0 THEN outb.ag ELSE outb.ap END AS seats_out,
               CASE WHEN inb.ag>0 THEN inb.ag ELSE inb.ap END AS seats_in
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND (inb.ag>=? OR inb.ap>=?) AND (outb.ag>=? OR outb.ap>=?)
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
          {{FILTERS}}
        ORDER BY inb.origin, inb.date, outb.date
        LIMIT 500
    """
    params = [min_seats, min_seats, min_seats, min_seats, TRIP_DAYS_MIN, TRIP_DAYS_MAX]
    conditions = []
    if origin:
        conditions.append("inb.origin = ?")
        params.append(origin)
    if destination:
        conditions.append("inb.airport_code = ?")
        params.append(destination)
    if city:
        conditions.append("(inb.city_name LIKE ? OR inb.airport_code LIKE ?)")
        params.append(f"%{city}%")
        params.append(f"%{city}%")
    if conditions:
        query = query.replace("{{FILTERS}}", " AND " + " AND ".join(conditions))
    else:
        query = query.replace("{{FILTERS}}", "")

    conn = get_conn()
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    return render_template(
        "table.html",
        title="Weekend pairs (≥{m} seats, {d1}-{d2} days)".format(m=min_seats, d1=TRIP_DAYS_MIN, d2=TRIP_DAYS_MAX),
        rows=rows,
        columns=["Origin", "City", "Code", "Outbound", "Inbound", "Seats out", "Seats in"],
        filters={"origin": origin, "destination": destination, "city": city, "min_seats": min_seats},
        destination_options=get_destination_options(origin),
    )


@app.route("/api/weekend-detail")
def weekend_detail():
    """Return full cabin breakdown (ag, ap, ab) for a weekend pair."""
    origin = request.args.get("origin")
    airport_code = request.args.get("airport_code")
    outbound = request.args.get("outbound")
    inbound = request.args.get("inbound")
    if not all([origin, airport_code, outbound, inbound]):
        return jsonify({"error": "Missing origin, airport_code, outbound, or inbound"}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT origin, airport_code, city_name, country_name, direction, date, ag, ap, ab
        FROM flights
        WHERE origin = ? AND airport_code = ? AND direction = 'outbound' AND date = ?
    """, (origin, airport_code, outbound))
    out_row = cur.fetchone()
    cur.execute("""
        SELECT origin, airport_code, city_name, country_name, direction, date, ag, ap, ab
        FROM flights
        WHERE origin = ? AND airport_code = ? AND direction = 'inbound' AND date = ?
    """, (origin, airport_code, inbound))
    in_row = cur.fetchone()
    conn.close()

    if not out_row or not in_row:
        return jsonify({"error": "Flight pair not found"}), 404

    def to_dict(row):
        return {
            "origin": row[0], "airport_code": row[1], "city_name": row[2], "country_name": row[3],
            "direction": row[4], "date": row[5], "ag": row[6], "ap": row[7], "ab": row[8],
        }

    return jsonify({
        "outbound": to_dict(out_row),
        "inbound": to_dict(in_row),
    })


@app.route("/api/weekend-routes")
def weekend_routes():
    """Fetch per-flight data from SAS routes/v1 (on-demand, low volume)."""
    origin = request.args.get("origin")
    airport_code = request.args.get("airport_code")
    outbound = request.args.get("outbound")
    inbound = request.args.get("inbound")
    if not all([origin, airport_code, outbound, inbound]):
        return jsonify({"error": "Missing origin, airport_code, outbound, or inbound"}), 400

    def fetch_routes(orig, dest, date):
        try:
            r = requests.get(
                ROUTES_API,
                params={
                    "market": "se-sv",
                    "origin": orig,
                    "destination": dest,
                    "departureDate": date,
                    "direct": "false",
                },
                timeout=15,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            return {"_error": str(e)}

    out_flights = fetch_routes(origin, airport_code, outbound)
    in_flights = fetch_routes(airport_code, origin, inbound)

    return jsonify({
        "outbound": {"date": outbound, "flights": out_flights},
        "inbound": {"date": inbound, "flights": in_flights},
    })


@app.route("/new")
def new_flights():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flight_history'")
    if not cur.fetchone():
        conn.close()
        return render_template("table.html", title="New since yesterday", rows=[], columns=[], filters={},
            message="No flight_history yet. Run update_sas_awards.py at least twice.")

    cur.execute("SELECT DISTINCT fetch_date FROM flight_history ORDER BY fetch_date DESC LIMIT 2")
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        conn.close()
        return render_template("table.html", title="New since yesterday", rows=[], columns=[], filters={},
            message="Need 2 days of history. Run update_sas_awards.py again tomorrow.")

    latest, prev = dates[0], dates[1]
    cur.execute("""
        WITH t AS (
            SELECT origin, airport_code, city_name, date, direction, ab FROM flight_history
            WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
        ),
        p AS (
            SELECT origin, airport_code, date FROM flight_history
            WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
        )
        SELECT t.origin, t.city_name, t.airport_code, t.date, 'outbound', t.ab FROM t
        LEFT JOIN p ON p.origin = t.origin AND p.airport_code = t.airport_code AND p.date = t.date
        WHERE p.airport_code IS NULL
        UNION ALL
        SELECT fh.origin, fh.city_name, fh.airport_code, fh.date, 'inbound', fh.ab FROM flight_history fh
        WHERE fh.fetch_date = ? AND fh.ab >= ? AND fh.direction = 'inbound'
        AND NOT EXISTS (
            SELECT 1 FROM flight_history p
            WHERE p.fetch_date = ? AND p.airport_code = fh.airport_code AND p.origin = fh.origin
              AND p.date = fh.date AND p.direction = 'inbound' AND p.ab >= ?
        )
        ORDER BY date, origin, city_name
        LIMIT 300
    """, (latest, MIN_SEATS, prev, MIN_SEATS, latest, MIN_SEATS, prev, MIN_SEATS))

    rows = cur.fetchall()
    conn.close()

    return render_template(
        "table.html",
        title="New since {} → {}".format(prev, latest),
        rows=rows,
        columns=["Origin", "City", "Code", "Date", "Direction", "Business"],
        filters={},
    )


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return render_template("search.html", q="", results=[])
    conn = get_conn()
    cur = conn.execute("""
        SELECT DISTINCT origin, city_name, airport_code FROM flights
        WHERE city_name LIKE ? OR airport_code LIKE ?
        ORDER BY city_name COLLATE NOCASE LIMIT 50
    """, (f"%{q}%", f"%{q}%"))
    results = cur.fetchall()
    conn.close()
    return render_template("search.html", q=q, results=results)


LONG_HAUL_COUNTRIES = (
    "USA", "Kanada", "Japan", "Korea", "Indien", "Thailand",
    "Förenade arabemiraten",
)

EU_COUNTRIES = (
    "Belgien",
    "Bulgarien",
    "Cypern",
    "Danmark",
    "Estland",
    "Finland",
    "Frankrike",
    "Grekland",
    "Irland",
    "Italien",
    "Kroatien",
    "Lettland",
    "Litauen",
    "Luxemburg",
    "Malta",
    "Nederländerna",
    "Polen",
    "Portugal",
    "Spanien",
    "Sverige",
    "Tjeckien",
    "Tyskland",
    "Ungern",
    "Österrike",
)


def get_plus_europe_destination_options(origin="", min_seats=MIN_SEATS, include_economy=False, include_plus_business=True):
    _, _, cabin_sql, cabin_params, _ = get_europe_class_config(min_seats, include_economy, include_plus_business)

    placeholders = ", ".join("?" for _ in EU_COUNTRIES)
    conditions = [
        cabin_sql,
        f"country_name IN ({placeholders})",
        "NOT ((origin = 'ARN' AND country_name = 'Sverige') OR (origin = 'CPH' AND country_name = 'Danmark'))",
    ]
    params = list(cabin_params) + list(EU_COUNTRIES)
    if origin:
        conditions.append("origin = ?")
        params.append(origin)

    where = " AND ".join(conditions)
    conn = get_conn()
    cur = conn.execute(
        f"""
        SELECT city_name, airport_code
        FROM flights
        WHERE {where}
        GROUP BY airport_code, city_name
        ORDER BY city_name COLLATE NOCASE
        """,
        params,
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_europe_class_config(min_seats, include_economy, include_plus_business):
    if not include_economy and not include_plus_business:
        include_plus_business = True

    filters = []
    params = []
    if include_economy:
        filters.append("ag >= ?")
        params.append(min_seats)
    if include_plus_business:
        filters.append("(ap >= ? OR ab >= ?)")
        params.extend([min_seats, min_seats])
    cabin_where = "(" + " OR ".join(filters) + ")"

    if include_economy and include_plus_business:
        seat_expr = "(ag + CASE WHEN ap > 0 THEN ap ELSE ab END)"
    elif include_economy:
        seat_expr = "ag"
    else:
        seat_expr = "CASE WHEN ap > 0 THEN ap ELSE ab END"

    return include_economy, include_plus_business, cabin_where, params, seat_expr


@app.route("/reports")
def reports_index():
    return render_template("reports_index.html")


@app.route("/reports/business-by-date")
def report_business_by_date():
    """Business seats aggregated by date per origin (mirrors daily_business_by_date)."""
    args = request.args
    origin = args.get("origin", "")
    direction = args.get("direction", "outbound")
    if direction not in {"outbound", "inbound", "both"}:
        direction = "outbound"
    destination = args.get("destination", "").strip().upper()
    min_seats = int(args.get("min_seats", MIN_SEATS))

    conditions = ["ab >= ?"]
    params = [min_seats]
    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if destination:
        conditions.append("airport_code = ?")
        params.append(destination)
    if direction != "both":
        conditions.append("direction = ?")
        params.append(direction)

    where = " AND ".join(conditions)
    conn = get_conn()
    cur = conn.execute(f"""
        SELECT date, origin, SUM(ab) AS total_business,
               COUNT(*) AS num_cities
        FROM flights
        WHERE {where}
        GROUP BY date, origin
        ORDER BY date, origin
    """, params)
    rows = cur.fetchall()

    cur2 = conn.execute(f"""
        SELECT origin, city_name, airport_code, date, ab
        FROM flights
        WHERE {where}
        ORDER BY date, origin, city_name COLLATE NOCASE
        LIMIT 500
    """, params)
    table_rows = cur2.fetchall()
    conn.close()

    dates = sorted(set(r[0] for r in rows))
    origins = sorted(set(r[1] for r in rows))
    by_origin = {}
    for d, o, total, ncities in rows:
        by_origin.setdefault(o, {})[d] = total

    chart_data = {
        "labels": dates,
        "datasets": [
            {"label": o, "data": [by_origin.get(o, {}).get(d, 0) for d in dates]}
            for o in origins
        ],
    }

    return render_template(
        "report_chart.html",
        title="Business Seats by Date",
        subtitle=f"{'Both directions' if direction == 'both' else f'{direction.capitalize()} flights'} with ≥{min_seats} business seats",
        chart_type="bar",
        chart_data=chart_data,
        table_rows=table_rows,
        table_columns=["Origin", "City", "Code", "Date", "Business"],
        filters={"origin": origin, "destination": destination, "direction": direction, "min_seats": min_seats},
        destination_options=get_destination_options(
            origin=origin,
            direction=direction,
            min_seats=min_seats,
            seat_column="ab",
        ),
        report_path="/reports/business-by-date",
    )


@app.route("/reports/new-business")
def report_new_business():
    """New business flights since yesterday (mirrors daily_new_business_report)."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flight_history'")
    if not cur.fetchone():
        conn.close()
        return render_template(
            "report_chart.html", title="New Business Flights", subtitle="",
            chart_type="bar", chart_data={"labels": [], "datasets": []},
            table_rows=[], table_columns=[], filters={},
            report_path="/reports/new-business",
            message="No flight_history yet. Run update_sas_awards.py at least twice.",
        )

    cur.execute("SELECT DISTINCT fetch_date FROM flight_history ORDER BY fetch_date DESC LIMIT 2")
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        conn.close()
        return render_template(
            "report_chart.html", title="New Business Flights", subtitle="",
            chart_type="bar", chart_data={"labels": [], "datasets": []},
            table_rows=[], table_columns=[], filters={},
            report_path="/reports/new-business",
            message="Need 2 days of history. Run update_sas_awards.py again tomorrow.",
        )

    latest, prev = dates[0], dates[1]
    direction = request.args.get("direction", "outbound")
    if direction not in {"outbound", "inbound", "both"}:
        direction = "outbound"
    min_seats = int(request.args.get("min_seats", MIN_SEATS))

    if direction == "both":
        direction_filter_sql = ""
        direction_params = ()
    else:
        direction_filter_sql = "AND direction = ?"
        direction_params = (direction,)

    cur.execute(f"""
        WITH t AS (
            SELECT origin, airport_code, city_name, date, ab
            FROM flight_history
            WHERE fetch_date = ? {direction_filter_sql} AND ab >= ?
        ),
        p AS (
            SELECT origin, airport_code, date
            FROM flight_history
            WHERE fetch_date = ? {direction_filter_sql} AND ab >= ?
        )
        SELECT t.origin, t.city_name, t.airport_code, t.date, t.ab
        FROM t
        LEFT JOIN p ON p.origin = t.origin AND p.airport_code = t.airport_code AND p.date = t.date
        WHERE p.airport_code IS NULL
        ORDER BY t.origin, t.city_name COLLATE NOCASE, t.date
    """, (latest,) + direction_params + (min_seats, prev) + direction_params + (min_seats,))
    table_rows = cur.fetchall()

    city_counts = {}
    city_seat_counts = {}
    for origin, city, code, date, ab in table_rows:
        key = f"{city} ({code})"
        city_counts[key] = city_counts.get(key, 0) + 1
        city_seat_counts[key] = city_seat_counts.get(key, 0) + ab

    sorted_cities = sorted(city_counts.items(), key=lambda x: -x[1])[:30]
    chart_data = {
        "labels": [c[0] for c in sorted_cities],
        "datasets": [{"label": "New Flights", "data": [c[1] for c in sorted_cities]}],
    }
    map_points = []
    for city_code, flights in sorted(city_counts.items(), key=lambda x: (-x[1], x[0])):
        city, code = city_code.rsplit(" (", 1)
        code = code.rstrip(")")
        map_points.append(
            {
                "city": city,
                "code": code,
                "flights": flights,
                "seats": city_seat_counts.get(city_code, 0),
            }
        )
    conn.close()

    return render_template(
        "report_chart.html",
        title="New Business Flights",
        subtitle=f"New {'both-direction' if direction == 'both' else direction} business seats since {prev} → {latest}",
        chart_type="bar",
        chart_data=chart_data,
        table_rows=table_rows,
        table_columns=["Origin", "City", "Code", "Date", "Business"],
        filters={"direction": direction, "min_seats": min_seats},
        map_points=map_points,
        report_path="/reports/new-business",
    )


@app.route("/reports/plus-europe")
def report_plus_europe():
    """Plus Europe availability by city (mirrors daily_plus_europe.sh)."""
    args = request.args
    origin = args.get("origin", "")
    destination = args.get("destination", "").strip().upper()
    min_seats = int(args.get("min_seats", MIN_SEATS))
    include_economy = is_checked(args.get("class_economy"))
    include_plus_business = is_checked(args.get("class_plus_business"))
    (
        include_economy,
        include_plus_business,
        cabin_where,
        cabin_params,
        seat_expr,
    ) = get_europe_class_config(min_seats, include_economy, include_plus_business)

    placeholders = ", ".join("?" for _ in EU_COUNTRIES)
    conditions = [
        cabin_where,
        f"country_name IN ({placeholders})",
        "NOT ((origin = 'ARN' AND country_name = 'Sverige') OR (origin = 'CPH' AND country_name = 'Danmark'))",
    ]
    params = list(cabin_params) + list(EU_COUNTRIES)

    if origin:
        conditions.append("origin = ?")
        params.append(origin)
    if destination:
        conditions.append("airport_code = ?")
        params.append(destination)

    where = " AND ".join(conditions)
    conn = get_conn()

    cur = conn.execute(f"""
        SELECT city_name, airport_code, SUM({seat_expr}) AS total_plus, COUNT(*) AS num_dates
        FROM flights
        WHERE {where}
        GROUP BY city_name, airport_code
        ORDER BY total_plus DESC
    """, params)
    city_rows = cur.fetchall()

    cur2 = conn.execute(f"""
        SELECT origin, city_name, airport_code, date, direction, {seat_expr} AS seats
        FROM flights
        WHERE {where}
        ORDER BY city_name COLLATE NOCASE, date
        LIMIT 500
    """, params)
    table_rows = cur2.fetchall()

    cur3 = conn.execute(f"""
        SELECT date, direction, SUM({seat_expr}) AS total_seats
        FROM flights
        WHERE {where}
        GROUP BY date, direction
        ORDER BY date, direction
    """, params)
    date_direction_rows = cur3.fetchall()
    conn.close()

    chart_data = {
        "labels": [f"{r[0]} ({r[1]})" for r in city_rows[:25]],
        "datasets": [{"label": "Total Plus Seats", "data": [r[2] for r in city_rows[:25]]}],
    }

    by_date = {}
    for d, direction, n in date_direction_rows:
        by_date.setdefault(d, {"outbound": 0, "inbound": 0})
        if direction == "outbound":
            by_date[d]["outbound"] += n
        elif direction == "inbound":
            by_date[d]["inbound"] += n
    sorted_dates = sorted(by_date.keys())
    timeline_data = {
        "labels": sorted_dates,
        "datasets": [
            {"label": "Outbound flights", "data": [by_date[d]["outbound"] for d in sorted_dates]},
            {"label": "Inbound flights", "data": [by_date[d]["inbound"] for d in sorted_dates]},
        ],
    }
    plus_year_calendar = build_dual_year_calendar_data(
        outbound_counts={d: by_date[d]["outbound"] for d in sorted_dates},
        inbound_counts={d: by_date[d]["inbound"] for d in sorted_dates},
        days=365,
        min_flights=min_seats,
    )

    return render_template(
        "report_chart.html",
        title="Europe Availability",
        subtitle=f"EU flights with ≥{min_seats} seats (domestic ARN-SE and CPH-DK excluded)",
        chart_type="bar",
        chart_data=chart_data,
        secondary_chart_type="line",
        secondary_chart_data=timeline_data,
        secondary_chart_title="Seats Over Time",
        table_rows=table_rows,
        table_columns=["Origin", "City", "Code", "Date", "Direction", "Seats"],
        filters={
            "origin": origin,
            "destination": destination,
            "min_seats": min_seats,
            "class_economy": include_economy,
            "class_plus_business": include_plus_business,
        },
        destination_options=get_plus_europe_destination_options(
            origin=origin,
            min_seats=min_seats,
            include_economy=include_economy,
            include_plus_business=include_plus_business,
        ),
        plus_year_calendar=plus_year_calendar,
        report_path="/reports/plus-europe",
    )


@app.route("/reports/weekend-trips")
def report_weekend_trips():
    """Weekend trip summary by city (mirrors split_weekend_trips.sh)."""
    args = request.args
    origin = args.get("origin", "")
    country = args.get("country", "")
    include_plus = is_checked(args.get("plus"))
    include_business = is_checked(args.get("business"))
    cabin_cols = weekend_cabin_columns(include_plus=include_plus, include_business=include_business)

    origin_filter = ""
    country_filter = ""
    params = [MIN_SEATS] * (len(cabin_cols) * 2) + [TRIP_DAYS_MIN, TRIP_DAYS_MAX]
    if origin:
        origin_filter = "AND inb.origin = ?"
        params.append(origin)
    if country:
        country_filter = "AND inb.country_name = ?"
        params.append(country)

    conn = get_conn()
    cur = conn.execute(f"""
        SELECT inb.origin, inb.city_name, inb.airport_code,
               COUNT(*) AS pairs,
               MIN(outb.date) AS earliest_out,
               MAX(inb.date) AS latest_in
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND {weekend_leg_clause("inb", cabin_cols)} AND {weekend_leg_clause("outb", cabin_cols)}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
          {origin_filter}
          {country_filter}
        GROUP BY inb.origin, inb.city_name, inb.airport_code
        ORDER BY pairs DESC
    """, params)
    city_rows = cur.fetchall()

    chart_data = {
        "labels": [f"{r[1]} ({r[2]})" for r in city_rows[:25]],
        "datasets": [{"label": "Weekend Pairs", "data": [r[3] for r in city_rows[:25]]}],
    }

    origin_summary = {}
    for origin_val, city, code, pairs, earliest, latest in city_rows:
        origin_summary.setdefault(origin_val, {"cities": 0, "pairs": 0})
        origin_summary[origin_val]["cities"] += 1
        origin_summary[origin_val]["pairs"] += pairs

    conn.close()

    return render_template(
        "report_weekend.html",
        title="Weekend Trips Summary",
        subtitle=f"Cities with weekend pairs (≥{MIN_SEATS} seats, {TRIP_DAYS_MIN}–{TRIP_DAYS_MAX} days)",
        chart_data=chart_data,
        city_rows=city_rows,
        origin_summary=origin_summary,
        filters={"origin": origin, "country": country, "plus": include_plus, "business": include_business},
        country_options=get_weekend_country_options(
            origin=origin,
            include_plus=include_plus,
            include_business=include_business,
        ),
        report_path="/reports/weekend-trips",
    )


@app.route("/api/weekend-year-calendar")
def weekend_year_calendar():
    """Return 365-day weekend-pair availability counts for one origin/city."""
    origin = request.args.get("origin", "").strip().upper()
    airport_code = request.args.get("airport_code", "").strip().upper()
    include_plus = is_checked(request.args.get("plus"))
    include_business = is_checked(request.args.get("business"))
    cabin_cols = weekend_cabin_columns(include_plus=include_plus, include_business=include_business)
    if not origin or not airport_code:
        return jsonify({"error": "Missing origin or airport_code"}), 400

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT outb.date AS outbound_date, inb.date AS inbound_date, COUNT(*) AS pair_count
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND inb.origin = ? AND inb.airport_code = ?
          AND {weekend_leg_clause("inb", cabin_cols)} AND {weekend_leg_clause("outb", cabin_cols)}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
        GROUP BY outb.date, inb.date
        ORDER BY outb.date, inb.date
        """,
        (origin, airport_code) + tuple([MIN_SEATS] * (len(cabin_cols) * 2)) + (TRIP_DAYS_MIN, TRIP_DAYS_MAX),
    )
    rows = cur.fetchall()

    cur.execute(
        """
        SELECT city_name, country_name
        FROM flights
        WHERE origin = ? AND airport_code = ?
        ORDER BY city_name COLLATE NOCASE
        LIMIT 1
        """,
        (origin, airport_code),
    )
    city_row = cur.fetchone()
    conn.close()

    by_outbound = {}
    by_inbound = {}
    for out_date, in_date, count in rows:
        by_outbound[out_date] = by_outbound.get(out_date, 0) + count
        by_inbound[in_date] = by_inbound.get(in_date, 0) + count

    return jsonify(
        {
            "origin": origin,
            "airport_code": airport_code,
            "city_name": city_row[0] if city_row else airport_code,
            "country_name": city_row[1] if city_row else "",
            "outbound_daily_pairs": by_outbound,
            "inbound_daily_pairs": by_inbound,
        }
    )


@app.route("/api/weekend-day-pairs")
def weekend_day_pairs():
    """Return detailed weekend pairs for a specific outbound or inbound day."""
    origin = request.args.get("origin", "").strip().upper()
    airport_code = request.args.get("airport_code", "").strip().upper()
    day = request.args.get("day", "").strip()
    mode = request.args.get("mode", "").strip().lower()  # outbound or inbound
    include_plus = is_checked(request.args.get("plus"))
    include_business = is_checked(request.args.get("business"))
    cabin_cols = weekend_cabin_columns(include_plus=include_plus, include_business=include_business)

    if not origin or not airport_code or not day or mode not in {"outbound", "inbound"}:
        return jsonify({"error": "Missing or invalid origin, airport_code, day, or mode"}), 400

    day_filter = "outb.date = ?" if mode == "outbound" else "inb.date = ?"
    params = (
        origin,
        airport_code,
    ) + tuple([MIN_SEATS] * (len(cabin_cols) * 2)) + (
        TRIP_DAYS_MIN,
        TRIP_DAYS_MAX,
        day,
    )

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            outb.date AS outbound_date,
            inb.date AS inbound_date,
            outb.ag, outb.ap, outb.ab,
            inb.ag, inb.ap, inb.ab
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND inb.origin = ? AND inb.airport_code = ?
          AND {weekend_leg_clause("inb", cabin_cols)} AND {weekend_leg_clause("outb", cabin_cols)}
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
          AND {day_filter}
        ORDER BY outb.date, inb.date
        LIMIT 400
        """,
        params,
    )
    rows = cur.fetchall()

    cur.execute(
        """
        SELECT city_name, country_name
        FROM flights
        WHERE origin = ? AND airport_code = ?
        ORDER BY city_name COLLATE NOCASE
        LIMIT 1
        """,
        (origin, airport_code),
    )
    city_row = cur.fetchone()
    conn.close()

    pairs = []
    for row in rows:
        pairs.append(
            {
                "outbound": row[0],
                "inbound": row[1],
                "outbound_cabins": {"ag": row[2], "ap": row[3], "ab": row[4]},
                "inbound_cabins": {"ag": row[5], "ap": row[6], "ab": row[7]},
            }
        )

    return jsonify(
        {
            "origin": origin,
            "airport_code": airport_code,
            "city_name": city_row[0] if city_row else airport_code,
            "country_name": city_row[1] if city_row else "",
            "day": day,
            "mode": mode,
            "pairs": pairs,
        }
    )


@app.route("/api/plus-day-routes")
def plus_day_routes():
    """Fetch per-flight outbound/inbound details for one day and route."""
    origin = request.args.get("origin", "").strip().upper()
    destination = request.args.get("destination", "").strip().upper()
    day = request.args.get("day", "").strip()
    if not origin or not destination or not day:
        return jsonify({"error": "Missing origin, destination, or day"}), 400

    def fetch_routes(orig, dest, date):
        try:
            r = requests.get(
                ROUTES_API,
                params={
                    "market": "se-sv",
                    "origin": orig,
                    "destination": dest,
                    "departureDate": date,
                    "direct": "false",
                },
                timeout=15,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            return {"_error": str(e)}

    outbound = fetch_routes(origin, destination, day)
    inbound = fetch_routes(destination, origin, day)
    return jsonify(
        {
            "origin": origin,
            "destination": destination,
            "day": day,
            "outbound": outbound,
            "inbound": inbound,
        }
    )


@app.route("/reports/summary")
def report_summary():
    """Morning summary report (mirrors scripts/morning_report.py)."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT origin, direction, COUNT(*) FROM flights
        WHERE ab >= ? OR ap >= ? OR ag >= ?
        GROUP BY origin, direction
    """, (MIN_SEATS, MIN_SEATS, MIN_SEATS))
    summary_counts = cur.fetchall()

    cur.execute("""
        SELECT origin, city_name, airport_code, date, direction, ab
        FROM flights WHERE ab >= ?
        ORDER BY ab DESC, date LIMIT 10
    """, (MIN_SEATS,))
    top_business = cur.fetchall()

    cur.execute(f"""
        SELECT inb.origin, inb.city_name, COUNT(*) AS pairs
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE inb.direction = 'inbound' AND outb.direction = 'outbound'
          AND (inb.ag >= ? OR inb.ap >= ?) AND (outb.ag >= ? OR outb.ap >= ?)
          AND strftime('%w', inb.date) IN ('6','0','1')
          AND strftime('%w', outb.date) IN ('3','4','5')
          AND (julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?
          AND date(inb.date) BETWEEN date('now') AND date('now','+1 year')
        GROUP BY inb.origin, inb.city_name
        ORDER BY pairs DESC LIMIT 10
    """, (MIN_SEATS, MIN_SEATS, MIN_SEATS, MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX))
    top_weekend = cur.fetchall()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flight_history'")
    has_history = cur.fetchone() is not None
    new_business = []
    if has_history:
        cur.execute("SELECT DISTINCT fetch_date FROM flight_history ORDER BY fetch_date DESC LIMIT 2")
        dates = [r[0] for r in cur.fetchall()]
        if len(dates) >= 2:
            latest, prev = dates[0], dates[1]
            cur.execute("""
                WITH t AS (
                    SELECT origin, airport_code, city_name, date, direction, ab
                    FROM flight_history
                    WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
                ),
                p AS (
                    SELECT origin, airport_code, date
                    FROM flight_history
                    WHERE fetch_date = ? AND ab >= ? AND direction = 'outbound'
                )
                SELECT t.origin, t.city_name, t.airport_code, t.date, t.ab
                FROM t LEFT JOIN p
                  ON p.origin = t.origin AND p.airport_code = t.airport_code AND p.date = t.date
                WHERE p.airport_code IS NULL
                ORDER BY t.ab DESC, t.date LIMIT 10
            """, (latest, MIN_SEATS, prev, MIN_SEATS))
            new_business = cur.fetchall()

    origin_chart = {}
    for origin_val, direction, cnt in summary_counts:
        origin_chart.setdefault(origin_val, {})[direction] = cnt
    origins = sorted(origin_chart.keys())
    summary_chart = {
        "labels": origins,
        "datasets": [
            {"label": "Outbound", "data": [origin_chart.get(o, {}).get("outbound", 0) for o in origins]},
            {"label": "Inbound", "data": [origin_chart.get(o, {}).get("inbound", 0) for o in origins]},
        ],
    }

    conn.close()
    return render_template(
        "report_summary.html",
        title="Morning Summary",
        subtitle="Overview of current flight availability",
        summary_counts=summary_counts,
        summary_chart=summary_chart,
        top_business=top_business,
        top_weekend=top_weekend,
        new_business=new_business,
    )


@app.route("/reports/us-calendar")
def report_us_calendar():
    """ARN → US flight pairs in Business/Plus on a 365-day calendar view."""
    args = request.args
    year = _dt.date.today().year
    min_days = int(args.get("min_days", 3))
    max_days = int(args.get("max_days", 10))
    city_filter = args.get("city", "")

    conn = get_conn()
    cur = conn.cursor()

    conditions = [
        "inb.direction = 'inbound'",
        "outb.direction = 'outbound'",
        "inb.origin = 'ARN'",
        "inb.country_name = 'USA'",
        "(outb.ab >= ? OR outb.ap >= ?)",
        "(inb.ab >= ? OR inb.ap >= ?)",
        "(julianday(inb.date) - julianday(outb.date)) BETWEEN ? AND ?",
        "date(inb.date) BETWEEN date('now') AND date('now','+1 year')",
    ]
    params = [MIN_SEATS, MIN_SEATS, MIN_SEATS, MIN_SEATS, min_days, max_days]

    if city_filter:
        conditions.append("outb.airport_code = ?")
        params.append(city_filter)

    where = " AND ".join(conditions)
    cur.execute(f"""
        SELECT
            outb.city_name, outb.airport_code,
            outb.date, inb.date,
            outb.ap, outb.ab,
            inb.ap, inb.ab
        FROM flights AS inb
        JOIN flights AS outb
          ON inb.airport_code = outb.airport_code AND inb.origin = outb.origin
        WHERE {where}
        ORDER BY outb.date, outb.city_name
    """, params)

    pairs = []
    for i, r in enumerate(cur.fetchall()):
        pairs.append({
            "id": i, "city": r[0], "code": r[1],
            "outbound": r[2], "inbound": r[3],
            "plus_out": r[4], "biz_out": r[5],
            "plus_in": r[6], "biz_in": r[7],
        })

    all_cities_cur = conn.execute("""
        SELECT DISTINCT city_name, airport_code FROM flights
        WHERE origin = 'ARN' AND country_name = 'USA'
          AND (ab >= ? OR ap >= ?) AND date >= date('now')
        ORDER BY city_name
    """, (MIN_SEATS, MIN_SEATS))
    all_us_cities = [(r[0], r[1]) for r in all_cities_cur.fetchall()]

    cities = sorted(set(p["code"] for p in pairs))
    conn.close()

    return render_template(
        "report_us_calendar.html",
        pairs=pairs, cities=cities, year=year,
        all_us_cities=all_us_cities,
        filters={"min_days": min_days, "max_days": max_days, "city": city_filter},
    )


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
