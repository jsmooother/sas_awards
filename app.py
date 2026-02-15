#!/usr/bin/env python3
"""
SAS Awards web dashboard. Run: flask run --host=0.0.0.0 --port=5000
Access from LAN: http://<macmini-ip>:5000
"""
import os
import requests
from flask import Flask, render_template, request, jsonify
import sqlite3

ROUTES_API = "https://www.sas.se/bff/award-finder/routes/v1"

app = Flask(__name__)
DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))

from report_config import MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX

EUROPE_COUNTRIES = (
    "Österrike", "Belgien", "Danmark", "Frankrike", "Tyskland",
    "Irland", "Italien", "Nederländerna", "Norge",
    "Portugal", "Spanien", "Sverige", "Schweiz", "Storbritannien"
)


def get_conn():
    return sqlite3.connect(DB_PATH)


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


@app.route("/business")
def business():
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
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
        filters={"origin": origin, "city": city, "from_date": from_date, "to_date": to_date, "min_seats": min_seats},
    )


@app.route("/all")
def all_flights():
    """European flights – all cabins (Economy, Plus, Business). Mostly Economy."""
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
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
        filters={"origin": origin, "city": city, "from_date": from_date, "to_date": to_date, "min_seats": min_seats},
    )


@app.route("/plus")
def plus():
    """European flights with Plus or Business (≥2 seats)."""
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
    city = args.get("city") or args.get("q", "").strip()
    from_date = args.get("from_date", "")
    to_date = args.get("to_date", "")

    placeholders = ", ".join("?" for _ in EUROPE_COUNTRIES)
    conditions = ["(ap >= ? OR ab >= ?)", "country_name IN ({})".format(placeholders)]
    params = [min_seats, min_seats] + list(EUROPE_COUNTRIES)

    if origin:
        conditions.append("origin = ?")
        params.append(origin)
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
        filters={"origin": origin, "city": city, "from_date": from_date, "to_date": to_date, "min_seats": min_seats},
    )


@app.route("/weekend")
def weekend():
    args = request.args
    min_seats = int(args.get("min_seats", MIN_SEATS))
    origin = args.get("origin", "")
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
        filters={"origin": origin, "city": city, "min_seats": min_seats},
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


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
