#!/usr/bin/env python3
"""
SAS Awards web dashboard.
Two pages: Dashboard (/) and Reports (/reports).
"""
import os
import requests
from flask import Flask, render_template, request, jsonify, redirect

import queries
import regions as _regions
from report_config import MIN_SEATS

ROUTES_API = "https://www.sas.se/bff/award-finder/routes/v1"
# SAS booking: sas.se/boka/flyg is 404; use flysas.com booking page
BOOK_BASE_URL = "https://www.flysas.com/en/book"

app = Flask(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Primary pages
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/")
def dashboard():
    args = request.args
    mode = args.get("mode", "flights")
    region = args.get("region", "")
    cabin = args.get("cabin", "all")
    origin = args.get("origin", "")
    city = args.get("city", "")
    min_seats = int(args.get("min_seats", MIN_SEATS))
    from_date = args.get("from", "")
    to_date = args.get("to", "")
    page = int(args.get("page", 1))

    stats = queries.dashboard_stats()
    region_defs = queries.region_counts()

    countries = _regions.region_countries(region) if region else None

    if mode == "weekend":
        results = queries.query_weekend_pairs(
            countries=countries, cabin=cabin, origin=origin,
            min_seats=min_seats, city=city, page=page, per_page=50,
        )
    else:
        results = queries.query_flights(
            countries=countries, cabin=cabin, origin=origin,
            min_seats=min_seats, from_date=from_date, to_date=to_date,
            city=city, page=page, per_page=50,
        )

    return render_template(
        "dashboard.html",
        stats=stats, regions=region_defs, results=results,
        filters={
            "mode": mode, "region": region, "cabin": cabin, "origin": origin,
            "city": city, "min_seats": min_seats,
            "from": from_date, "to": to_date, "page": page,
        },
    )


@app.route("/reports")
def reports():
    tab = request.args.get("tab", "region")
    origin = request.args.get("origin", "")
    cabin = request.args.get("cabin", "all")
    country = request.args.get("country", "")
    min_seats = int(request.args.get("min_seats", MIN_SEATS))
    region = request.args.get("region", "")

    data = None
    if tab == "region":
        data = queries.report_region(
            cabin=cabin, origin=origin, country=country, min_seats=min_seats
        )
    elif tab == "city":
        countries = [country] if country else (
            _regions.region_countries(region) if region else None
        )
        data = queries.report_cities(
            countries=countries, cabin=cabin, origin=origin, min_seats=min_seats
        )
    elif tab == "business":
        data = queries.report_business(
            origin=origin, country=country, min_seats=min_seats
        )
    elif tab == "weekend":
        if cabin not in ("all", "business_plus", "business"):
            cabin = "all"
        data = queries.report_weekend(
            origin=origin, country=country, min_seats=min_seats, cabin=cabin
        )
    elif tab == "new":
        data = queries.report_new()

    countries_list = _regions.all_countries()
    if tab == "weekend":
        weekend_countries = queries.countries_with_weekend_pairs(
            origin=origin, cabin=cabin, min_seats=min_seats
        )
        if weekend_countries:
            countries_list = weekend_countries
            if country and country not in weekend_countries:
                countries_list = [country] + [c for c in countries_list if c != country]
    return render_template(
        "reports.html", tab=tab, data=data,
        filters={"origin": origin, "cabin": cabin, "country": country,
                 "min_seats": min_seats, "region": region},
        countries_list=countries_list,
    )


# ═══════════════════════════════════════════════════════════════════════════
# API endpoints
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/reports/region/cities")
def api_reports_region_cities():
    """Cities in a single country for region-tab drill-down."""
    country = request.args.get("country", "")
    cabin = request.args.get("cabin", "all")
    origin = request.args.get("origin", "")
    min_seats = int(request.args.get("min_seats", MIN_SEATS))
    if not country:
        return jsonify({"error": "Missing country"}), 400
    data = queries.cities_for_country(
        country=country, cabin=cabin, origin=origin, min_seats=min_seats
    )
    return jsonify({"table": data.get("table", [])})


@app.route("/api/reports/calendar")
def api_reports_calendar():
    """Daily availability for origin+code from today to +365 days."""
    origin = request.args.get("origin", "")
    code = request.args.get("code", "")
    min_seats = int(request.args.get("min_seats", MIN_SEATS))
    if not origin or not code:
        return jsonify({"error": "Missing origin or code"}), 400
    days = queries.calendar_availability(origin, code, min_seats=min_seats)
    city_name = ""
    if days:
        first_date = next(iter(days))
        detail = queries.route_detail(origin, code, first_date)
        if detail:
            leg = detail.get("outbound") or detail.get("inbound") or {}
            city_name = leg.get("city_name", "")
    return jsonify({"origin": origin, "code": code, "city": city_name, "days": days})


@app.route("/api/reports/calendar/weekend-pairs")
def api_reports_calendar_weekend_pairs():
    """Weekend pairs (out Wed–Fri, back Sat–Mon) for a single route."""
    origin = request.args.get("origin", "")
    code = request.args.get("code", "")
    min_seats = int(request.args.get("min_seats", MIN_SEATS))
    cabin = request.args.get("cabin", "all")
    if not origin or not code:
        return jsonify({"error": "Missing origin or code"}), 400
    pairs = queries.weekend_pairs_for_route(
        origin, code, min_seats=min_seats, cabin=cabin
    )
    return jsonify({"origin": origin, "code": code, "pairs": pairs})


@app.route("/api/detail")
def api_detail():
    origin = request.args.get("origin", "")
    dest = request.args.get("dest", "")
    date = request.args.get("date", "")
    if not all([origin, dest, date]):
        return jsonify({"error": "Missing origin, dest, or date"}), 400

    detail = queries.route_detail(origin, dest, date)
    if not detail:
        return jsonify({"error": "Route not found"}), 404

    sas_url = (
        f"{BOOK_BASE_URL}?from={origin}&to={dest}"
        f"&outDate={date}&adt=2&bookingType=O"
    )
    return jsonify({"legs": detail, "booking_url": sas_url})


@app.route("/api/routes")
def api_routes_proxy():
    """Proxy to SAS routes/v1 for per-flight departure/arrival data."""
    orig = request.args.get("origin", "")
    dest = request.args.get("dest", "")
    date = request.args.get("date", "")
    if not all([orig, dest, date]):
        return jsonify({"error": "Missing origin, dest, or date"}), 400
    try:
        r = requests.get(ROUTES_API, params={
            "market": "se-sv", "origin": orig, "destination": dest,
            "departureDate": date, "direct": "false",
        }, timeout=15)
        r.raise_for_status()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"_error": str(e)})


@app.route("/api/weekend-pair-detail")
def api_weekend_pair_detail():
    """Full cabin breakdown for an outbound+inbound weekend pair."""
    origin = request.args.get("origin", "")
    dest = request.args.get("dest", "")
    outbound = request.args.get("outbound", "")
    inbound = request.args.get("inbound", "")
    if not all([origin, dest, outbound, inbound]):
        return jsonify({"error": "Missing origin, dest, outbound, or inbound"}), 400

    out_detail = queries.route_detail(origin, dest, outbound)
    in_detail = queries.route_detail(origin, dest, inbound)

    out_leg = (out_detail or {}).get("outbound")
    in_leg = (in_detail or {}).get("inbound")
    if not out_leg and not in_leg:
        return jsonify({"error": "Pair not found"}), 404

    sas_url = (
        f"{BOOK_BASE_URL}?from={origin}&to={dest}"
        f"&outDate={outbound}&inDate={inbound}&adt=2&bookingType=R"
    )
    return jsonify({
        "outbound": out_leg, "inbound": in_leg,
        "booking_url": sas_url,
    })


@app.route("/api/flow/regions")
def api_flow_regions():
    return jsonify(queries.region_counts())


@app.route("/api/flow/results")
def api_flow_results():
    args = request.args
    region = args.get("region", "")
    countries = _regions.region_countries(region) if region else None
    data = queries.query_flights(
        countries=countries,
        cabin=args.get("cabin", "all"),
        origin=args.get("origin", ""),
        min_seats=int(args.get("min_seats", MIN_SEATS)),
        from_date=args.get("from", ""),
        to_date=args.get("to", ""),
        city=args.get("city", ""),
        page=int(args.get("page", 1)),
    )
    return jsonify(data)


# ═══════════════════════════════════════════════════════════════════════════
# Legacy redirects (301)
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/search")
def legacy_search():
    q = request.args.get("q", "")
    return redirect(f"/?city={q}" if q else "/", 301)


@app.route("/flow")
def legacy_flow():
    params = request.query_string.decode()
    return redirect(f"/?{params}" if params else "/", 301)


@app.route("/all")
def legacy_all():
    return redirect("/reports?tab=region", 301)


@app.route("/business")
def legacy_business():
    return redirect("/reports?tab=business", 301)


@app.route("/plus")
def legacy_plus():
    return redirect("/reports?tab=region&cabin=plus", 301)


@app.route("/weekend")
def legacy_weekend():
    return redirect("/reports?tab=weekend", 301)


@app.route("/new")
def legacy_new():
    return redirect("/reports?tab=new", 301)


@app.route("/reports/business-by-date")
def legacy_biz_date():
    return redirect("/reports?tab=business", 301)


@app.route("/reports/new-business")
def legacy_new_biz():
    return redirect("/reports?tab=new", 301)


@app.route("/reports/plus-europe")
def legacy_plus_europe():
    return redirect("/reports?tab=region", 301)


@app.route("/reports/weekend-trips")
def legacy_weekend_trips():
    return redirect("/reports?tab=weekend", 301)


@app.route("/reports/summary")
def legacy_summary():
    return redirect("/reports?tab=region", 301)


@app.route("/reports/us-calendar")
def legacy_us_cal():
    return redirect("/reports?tab=region", 301)


# Keep old API aliases working
@app.route("/api/weekend-detail")
def legacy_weekend_detail():
    origin = request.args.get("origin", "")
    code = request.args.get("airport_code", "")
    outbound = request.args.get("outbound", "")
    inbound = request.args.get("inbound", "")
    if not all([origin, code, outbound, inbound]):
        return jsonify({"error": "Missing params"}), 400
    out = queries.route_detail(origin, code, outbound)
    inb = queries.route_detail(origin, code, inbound)
    return jsonify({
        "outbound": (out or {}).get("outbound"),
        "inbound": (inb or {}).get("inbound"),
    })


def _normalize_routes_response(raw):
    """Extract list of flight objects from SAS routes/v1 response (array or wrapped)."""
    if raw is None:
        return []
    if isinstance(raw, dict) and "_error" in raw:
        return raw  # pass through error so caller can check _error
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("flights", "routes", "data", "outbound", "inbound"):
            val = raw.get(key)
            if isinstance(val, list):
                return val
    return []


@app.route("/api/weekend-routes")
def legacy_weekend_routes():
    origin = request.args.get("origin", "")
    code = request.args.get("airport_code", "")
    outbound = request.args.get("outbound", "")
    inbound = request.args.get("inbound", "")
    if not all([origin, code, outbound, inbound]):
        return jsonify({"error": "Missing params"}), 400

    def fetch(orig, dest, date):
        try:
            r = requests.get(ROUTES_API, params={
                "market": "se-sv", "origin": orig, "destination": dest,
                "departureDate": date, "direct": "false",
            }, timeout=15)
            r.raise_for_status()
            raw = r.json()
            out = _normalize_routes_response(raw)
            return out if isinstance(out, list) else out  # list or {_error: ...}
        except Exception as e:
            return {"_error": str(e)}

    out_res = fetch(origin, code, outbound)
    inb_res = fetch(code, origin, inbound)

    def to_payload(res):
        if isinstance(res, dict) and "_error" in res:
            return {"_error": res["_error"], "flights": []}
        return {"flights": res if isinstance(res, list) else []}

    return jsonify({
        "outbound": {"date": outbound, **to_payload(out_res)},
        "inbound": {"date": inbound, **to_payload(inb_res)},
    })


@app.route("/api/flow/detail")
def legacy_flow_detail():
    return api_detail()


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
