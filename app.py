#!/usr/bin/env python3
"""
SAS Awards web dashboard. Run: flask run --host=0.0.0.0 --port=5000
Access from LAN: http://<macmini-ip>:5000
"""
import json
import os
import subprocess
import sys
from pathlib import Path
import datetime as _dt
import requests
from flask import Flask, render_template, request, jsonify, redirect, flash
import sqlite3

ROUTES_API = "https://www.sas.se/bff/award-finder/routes/v1"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-in-production")
DB_PATH = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards/sas_awards.sqlite"))

from report_config import MIN_SEATS, TRIP_DAYS_MIN, TRIP_DAYS_MAX

from partner_awards.airfrance.routes import bp as partner_awards_airfrance_bp

app.register_blueprint(partner_awards_airfrance_bp)


def _partner_awards_error_handler(f):
    """Wrap partner-awards routes: log traceback, return friendly error."""
    from functools import wraps
    import traceback
    @wraps(f)
    def inner(*a, **k):
        try:
            return f(*a, **k)
        except Exception as e:
            traceback.print_exc()
            if request.path.endswith("/delta") or request.path.endswith("/telegram"):
                return jsonify({"ok": False, "message": str(e)[:300]})
            return render_template(
                "partner_awards_error.html",
                message=str(e)[:300],
                path=request.path,
            ), 500
    return inner


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


def build_partner_top_deals_calendar(daily_deals, days=365):
    """Build month calendar grids (weeks × 7 days) for Partner top deals, SAS-style."""
    import calendar as cal_mod
    start = _dt.date.today()
    miles_list = [d["miles"] for d in daily_deals.values() if d.get("miles") is not None]
    global_min = min(miles_list, default=None)
    months = {}
    for i in range(days):
        d = start + _dt.timedelta(days=i)
        iso = d.isoformat()
        deal = daily_deals.get(iso, {})
        miles = deal.get("miles")
        has_data = miles is not None
        is_min = has_data and global_min is not None and miles == global_min
        key = d.strftime("%Y-%m")
        if key not in months:
            months[key] = {"label": d.strftime("%b %Y"), "year": d.year, "month": d.month, "by_day": {}}
        months[key]["by_day"][d.day] = {
            "iso": iso,
            "day": d.strftime("%d"),
            "miles": miles,
            "origin": deal.get("origin", ""),
            "destination": deal.get("destination", ""),
            "has_data": has_data,
            "is_today": i == 0,
            "is_min": is_min,
        }
    out = []
    for key in sorted(months.keys()):
        m = months[key]
        y, mo = m["year"], m["month"]
        by_day = m["by_day"]
        weeks = cal_mod.monthcalendar(y, mo)
        grid = []
        for w in weeks:
            row = []
            for day_num in w:
                if day_num == 0:
                    row.append({"day": None, "miles": None, "iso": None, "origin": "", "destination": "", "has_data": False, "is_min": False, "is_today": False})
                else:
                    cell = by_day.get(day_num, {})
                    row.append({
                        "day": day_num,
                        "miles": cell.get("miles"),
                        "iso": cell.get("iso"),
                        "origin": cell.get("origin", ""),
                        "destination": cell.get("destination", ""),
                        "has_data": cell.get("has_data", False),
                        "is_min": cell.get("is_min", False),
                        "is_today": cell.get("is_today", False),
                    })
                grid.append(row)
        out.append({"label": m["label"], "year": y, "month": mo, "weeks": grid})
    return out


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
    """Partner Awards home: cards for SAS, Flying Blue, Virgin."""
    return render_template("partner_awards_home.html")


@app.route("/partner-awards/sas")
def partner_awards_sas():
    """Redirect: SAS EuroBonus views/reports are in main nav."""
    return redirect("/")


@app.route("/partner-awards/virgin")
def partner_awards_virgin():
    """Virgin Atlantic dashboard: workflow shell (coming soon)."""
    watch_routes = []
    db_path = _get_partner_db_path()
    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.row_factory = sqlite3.Row
        from partner_awards.airfrance.watchlist import list_watch_routes
        watch_routes = list_watch_routes(conn, "virgin")
        conn.close()
    return render_template("partner_awards_virgin.html", watch_routes=watch_routes)


@_partner_awards_error_handler
@app.route("/partner-awards/virgin/reports", methods=["GET"])
def partner_awards_virgin_reports():
    """Virgin Atlantic reports: placeholder tabs (Coming soon)."""
    tab_param = request.args.get("tab", "summary")
    if tab_param not in ("summary", "year-view", "discovery", "heatmap"):
        tab_param = "summary"
    month_param = request.args.get("month") or __import__("datetime").datetime.now().strftime("%Y-%m")
    cabin_param = request.args.get("cabin", "BUSINESS")

    def _report_url(override_tab=None):
        from urllib.parse import urlencode
        p = {"month": month_param, "cabin": cabin_param, "tab": override_tab or tab_param}
        return "/partner-awards/virgin/reports?" + urlencode(p)

    now = __import__("datetime").datetime.now()
    months_all = []
    y, m = now.year, now.month
    for _ in range(12):
        months_all.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1

    return render_template(
        "partner_awards_virgin_reports.html",
        tab=tab_param,
        month=month_param,
        cabin=cabin_param,
        months_all=months_all,
        report_url=_report_url,
    )


def _get_partner_db_path():
    path = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
        os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")), "partner_awards.sqlite"
    )
    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return path


def _get_runner_config_path():
    """Path to partner_awards_remote_runner/config.json."""
    from pathlib import Path
    return Path(__file__).resolve().parent / "partner_awards_remote_runner" / "config.json"


def _enrich_watch_routes_with_scan_info(conn, watch_routes, month_param):
    """Add last_scan_at and coverage (days/total) per route."""
    import calendar
    try:
        y, m = int(month_param[:4]), int(month_param[5:7])
        month_days_total = calendar.monthrange(y, m)[1]
    except (ValueError, IndexError):
        month_days_total = 31
    enriched = []
    for r in watch_routes:
        row = dict(r)
        row["last_scan_at"] = None
        row["coverage_days"] = 0
        row["coverage_total"] = month_days_total
        cur = conn.execute(
            """SELECT t.finished_at FROM partner_award_job_tasks t
               INNER JOIN partner_award_jobs j ON j.id = t.job_id AND j.program = 'flyingblue'
               WHERE t.origin=? AND t.destination=? AND t.status='done' AND t.finished_at IS NOT NULL
               ORDER BY t.finished_at DESC LIMIT 1""",
            (r["origin"], r["destination"]),
        )
        scan_row = cur.fetchone()
        if scan_row:
            row["last_scan_at"] = scan_row[0]
        cur = conn.execute(
            """SELECT COUNT(DISTINCT depart_date) FROM partner_award_calendar_fares
               WHERE source='AF' AND origin=? AND destination=? AND substr(depart_date,1,7)=?
               AND cabin_class IN ('BUSINESS','PREMIUM')""",
            (r["origin"], r["destination"], month_param),
        )
        cov_row = cur.fetchone()
        if cov_row and cov_row[0] is not None:
            row["coverage_days"] = cov_row[0]
        enriched.append(row)
    return enriched


@_partner_awards_error_handler
@app.route("/partner-awards/flyingblue", methods=["GET"])
def partner_awards_flyingblue():
    """Flying Blue: single page with Routes | Calendar | Windows tabs."""
    import sqlite3
    from datetime import datetime
    from partner_awards.airfrance.watchlist import list_watch_routes
    from partner_awards.airfrance.top_deals import get_top_deals_for_month
    from partner_awards.airfrance.route_discovery import months_present, discovery_multi_origin
    from partner_awards.airfrance.heatmap import build_daily_year_calendar
    from partner_awards.airfrance.windows import get_round_trip_pairs, get_routes_with_data

    db_path = _get_partner_db_path()
    tab_param = request.args.get("tab", "routes")
    if tab_param not in ("routes", "calendar", "windows"):
        tab_param = "routes"
    month_param = request.args.get("month")
    cabin_param = request.args.get("cabin", "BUSINESS")
    route_filter = request.args.get("route", "all")  # calendar: single route only
    # Windows tab
    win_origin = request.args.get("win_origin", "AMS")
    win_dest = request.args.get("win_dest", "")
    win_trip = request.args.get("win_trip", "weekend")  # weekend, 5-7, custom
    win_nights_min = request.args.get("win_nights_min", "3")
    win_nights_max = request.args.get("win_nights_max", "10")

    validation_warning = None
    if month_param and (len(month_param) != 7 or not (month_param[:4].isdigit() and month_param[5:7].isdigit() and month_param[4] == "-")):
        month_param = None
        validation_warning = "Invalid month format; using default."
    if cabin_param not in ("BUSINESS", "PREMIUM"):
        cabin_param = "BUSINESS"
        validation_warning = "Invalid cabin; using BUSINESS."

    watch_routes = []
    enabled_routes = []
    top_deals = []
    suggested_routes = []
    stats = {"global_min_miles": None, "days_at_min": 0, "total_days": 0}
    months_available = []
    latest_job = None
    heatmap = {"days": [], "rows": [], "month": "", "cabin_class": ""}
    flyingblue_year_calendar: list = []
    windows_pairs = []
    origins_available = ["AMS", "PAR"]
    routes_with_data: list[tuple[str, str]] = []
    win_origins: list[str] = []
    win_dests_for_origin: list[str] = []
    calendar_route_options: list[tuple[str, str]] = []

    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.row_factory = sqlite3.Row

        watch_routes = list_watch_routes(conn, "flyingblue")
        enabled_routes = [(r["origin"], r["destination"]) for r in watch_routes if r["enabled"]]

        if enabled_routes:
            route_conds = " OR ".join("(origin=? AND destination=?)" for _ in enabled_routes)
            route_params = [p for pair in enabled_routes for p in pair]
            months_cur = conn.execute(
                f"""SELECT DISTINCT substr(depart_date, 1, 7) as ym
                   FROM partner_award_calendar_fares
                   WHERE source='AF' AND ({route_conds})
                   ORDER BY ym DESC LIMIT 24""",
                route_params,
            )
            months_available = [r[0] for r in months_cur.fetchall() if r[0]]

        if not month_param:
            month_param = months_available[0] if months_available else datetime.now().strftime("%Y-%m")

        # Enrich watchlist with last_scan and coverage (Routes tab)
        watch_routes = _enrich_watch_routes_with_scan_info(conn, watch_routes, month_param)

        if enabled_routes:
            top_deals, stats = get_top_deals_for_month(
                conn, month_param, cabin_param, enabled_routes, limit=5
            )

        # Suggested routes (discovery, limit 5)
        disc_months = months_present(conn, cabin_param, origins_available)[:6]
        if disc_months:
            by_origin = discovery_multi_origin(conn, origins_available, cabin_param, disc_months, limit_per_origin=5)
            flat = []
            for o in origins_available:
                flat.extend(by_origin.get(o, [])[:3])
            flat.sort(key=lambda x: (x.get("best_miles") or 999999, x.get("destination", "")))
            suggested_routes = flat[:5]

        cur = conn.execute(
            """SELECT id, program, job_type, status, created_at, started_at, finished_at,
                      params_json, progress_json, last_error
               FROM partner_award_jobs WHERE program='flyingblue'
               ORDER BY id DESC LIMIT 1"""
        )
        row = cur.fetchone()
        if row:
            latest_job = dict(row)
            try:
                latest_job["progress"] = json.loads(latest_job.get("progress_json") or "{}")
            except Exception:
                latest_job["progress"] = {}

        # Calendar tab: 365-day day-by-day view (single route only)
        if tab_param == "calendar":
            # Use routes that have data in DB (like Windows tab), not just watchlist
            routes_with_calendar_data = get_routes_with_data(conn)
            routes_filtered = [(o, d) for o, d in routes_with_calendar_data]
            if not routes_filtered:
                routes_filtered = enabled_routes if enabled_routes else [(r["origin"], r["destination"]) for r in watch_routes]
            calendar_route_options[:] = [(f"{o}-{d}", f"{o}→{d}") for o, d in routes_filtered]
            # Resolve selected route (single route only, no "all")
            sel_route = None
            if route_filter and "-" in route_filter:
                parts = route_filter.split("-", 1)
                if len(parts) == 2:
                    rf = (parts[0].strip().upper(), parts[1].strip().upper())
                    if rf in routes_filtered:
                        sel_route = rf
            if not sel_route and routes_filtered:
                sel_route = routes_filtered[0]
                route_filter = f"{sel_route[0]}-{sel_route[1]}"
            if sel_route:
                flyingblue_year_calendar = build_daily_year_calendar(
                    conn, cabin_param, sel_route[0], sel_route[1]
                )

        # Windows tab: round-trip pairs + routes for dropdowns
        if tab_param == "windows":
            routes_with_data = get_routes_with_data(conn)
            win_origins = sorted(set(o for o, _ in routes_with_data))
            if win_origins and (not win_origin or win_origin not in win_origins):
                win_origin = win_origins[0]
            win_dests_for_origin = sorted(set(d for o, d in routes_with_data if o == win_origin))
            if win_dests_for_origin and (not win_dest or win_dest not in win_dests_for_origin):
                win_dest = win_dests_for_origin[0]
        if tab_param == "windows" and month_param and win_origin and win_dest:
            if win_trip == "weekend":
                trip_min, trip_max = 2, 4
            elif win_trip == "5-7":
                trip_min, trip_max = 5, 7
            else:
                try:
                    trip_min = max(3, min(10, int(win_nights_min)))
                    trip_max = max(3, min(10, int(win_nights_max)))
                except ValueError:
                    trip_min, trip_max = 3, 10
                if trip_min > trip_max:
                    trip_min, trip_max = trip_max, trip_min
            windows_pairs = get_round_trip_pairs(
                conn, win_origin.strip().upper()[:4], win_dest.strip().upper()[:4],
                cabin_param, month_param, trip_min, trip_max
            )

        conn.close()

    if not month_param:
        month_param = datetime.now().strftime("%Y-%m")

    has_cookies = False
    config_path = _get_runner_config_path()
    if config_path.exists():
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = json.load(f)
            has_cookies = bool(cfg.get("cookie_string") or os.environ.get("AF_COOKIE_STRING"))
        except Exception:
            pass

    blocked_until = None
    try:
        from partner_awards.airfrance.state import is_blocked
        blocked, until = is_blocked()
        if blocked:
            blocked_until = until
    except Exception:
        pass

    now = datetime.now()
    months_all = []
    y, m = now.year, now.month
    for _ in range(12):
        months_all.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    return render_template(
        "partner_awards_flyingblue.html",
        tab=tab_param,
        watch_routes=watch_routes,
        top_deals=top_deals,
        suggested_routes=suggested_routes,
        stats=stats,
        month=month_param,
        cabin=cabin_param,
        months_all=months_all,
        latest_job=latest_job,
        validation_warning=validation_warning,
        has_cookies=has_cookies,
        data_freshness=_flyingblue_data_freshness(db_path, enabled_routes if os.path.exists(db_path) else [], month_param),
        flyingblue_year_calendar=flyingblue_year_calendar,
        route_filter=route_filter,
        calendar_route_options=calendar_route_options,
        routes_with_data=routes_with_data,
        win_origins=win_origins,
        win_dests_for_origin=win_dests_for_origin,
        windows_pairs=windows_pairs,
        win_origin=win_origin,
        win_dest=win_dest,
        win_trip=win_trip,
        win_nights_min=win_nights_min,
        win_nights_max=win_nights_max,
        blocked_until=blocked_until,
    )


def _flyingblue_data_freshness(db_path, enabled_routes, month_param):
    """Return dict: last_scan_at, last_scan_host, routes_24h, total_routes, month_days_captured, month_days_total."""
    import calendar
    out = {"last_scan_at": None, "last_scan_host": None, "routes_24h": 0, "total_routes": len(enabled_routes), "month_days_captured": 0, "month_days_total": 31}
    if not os.path.exists(db_path) or not month_param:
        return out
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            """SELECT COUNT(DISTINCT origin || '-' || destination || '-' || month || '-' || cabin)
               FROM partner_award_job_tasks t
               INNER JOIN partner_award_jobs j ON j.id = t.job_id AND j.program = 'flyingblue'
               WHERE t.status='done' AND t.finished_at > datetime('now', '-24 hours')"""
        )
        out["routes_24h"] = cur.fetchone()[0] or 0
        cur = conn.execute(
            """SELECT t.finished_at FROM partner_award_job_tasks t
               INNER JOIN partner_award_jobs j ON j.id = t.job_id AND j.program = 'flyingblue'
               WHERE t.status='done' ORDER BY t.finished_at DESC LIMIT 1"""
        )
        row = cur.fetchone()
        if row:
            out["last_scan_at"] = row[0]
        cur = conn.execute(
            """SELECT host_used FROM partner_award_calendar_fares
               WHERE source='AF' AND substr(depart_date,1,7)=?
               ORDER BY updated_at DESC LIMIT 1""",
            (month_param,),
        )
        host_row = cur.fetchone()
        if host_row and host_row[0]:
            out["last_scan_host"] = host_row[0]
        try:
            y, m = int(month_param[:4]), int(month_param[5:7])
            out["month_days_total"] = calendar.monthrange(y, m)[1]
        except (ValueError, IndexError):
            pass
        if enabled_routes:
            route_conds = " OR ".join("(origin=? AND destination=?)" for _ in enabled_routes)
            route_params = [p for pair in enabled_routes for p in pair]
            cur = conn.execute(
                f"""SELECT COUNT(DISTINCT depart_date) FROM partner_award_calendar_fares
                    WHERE source='AF' AND cabin_class IN ('BUSINESS','PREMIUM') AND substr(depart_date,1,7)=?
                    AND ({route_conds})""",
                [month_param] + route_params,
            )
            out["month_days_captured"] = cur.fetchone()[0] or 0
        conn.close()
    except Exception:
        pass
    return out




@app.route("/partner-awards/self-test")
def partner_awards_self_test():
    """Read-only self-test: DB, watchlist CRUD, calendar, top deals, heatmap, delta/telegram."""
    db_path = _get_partner_db_path()
    checks = []
    diag = {"db_path": db_path, "checks": []}

    # 1) DB reachable, tables exist
    try:
        if not os.path.exists(db_path):
            checks.append({"name": "DB reachable", "status": "FAIL", "msg": "DB file not found"})
            diag["checks"].append({"name": "DB", "status": "FAIL", "msg": "not found"})
        else:
            from partner_awards.airfrance.adapter import init_db
            conn = sqlite3.connect(db_path)
            init_db(conn)
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('partner_award_watch_routes','partner_award_calendar_fares')"
            )
            tables = {r[0] for r in cur.fetchall()}
            conn.close()
            if tables == {"partner_award_watch_routes", "partner_award_calendar_fares"}:
                checks.append({"name": "DB + tables", "status": "PASS", "msg": "OK"})
            else:
                checks.append({"name": "DB + tables", "status": "FAIL", "msg": f"Missing: {tables}"})
    except Exception as e:
        checks.append({"name": "DB + tables", "status": "FAIL", "msg": str(e)[:200]})
        diag["checks"].append({"name": "DB", "status": "FAIL", "msg": str(e)[:200]})

    # 2) Watchlist CRUD
    try:
        from partner_awards.airfrance.adapter import init_db
        from partner_awards.airfrance.watchlist import list_watch_routes, upsert_watch_route, set_watch_route_enabled
        conn = sqlite3.connect(db_path)
        init_db(conn)
        before = len(list_watch_routes(conn, "flyingblue"))
        rid = upsert_watch_route(conn, "flyingblue", "ZZZ", "YYY", enabled=1)
        after = len(list_watch_routes(conn, "flyingblue"))
        set_watch_route_enabled(conn, rid, 0)
        set_watch_route_enabled(conn, rid, 1)
        set_watch_route_enabled(conn, rid, 0)
        conn.close()
        checks.append({"name": "Watchlist CRUD", "status": "PASS", "msg": f"insert/toggle OK (id={rid})"})
    except Exception as e:
        checks.append({"name": "Watchlist CRUD", "status": "FAIL", "msg": str(e)[:200]})

    # 3) Calendar fares
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT origin, destination, substr(depart_date,1,7) as ym, cabin_class, COUNT(*) FROM partner_award_calendar_fares WHERE source='AF' GROUP BY origin, destination, substr(depart_date,1,7), cabin_class ORDER BY origin, destination, ym LIMIT 20"
        )
        rows = cur.fetchall()
        conn.close()
        if rows:
            summary = "; ".join(f"{r[0]}→{r[1]} {r[2]} {r[4]}d" for r in rows[:5])
            if len(rows) > 5:
                summary += f" … (+{len(rows)-5})"
            checks.append({"name": "Calendar fares", "status": "PASS", "msg": summary})
        else:
            checks.append({"name": "Calendar fares", "status": "SKIP", "msg": "No rows. Run batch scan + import."})
    except Exception as e:
        checks.append({"name": "Calendar fares", "status": "FAIL", "msg": str(e)[:200]})

    # 4) Top deals
    try:
        from partner_awards.airfrance.top_deals import get_top_deals_for_month
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT origin, destination, substr(depart_date,1,7), cabin_class FROM partner_award_calendar_fares WHERE source='AF' AND cabin_class IN ('BUSINESS','PREMIUM') LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            routes = [(row[0], row[1])]
            rows, stats = get_top_deals_for_month(conn, row[2], row[3], routes, limit=5)
            conn.close()
            checks.append({"name": "Top deals", "status": "PASS", "msg": f"{len(rows)} rows, min={stats.get('global_min_miles')}"})
        else:
            conn.close()
            checks.append({"name": "Top deals", "status": "SKIP", "msg": "No calendar data"})
    except Exception as e:
        checks.append({"name": "Top deals", "status": "FAIL", "msg": str(e)[:200]})

    # 5) Heatmap
    try:
        from partner_awards.airfrance.heatmap import build_heatmap
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT origin, destination, substr(depart_date,1,7), cabin_class FROM partner_award_calendar_fares WHERE source='AF' LIMIT 1"
        )
        row = cur.fetchone()
        if row:
            routes = [(row[0], row[1])]
            hm = build_heatmap(conn, row[2], row[3], routes)
            conn.close()
            ok = isinstance(hm.get("days"), list) and isinstance(hm.get("rows"), list)
            checks.append({"name": "Heatmap", "status": "PASS" if ok else "FAIL", "msg": f"days={len(hm.get('days',[]))} rows={len(hm.get('rows',[]))}"})
        else:
            conn.close()
            checks.append({"name": "Heatmap", "status": "SKIP", "msg": "No data"})
    except Exception as e:
        checks.append({"name": "Heatmap", "status": "FAIL", "msg": str(e)[:200]})

    # 6) Delta/Telegram
    try:
        from partner_awards.airfrance.calendar_delta import get_scan_runs_for_month, compute_month_delta, build_telegram_month_text
        from partner_awards.airfrance.calendar_delta import get_month_fares_by_scan_run
        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            "SELECT origin, destination, substr(depart_date,1,7), cabin_class FROM partner_award_calendar_fares WHERE source='AF' LIMIT 5"
        )
        rows = cur.fetchall()
        tested = False
        for r in rows:
            runs = get_scan_runs_for_month(conn, r[0], r[1], r[3], r[2])
            if len(runs) >= 2:
                latest, prev = runs[0], runs[1]
                lm = get_month_fares_by_scan_run(conn, latest["scan_run_id"], r[0], r[1], r[3], r[2])
                pm = get_month_fares_by_scan_run(conn, prev["scan_run_id"], r[0], r[1], r[3], r[2])
                delta = compute_month_delta(lm, pm)
                text = build_telegram_month_text(r[0], r[1], r[2], r[3], delta, latest, prev_missing=False)
                tested = True
                checks.append({"name": "Delta/Telegram", "status": "PASS", "msg": "OK"})
                break
        conn.close()
        if not tested:
            checks.append({"name": "Delta/Telegram", "status": "SKIP", "msg": "Need 2+ scans for same route/month/cabin"})
    except Exception as e:
        checks.append({"name": "Delta/Telegram", "status": "FAIL", "msg": str(e)[:200]})

    diag["checks"] = [{"name": c["name"], "status": c["status"], "msg": c["msg"]} for c in checks]
    return render_template("partner_awards_self_test.html", checks=checks, diag_json=json.dumps(diag, indent=2))


def _extract_cookie_string_from_paste(paste: str) -> str | None:
    """Extract cookie string from pasted full cURL, Cookie-Editor JSON, or return None if already plain cookies."""
    import re
    paste = paste.strip()
    if not paste:
        return None
    # Full cURL: -b '...' or -b "..." — use [^']* for single-quoted (cookie can contain " from %22)
    for pattern in [
        r"-b\s+'([^']*)'",
        r'-b\s+"((?:[^"\\]|\\.)*)"',
        r"--cookie\s+'([^']*)'",
        r'--cookie\s+"((?:[^"\\]|\\.)*)"',
        r"-H\s+['\"]Cookie:\s*([^'\"]+)['\"]",
    ]:
        m = re.search(pattern, paste, re.IGNORECASE | re.DOTALL)
        if m and m.group(1).strip():
            return m.group(1).strip()
    # Cookie-Editor JSON: [{"name":"x","value":"y",...}, ...]
    if paste.startswith("[") and '"name"' in paste and '"value"' in paste:
        try:
            arr = json.loads(paste)
            parts = []
            for c in arr if isinstance(arr, list) else []:
                if isinstance(c, dict) and c.get("name") and c.get("value"):
                    parts.append(f"{c['name']}={c['value']}")
            if parts:
                return "; ".join(parts)
        except json.JSONDecodeError:
            pass
    # Assume plain cookie string (name=val; name=val)
    if "=" in paste:
        return paste
    return None


@app.route("/partner-awards/flyingblue/cookies", methods=["POST"])
def partner_awards_flyingblue_cookies():
    """Save Flying Blue cookie string to runner config. Form: cookie_string (or empty to clear).
    Accepts: plain cookies, full cURL (Copy as cURL), or Cookie-Editor JSON.
    Returns JSON when X-Requested-With: XMLHttpRequest (for fetch, no reload)."""
    config_path = _get_runner_config_path()
    raw = (request.form.get("cookie_string") or "").strip()
    cookie_string = _extract_cookie_string_from_paste(raw) if raw else ""
    is_xhr = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if raw and not cookie_string:
        if is_xhr:
            return jsonify({"ok": False, "message": "Could not extract cookies. Paste the full cURL (Copy as cURL) or a cookie string."})
        flash("Could not extract cookies. Paste the full cURL (Copy as cURL) or a cookie string.", "error")
        return redirect(request.referrer or "/partner-awards/flyingblue")
    try:
        config = {}
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                config = json.load(f)
        if cookie_string:
            config["cookie_string"] = cookie_string
            # Infer host from curl URL so runner uses matching domain (klm.se vs airfrance.us)
            import re as _re
            url_m = _re.search(r"curl\s+['\"]?(https?://[^'\s\"]+)['\"]?", raw)
            if url_m:
                from urllib.parse import urlparse
                netloc = (urlparse(url_m.group(1)).netloc or "").lower()
                config["cookie_prefer_host"] = "AF-US" if "airfrance" in netloc else "KLM-SE"
        else:
            config.pop("cookie_string", None)
            config.pop("cookie_prefer_host", None)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
        msg = "Flying Blue cookies saved." if cookie_string else "Flying Blue cookies cleared."
        if is_xhr:
            return jsonify({"ok": True, "message": msg, "has_cookies": bool(cookie_string)})
        flash(msg, "success")
    except OSError as e:
        if is_xhr:
            return jsonify({"ok": False, "message": str(e)})
        flash(f"Could not save config: {e}", "error")
    except json.JSONDecodeError as e:
        if is_xhr:
            return jsonify({"ok": False, "message": str(e)})
        flash(f"Invalid config: {e}", "error")
    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/flyingblue/cookies/test", methods=["POST"])
def partner_awards_flyingblue_cookies_test():
    """Run a quick AMS-CPT scan to test if cookies work. Returns JSON {ok, message, connections_count, debug}."""
    import subprocess
    from pathlib import Path
    runner_dir = Path(__file__).resolve().parent / "partner_awards_remote_runner"
    if not (runner_dir / "runner.py").exists():
        return jsonify({"ok": False, "message": "Runner not found"})
    try:
        result = subprocess.run(
            [sys.executable, "runner.py", "open-dates-month", "--origin", "AMS", "--destination", "CPT",
             "--month", "2026-03", "--cabins", "BUSINESS"],
            cwd=str(runner_dir),
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parent)},
        )
        out_path = runner_dir / "outputs" / "AF" / "AMS-CPT" / "2026-03"
        connections = 0
        debug_lines = []
        if out_path.exists():
            files = sorted(out_path.glob("lowest_fares_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            for f in files[:1]:
                try:
                    data = json.load(open(f, encoding="utf-8"))
                    body = data.get("body") or data
                    inner = body.get("data") or {}
                    offers = inner.get("lowestFareOffers") or {}
                    conns = offers.get("connections") or {}
                    lowest = offers.get("lowestOffers") or []
                    connections = (
                        len(conns) if isinstance(conns, dict) and conns else 0
                    ) or (len(lowest) if isinstance(lowest, list) else 0)
                    if connections == 0 and body:
                        errors = body.get("errors") or []
                        if errors:
                            debug_lines.append("API errors: " + str(errors)[:300])
                        else:
                            debug_lines.append("Response has no connections/lowestOffers")
                except Exception as ex:
                    debug_lines.append(f"Parse error: {ex}")
                break
        else:
            debug_lines.append("No output file written")
        if result.returncode != 0:
            err = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
            debug_lines.append(err[:500] if err else f"Runner exit code {result.returncode}")
        elif result.stderr and not debug_lines:
            debug_lines.append(result.stderr.strip()[:300])

        ok = connections > 0
        debug_str = "; ".join(debug_lines) if debug_lines else None
        if ok:
            msg = f"Got {connections} days of data"
        elif debug_str:
            msg = f"No data. {debug_str}"
        else:
            msg = "No data. Cookies may have expired or the API returned empty."
        return jsonify({
            "ok": ok,
            "message": msg,
            "connections_count": connections,
            "debug": debug_str,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "message": "Test timed out (60s)"})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)[:200]})


@app.route("/partner-awards/flyingblue/clear-block", methods=["POST"])
def partner_awards_flyingblue_clear_block():
    """Clear AF/KLM cooldown block so scans can resume."""
    try:
        from partner_awards.airfrance.state import clear_blocked
        clear_blocked()
        flash("Block cleared. Scans can run again.")
    except Exception as e:
        flash(f"Failed to clear block: {e}", "error")
    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/flyingblue/run-batch", methods=["POST"])
def partner_awards_flyingblue_run_batch():
    """Create queued open_dates_batch job for enabled watchlist routes. Redirect back."""
    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import list_watch_routes

    conn = sqlite3.connect(db_path)
    init_db(conn)
    watch_routes = list_watch_routes(conn, "flyingblue")
    enabled_count = sum(1 for r in watch_routes if r["enabled"])
    conn.close()

    if enabled_count == 0:
        flash("Enable at least one route in the watchlist to run a scan.", "error")
        return redirect(request.referrer or "/partner-awards/flyingblue")

    force_refresh = request.form.get("force_refresh") in ("1", "true", "on", "yes")
    conn = sqlite3.connect(db_path)
    init_db(conn)
    params = {"months": 12, "cabins": ["BUSINESS", "PREMIUM"], "force_refresh": force_refresh}
    progress = {"total_tasks": 0, "done_tasks": 0, "skipped_tasks": 0, "current_task": None}
    conn.execute(
        """INSERT INTO partner_award_jobs (program, job_type, status, params_json, progress_json)
           VALUES ('flyingblue', 'open_dates_batch', 'queued', ?, ?)""",
        (json.dumps(params), json.dumps(progress)),
    )
    conn.commit()
    conn.close()

    # Start worker in background so the job is processed without running a separate terminal
    project_root = Path(__file__).resolve().parent
    try:
        subprocess.Popen(
            [sys.executable, "-m", "partner_awards.jobs_worker"],
            cwd=str(project_root),
            env=os.environ.copy(),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass  # If spawn fails, user can still run the worker manually

    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/flyingblue/status", methods=["GET"])
def partner_awards_flyingblue_status():
    """JSON: latest Flying Blue job progress for status poller."""
    db_path = _get_partner_db_path()
    out = {"job": None}
    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        cur = conn.execute(
            """SELECT id, status, progress_json, last_error, started_at, finished_at
               FROM partner_award_jobs WHERE program='flyingblue'
               ORDER BY id DESC LIMIT 1"""
        )
        row = cur.fetchone()
        conn.close()
        if row:
            try:
                progress = json.loads(row[2] or "{}")
            except Exception:
                progress = {}
            out["job"] = {
                "id": row[0],
                "status": row[1],
                "progress": progress,
                "last_error": row[3],
                "started_at": row[4],
                "finished_at": row[5],
            }
    return jsonify(out)


@app.route("/partner-awards/flyingblue/ingest", methods=["GET"])
def partner_awards_flyingblue_ingest():
    """Flying Blue import & test ingest (legacy AF/KLM page)."""
    return render_template("partner_awards.html")


@app.route("/partner-awards/watchlist/seed-recommended", methods=["POST"])
def partner_awards_watchlist_seed_recommended():
    """Add recommended routes to watchlist. Adds missing; keeps existing. Redirect back."""
    import json
    rec_path = Path(__file__).resolve().parent / "partner_awards_remote_runner" / "recommended_routes.json"
    if not rec_path.exists():
        flash("recommended_routes.json not found", "error")
        return redirect(request.referrer or "/partner-awards/flyingblue")
    try:
        with open(rec_path, encoding="utf-8") as f:
            data = json.load(f)
        routes = data.get("routes") or []
    except Exception as e:
        flash(f"Invalid recommended_routes.json: {e}", "error")
        return redirect(request.referrer or "/partner-awards/flyingblue")
    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import upsert_watch_route, list_watch_routes
    conn = sqlite3.connect(db_path)
    init_db(conn)
    existing = {(r["origin"], r["destination"]) for r in list_watch_routes(conn, "flyingblue")}
    added = 0
    for item in routes:
        if not isinstance(item, dict):
            continue
        o, d = item.get("origin"), item.get("destination")
        if not o or not d or (o, d) in existing:
            if o and d:
                existing.add((o, d))
            continue
        try:
            upsert_watch_route(conn, "flyingblue", o, d, enabled=1, include_returns=1)
            added += 1
            existing.add((o, d))
        except ValueError:
            pass
    conn.close()
    flash(f"Added {added} routes from recommended list. Total watchlist: {len(existing)}.", "success" if added else None)
    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/watchlist/add", methods=["POST"])
def partner_awards_watchlist_add():
    """Add route to watchlist. Form: program, origin, destination. Redirect back."""
    program = request.form.get("program", "").strip().lower() or "flyingblue"
    origin = request.form.get("origin", "").strip()
    destination = request.form.get("destination", "").strip()

    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import upsert_watch_route
    conn = sqlite3.connect(db_path)
    init_db(conn)
    try:
        upsert_watch_route(conn, program, origin, destination, enabled=1, include_returns=1)
    except ValueError as e:
        flash(str(e), "error")
    finally:
        conn.close()

    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/watchlist/remove", methods=["POST"])
def partner_awards_watchlist_remove():
    """Remove route from watchlist. Form: id. Redirect back."""
    route_id = request.form.get("id")
    try:
        route_id = int(route_id)
    except (TypeError, ValueError):
        return redirect(request.referrer or "/partner-awards/flyingblue")

    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import delete_watch_route
    conn = sqlite3.connect(db_path)
    init_db(conn)
    delete_watch_route(conn, route_id)
    conn.close()

    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/watchlist/toggle", methods=["POST"])
def partner_awards_watchlist_toggle():
    """Toggle watchlist route enabled. Form: id, enabled (0/1). Redirect back."""
    route_id = request.form.get("id")
    enabled = 1 if str(request.form.get("enabled", "1")).strip() in ("1", "true", "on", "yes") else 0
    try:
        route_id = int(route_id)
    except (TypeError, ValueError):
        return redirect(request.referrer or "/partner-awards/flyingblue")

    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import set_watch_route_enabled
    conn = sqlite3.connect(db_path)
    init_db(conn)
    set_watch_route_enabled(conn, route_id, enabled)
    conn.close()

    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/watchlist/toggle-returns", methods=["POST"])
def partner_awards_watchlist_toggle_returns():
    """Toggle watchlist route include_returns. Form: id, include_returns (0/1). Redirect back."""
    route_id = request.form.get("id")
    include_returns = 1 if str(request.form.get("include_returns", "1")).strip() in ("1", "true", "on", "yes") else 0
    try:
        route_id = int(route_id)
    except (TypeError, ValueError):
        return redirect(request.referrer or "/partner-awards/flyingblue")

    db_path = _get_partner_db_path()
    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.watchlist import set_watch_route_include_returns
    conn = sqlite3.connect(db_path)
    init_db(conn)
    set_watch_route_include_returns(conn, route_id, include_returns)
    conn.close()

    return redirect(request.referrer or "/partner-awards/flyingblue")


@app.route("/partner-awards/jobs")
def partner_awards_jobs():
    """List batch jobs for Flying Blue."""
    db_path = _get_partner_db_path()
    jobs = []
    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT id, program, job_type, status, created_at, started_at, finished_at,
                      params_json, progress_json, last_error
               FROM partner_award_jobs WHERE program='flyingblue'
               ORDER BY id DESC LIMIT 50"""
        )
        jobs = [dict(r) for r in cur.fetchall()]
        for j in jobs:
            try:
                j["progress"] = json.loads(j.get("progress_json") or "{}")
            except Exception:
                j["progress"] = {}
        conn.close()
    return render_template("partner_awards_jobs.html", jobs=jobs)


@app.route("/partner-awards/jobs/<int:job_id>")
def partner_awards_job_detail(job_id):
    """Job detail with tasks."""
    db_path = _get_partner_db_path()
    job = None
    tasks = []
    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT id, program, job_type, status, created_at, started_at, finished_at,
                      params_json, progress_json, last_error
               FROM partner_award_jobs WHERE id=?""",
            (job_id,),
        )
        row = cur.fetchone()
        if row:
            job = dict(row)
            try:
                job["progress"] = json.loads(job.get("progress_json") or "{}")
            except Exception:
                job["progress"] = {}
            cur = conn.execute(
                """SELECT id, origin, destination, month, cabin, status, started_at, finished_at, last_error
                   FROM partner_award_job_tasks WHERE job_id=? ORDER BY id""",
                (job_id,),
            )
            tasks = [dict(r) for r in cur.fetchall()]
        conn.close()
    return render_template("partner_awards_job_detail.html", job=job, tasks=tasks)


@app.route("/partner-awards/dashboard")
def partner_awards_dashboard():
    """Removed: redirect to Partner overview."""
    return redirect("/partner-awards")


def _format_miles_k(miles: int | None) -> str:
    """Format miles as 85.0k, 199.5k, etc."""
    if miles is None:
        return "—"
    if miles >= 1000:
        k = miles / 1000
        return f"{k:.1f}k"
    return str(miles)


@_partner_awards_error_handler
@app.route("/partner-awards/calendar")
def partner_awards_calendar():
    """Partner Awards calendar fares (from LowestFareOffers). Supports month grid view."""
    import calendar as cal_mod
    import sqlite3
    import os
    from datetime import datetime, timedelta

    db_path = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
        os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")), "partner_awards.sqlite"
    )
    origin = request.args.get("origin", "AMS")
    destination = request.args.get("destination", "JNB")
    cabin_param = request.args.get("cabin", "BUSINESS")
    month_param = request.args.get("month")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    host_filter = request.args.get("host_used")
    view_mode = request.args.get("view_mode", "best")

    rows = []
    month_grid = None
    min_miles = None
    ingest_types = []
    last_run = None
    blocked_until = None
    routes_available = []
    months_available = []

    if os.path.exists(db_path):
        from partner_awards.airfrance.adapter import init_db
        conn = sqlite3.connect(db_path)
        init_db(conn)
        conn.row_factory = sqlite3.Row

        conditions = ["source='AF'", "origin=?", "destination=?"]
        params = [origin, destination]
        if view_mode == "per_host" and host_filter:
            conditions.append("host_used=?")
            params.append(host_filter)

        cur = conn.execute(
            f"""SELECT depart_date, cabin_class, miles, tax, host_used
               FROM partner_award_calendar_fares
               WHERE {" AND ".join(conditions)}
               ORDER BY depart_date, cabin_class""",
            params,
        )
        raw = [dict(r) for r in cur.fetchall()]

        if view_mode == "best" and raw:
            by_key = {}
            for r in raw:
                k = (r["depart_date"], r["cabin_class"])
                if k not in by_key or (r["miles"] or 999999) < (by_key[k].get("miles") or 999999):
                    by_key[k] = r
            raw = list(by_key.values())

        months_cur = conn.execute(
            """SELECT DISTINCT substr(depart_date, 1, 7) as ym
               FROM partner_award_calendar_fares
               WHERE source='AF' AND origin=? AND destination=?
               ORDER BY ym DESC LIMIT 12""",
            (origin, destination),
        )
        months_available = [r[0] for r in months_cur.fetchall() if r[0]]

        if not month_param:
            if start_date:
                raw = [r for r in raw if r["depart_date"] >= start_date]
            if end_date:
                raw = [r for r in raw if r["depart_date"] <= end_date]

        if month_param:
            month_str = (str(month_param).strip() + "-01")[:7] if month_param else ""
            try:
                if not month_str or len(month_str) < 7:
                    raise ValueError("Invalid month")
                year, m = int(month_str[:4]), int(month_str[5:7])
                start_m = f"{year:04d}-{m:02d}-01"
                last_d = cal_mod.monthrange(year, m)[1]
                end_m = f"{year:04d}-{m:02d}-{last_d:02d}"
                month_raw = [r for r in raw if start_m <= r["depart_date"] <= end_m and r["cabin_class"] == cabin_param]
                by_day = {}
                for r in month_raw:
                    d = int(r["depart_date"][8:10])
                    by_day[d] = r["miles"]
                min_miles = min((m for m in by_day.values() if m is not None), default=None)
                first_weekday = cal_mod.weekday(year, m, 1)
                weeks = cal_mod.monthcalendar(year, m)
                grid = []
                for w in weeks:
                    row = []
                    for d in w:
                        if d == 0:
                            row.append({"day": None, "miles": None})
                        else:
                            mi = by_day.get(d)
                            is_min = min_miles is not None and mi == min_miles
                            row.append({"day": d, "miles": mi, "is_min": is_min, "formatted": _format_miles_k(mi)})
                    grid.append(row)
                month_grid = {"year": year, "month": m, "month_name": datetime(year, m, 1).strftime("%B %Y"), "weeks": grid}
            except (ValueError, IndexError):
                month_param = None

        if not month_param:
            by_date = {}
            for r in raw:
                d = r["depart_date"]
                if d not in by_date:
                    by_date[d] = {"date": d, "ECONOMY": None, "PREMIUM": None, "BUSINESS": None}
                cab = r["cabin_class"]
                by_date[d][cab] = {"miles": r["miles"], "tax": r["tax"]}
            rows = sorted(by_date.values(), key=lambda x: x["date"])

        best_eco = min((r["ECONOMY"]["miles"] for r in rows if r.get("ECONOMY") and r["ECONOMY"].get("miles")), default=None)
        best_biz = min((r["BUSINESS"]["miles"] for r in rows if r.get("BUSINESS") and r["BUSINESS"].get("miles")), default=None)
        if month_grid and min_miles is not None:
            best_eco = best_biz = min_miles if cabin_param in ("ECONOMY", "BUSINESS") else (best_eco or best_biz)

        ingest_types = list(dict.fromkeys((r.get("host_used") or "unknown") for r in raw))
        if not ingest_types and raw:
            ingest_types = list(dict.fromkeys((r.get("host_used") or "") for r in raw))

        last_run_cur = conn.execute(
            """SELECT s.id, s.started_at, s.host_used, s.origin, s.destination
               FROM partner_award_scan_runs s
               WHERE s.source='AF' AND s.ingest_type='remote_runner'
                 AND s.origin=? AND s.destination=?
               ORDER BY s.started_at DESC LIMIT 1""",
            (origin, destination),
        )
        lr = last_run_cur.fetchone()
        if lr:
            last_run = {"id": lr[0], "started_at": lr[1], "host_used": lr[2], "origin": lr[3], "destination": lr[4]}

        routes_cur = conn.execute(
            """SELECT DISTINCT origin, destination FROM partner_award_calendar_fares
               WHERE source='AF' ORDER BY origin, destination LIMIT 50"""
        )
        routes_available = [{"origin": r[0], "destination": r[1]} for r in routes_cur.fetchall()]
        conn.close()

    try:
        from partner_awards.airfrance.state import is_blocked
        blocked, until = is_blocked()
        if blocked:
            blocked_until = until
    except Exception:
        pass

    min_display = f"{min_miles:,}" if min_miles is not None else "—"

    return render_template(
        "partner_awards_calendar.html",
        rows=rows,
        month_grid=month_grid,
        min_miles=min_miles,
        min_miles_formatted=_format_miles_k(min_miles) if min_miles else "—",
        min_miles_display=min_display,
        best_eco_miles=best_eco,
        best_biz_miles=best_biz,
        origin=origin,
        destination=destination,
        cabin=cabin_param,
        month=month_param,
        ingest_types=ingest_types,
        start_date=start_date,
        end_date=end_date,
        host_used=host_filter,
        view_mode=view_mode,
        last_run=last_run,
        blocked_until=blocked_until,
        routes_available=routes_available,
        months_available=months_available,
    )


@_partner_awards_error_handler
@app.route("/partner-awards/calendar/delta")
def partner_awards_calendar_delta():
    """Month delta: latest vs previous scan. Query: origin, destination, month, cabin."""
    import os
    import sqlite3
    origin = request.args.get("origin", "AMS")
    destination = request.args.get("destination", "JNB")
    month = request.args.get("month", "2026-03")
    cabin = request.args.get("cabin", "BUSINESS")

    db_path = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
        os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")), "partner_awards.sqlite"
    )
    out = {
        "ok": False,
        "origin": origin,
        "destination": destination,
        "month": month,
        "cabin": cabin,
        "latest_scan_run_id": None,
        "prev_scan_run_id": None,
        "host_used_latest": None,
        "ingest_type_latest": None,
        "count_days_with_data_latest": 0,
        "min_miles_latest": None,
        "min_dates_latest": [],
        "changed_dates": [],
        "new_dates": [],
        "removed_dates": [],
        "biggest_drops": [],
        "biggest_increases": [],
        "expensive_days": [],
    }

    if not os.path.exists(db_path):
        out["error"] = "DB not found"
        return jsonify(out)

    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.calendar_delta import (
        get_scan_runs_for_month,
        get_month_fares_by_scan_run,
        compute_month_delta,
    )

    conn = sqlite3.connect(db_path)
    init_db(conn)
    runs = get_scan_runs_for_month(conn, origin, destination, cabin, month)
    conn.close()

    if not runs:
        out["error"] = "No scan data for this route/month/cabin"
        return jsonify(out)

    latest = runs[0]
    prev = runs[1] if len(runs) > 1 else None
    out["latest_scan_run_id"] = latest["scan_run_id"]
    out["prev_scan_run_id"] = prev["scan_run_id"] if prev else None
    out["host_used_latest"] = latest["host_used"]
    out["ingest_type_latest"] = latest["ingest_type"]

    conn = sqlite3.connect(db_path)
    latest_map = get_month_fares_by_scan_run(conn, latest["scan_run_id"], origin, destination, cabin, month)
    prev_map = get_month_fares_by_scan_run(conn, prev["scan_run_id"], origin, destination, cabin, month) if prev else {}
    conn.close()

    delta = compute_month_delta(latest_map, prev_map)
    out["count_days_with_data_latest"] = delta["count_days_with_data_latest"]
    out["min_miles_latest"] = delta["min_miles_latest"]
    out["min_dates_latest"] = delta["min_dates_latest"]
    out["changed_dates"] = delta["changed_dates"]
    out["new_dates"] = delta["new_dates"]
    out["removed_dates"] = delta["removed_dates"]
    out["biggest_drops"] = delta["biggest_drops"]
    out["biggest_increases"] = delta["biggest_increases"]
    out["expensive_days"] = [{"date": d, "miles": m} for d, m in delta["expensive_days"]]
    out["ok"] = True
    return jsonify(out)


@_partner_awards_error_handler
@app.route("/partner-awards/calendar/telegram")
def partner_awards_calendar_telegram():
    """Telegram-ready text for month. Query: origin, destination, month, cabin."""
    import os
    import sqlite3
    origin = request.args.get("origin", "AMS")
    destination = request.args.get("destination", "JNB")
    month = request.args.get("month", "2026-03")
    cabin = request.args.get("cabin", "BUSINESS")

    db_path = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
        os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")), "partner_awards.sqlite"
    )

    if not os.path.exists(db_path):
        return jsonify({"ok": False, "message": "DB not found"})

    from partner_awards.airfrance.adapter import init_db
    from partner_awards.airfrance.calendar_delta import (
        get_scan_runs_for_month,
        get_month_fares_by_scan_run,
        compute_month_delta,
        build_telegram_month_text,
    )

    conn = sqlite3.connect(db_path)
    init_db(conn)
    runs = get_scan_runs_for_month(conn, origin, destination, cabin, month)
    if not runs:
        conn.close()
        return jsonify({"ok": False, "message": "No scan data for this route/month/cabin"})

    latest = runs[0]
    prev = runs[1] if len(runs) > 1 else None
    latest_map = get_month_fares_by_scan_run(conn, latest["scan_run_id"], origin, destination, cabin, month)
    prev_map = get_month_fares_by_scan_run(conn, prev["scan_run_id"], origin, destination, cabin, month) if prev else {}
    conn.close()

    delta = compute_month_delta(latest_map, prev_map)
    text = build_telegram_month_text(
        origin, destination, month, cabin,
        delta, latest,
        prev_missing=prev is None,
    )
    return jsonify({"ok": True, "text": text})


# Reference values from KLM screenshot (AMS→JNB, March 2026, Business)
_VERIFY_AMS_JNB_2026_03_BUSINESS = {
    2: 111000, 3: 85000, 4: 85000, 5: 85000, 6: 85000, 7: 222000, 8: 222000,
    9: 85000, 10: 85000, 11: 85000, 12: 85000, 13: 85000, 14: 111000, 15: 222000,
    16: 85000, 17: 85000, 18: 85000, 19: 85000, 20: 114000, 21: 114000, 22: 222000,
    23: 222000, 24: 85000, 25: 85000, 26: 114000, 27: 222000, 28: 222000, 29: 199500,
    30: 85000, 31: 85000,
}


@app.route("/partner-awards/calendar/verify")
def partner_awards_calendar_verify():
    """Verify DB matches expected values (e.g. KLM screenshot). Returns JSON."""
    import sqlite3
    import os
    origin = request.args.get("origin", "AMS")
    destination = request.args.get("destination", "JNB")
    month = request.args.get("month", "2026-03")
    cabin = request.args.get("cabin", "BUSINESS")

    db_path = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
        os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards")), "partner_awards.sqlite"
    )
    result = {"ok": False, "mismatches": [], "found_count": 0, "min_miles": None, "host_used_latest": None}

    if origin == "AMS" and destination == "JNB" and month == "2026-03" and cabin == "BUSINESS":
        expected = _VERIFY_AMS_JNB_2026_03_BUSINESS
    else:
        expected = {}

    if not os.path.exists(db_path):
        result["error"] = "DB not found"
        return jsonify(result)

    from partner_awards.airfrance.adapter import init_db
    import calendar as _cal
    try:
        y, m = (int(month[:4]), int(month[5:7])) if len(month) >= 7 else (2026, 3)
        last_d = _cal.monthrange(y, m)[1]
    except (ValueError, IndexError):
        last_d = 31
    end_date_str = f"{month}-{last_d:02d}" if len(month) >= 7 else f"{month}-31"

    conn = sqlite3.connect(db_path)
    init_db(conn)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """SELECT depart_date, miles, host_used FROM partner_award_calendar_fares
           WHERE source='AF' AND origin=? AND destination=? AND cabin_class=?
           AND depart_date >= ? AND depart_date <= ?""",
        (origin, destination, cabin, f"{month}-01", end_date_str),
    )
    rows = {int(r["depart_date"][8:10]): {"miles": r["miles"], "host_used": r["host_used"]} for r in cur.fetchall()}
    host_cur = conn.execute(
        """SELECT host_used FROM partner_award_scan_runs
           WHERE source='AF' AND origin=? AND destination=? ORDER BY started_at DESC LIMIT 1""",
        (origin, destination),
    )
    host_row = host_cur.fetchone()
    conn.close()

    result["found_count"] = len(rows)
    result["min_miles"] = min((m["miles"] for m in rows.values() if m["miles"] is not None), default=None)
    result["host_used_latest"] = host_row[0] if host_row else None

    if expected:
        for day, exp_miles in expected.items():
            got = rows.get(day, {}).get("miles")
            if got != exp_miles:
                result["mismatches"].append({"day": day, "expected": exp_miles, "got": got})
        result["ok"] = len(result["mismatches"]) == 0 and result["min_miles"] == 85000
    else:
        result["ok"] = result["found_count"] > 0

    return jsonify(result)


@app.route("/all")
@app.route("/business")
@app.route("/plus")
def _removed_sas_views():
    """Removed: redirect to dashboard."""
    return redirect("/")


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
    """Removed: redirect to dashboard."""
    return redirect("/")


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
    """Removed: reports moved to dashboard."""
    return redirect("/")


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
