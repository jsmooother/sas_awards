"""
Partner Awards page routes: hub, Flying Blue dashboard, watchlist, jobs, calendar.
"""

from __future__ import annotations

import json
import os
import re
import requests
from datetime import datetime, timedelta
from pathlib import Path

from flask import Blueprint, request, render_template, redirect, url_for, flash, jsonify

from partner_awards.airfrance.adapter import init_db
from partner_awards.airfrance.routes import get_partner_conn, PARTNER_DB_DIR, PARTNER_DB_PATH
from partner_awards.airfrance.watchlist import (
    list_watch_routes,
    upsert_watch_route,
    set_watch_route_enabled,
    delete_watch_route,
)
from partner_awards.airfrance.state import read_state, write_state, is_blocked, clear_blocked
from partner_awards.airfrance.heatmap import build_daily_year_calendar
from partner_awards.airfrance.windows import get_round_trip_pairs, get_routes_with_data
from partner_awards.airfrance.route_discovery import months_present, discovery_multi_origin


# Recommended routes to seed (origin, destination)
SEED_RECOMMENDED = [
    ("AMS", "JNB"), ("AMS", "CPT"), ("AMS", "BKK"), ("AMS", "SIN"), ("PAR", "JNB"),
    ("PAR", "CPT"), ("PAR", "BKK"), ("PAR", "SIN"), ("AMS", "NRT"), ("PAR", "NRT"),
    ("AMS", "SYD"), ("PAR", "SYD"), ("AMS", "GRU"), ("PAR", "GRU"), ("AMS", "EZE"),
    ("PAR", "EZE"), ("AMS", "JFK"), ("PAR", "JFK"), ("AMS", "LAX"), ("PAR", "LAX"),
]

# Virgin Atlantic: flights from London Heathrow (city label, airport code)
VIRGIN_LHR_ROUTES = [
    ("New York", "JFK"),
    ("Las Vegas", "LAS"),
    ("Jamaica", "MBJ"),
    ("Orlando", "MCO"),
    ("Barbados", "BGI"),
    ("Los Angeles", "LAX"),
    ("San Francisco", "SFO"),
    ("Riyadh", "RUH"),
    ("Delhi", "DEL"),
    ("Miami", "MIA"),
    ("Mumbai", "BOM"),
    ("Boston", "BOS"),
    ("Antigua", "ANU"),
    ("Grenada", "GND"),
    ("Dubai", "DXB"),
    ("Lagos", "LOS"),
    ("Cancun", "CUN"),
    ("Maldives", "MLE"),
    ("Cape Town", "CPT"),
    ("Atlanta", "ATL"),
    ("Toronto", "YYZ"),
    ("Tampa", "TPA"),
    ("Bengaluru", "BLR"),
    ("Johannesburg", "JNB"),
    ("Washington", "IAD"),
    ("St Vincent and the Grenadines", "SVD"),
    ("Seoul", "ICN"),
    ("Seattle", "SEA"),
    ("Phuket", "HKT"),
]

bp = Blueprint("partner_awards_pages", __name__, url_prefix="/partner-awards")


def _watch_routes_with_extra(conn):
    """List watch routes with last_scan_at and coverage_days/coverage_total (next 365d)."""
    routes = list_watch_routes(conn, "flyingblue")
    today = datetime.now().strftime("%Y-%m-%d")
    end_365 = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
    out = []
    for r in routes:
        o, d = r["origin"], r["destination"]
        cur = conn.execute(
            """SELECT MAX(s.started_at) FROM partner_award_scan_runs s
               INNER JOIN partner_award_calendar_fares c ON c.scan_run_id = s.id
               WHERE c.origin=? AND c.destination=?""",
            (o, d),
        )
        row = cur.fetchone()
        last_scan_at = row[0] if row and row[0] else None
        cur = conn.execute(
            """SELECT COUNT(DISTINCT depart_date) FROM partner_award_calendar_fares
               WHERE source='AF' AND origin=? AND destination=? AND miles IS NOT NULL
                 AND depart_date >= ? AND depart_date <= ?""",
            (o, d, today, end_365),
        )
        coverage_days = cur.fetchone()[0] or 0
        out.append({
            **r,
            "last_scan_at": last_scan_at,
            "coverage_days": coverage_days,
            "coverage_total": 365,
        })
    return out


def _data_freshness(conn, month: str):
    """Aggregate freshness: last_scan_at, routes_24h, total_routes, month_days_captured/total."""
    try:
        y, m = int(month[:4]), int(month[5:7])
        import calendar
        last_d = calendar.monthrange(y, m)[1]
        start_date = f"{month}-01"
        end_date = f"{month}-{last_d:02d}"
    except (ValueError, IndexError):
        return None
    cur = conn.execute(
        """SELECT MAX(started_at), host_used FROM partner_award_scan_runs
           WHERE id IN (SELECT scan_run_id FROM partner_award_calendar_fares WHERE scan_run_id IS NOT NULL)
           LIMIT 1"""
    )
    row = cur.fetchone()
    last_scan_at = row[0] if row and row[0] else None
    last_scan_host = (row[1] or "") if row else ""
    cutoff_24h = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.execute(
        """SELECT COUNT(DISTINCT origin || '-' || destination) FROM partner_award_scan_runs
           WHERE started_at >= ? AND id IN (SELECT scan_run_id FROM partner_award_calendar_fares WHERE scan_run_id IS NOT NULL)""",
        (cutoff_24h,),
    )
    routes_24h = cur.fetchone()[0] or 0
    cur = conn.execute(
        """SELECT COUNT(DISTINCT origin || '-' || destination) FROM partner_award_calendar_fares WHERE source='AF'"""
    )
    total_routes = cur.fetchone()[0] or 0
    cur = conn.execute(
        """SELECT COUNT(DISTINCT depart_date) FROM partner_award_calendar_fares
           WHERE source='AF' AND depart_date >= ? AND depart_date <= ? AND miles IS NOT NULL""",
        (start_date, end_date),
    )
    month_days_captured = cur.fetchone()[0] or 0
    return {
        "last_scan_at": last_scan_at,
        "last_scan_host": last_scan_host,
        "routes_24h": routes_24h,
        "total_routes": total_routes,
        "month_days_captured": month_days_captured,
        "month_days_total": last_d,
    }


def _latest_job(conn):
    """Latest Flying Blue job with progress parsed."""
    cur = conn.execute(
        """SELECT id, status, created_at, started_at, finished_at, progress_json, last_error
           FROM partner_award_jobs WHERE program='flyingblue' ORDER BY id DESC LIMIT 1"""
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        progress = json.loads(row[5] or "{}")
    except (json.JSONDecodeError, TypeError):
        progress = {}
    return {
        "id": row[0],
        "status": row[1],
        "created_at": row[2],
        "started_at": row[3],
        "finished_at": row[4],
        "progress": progress,
        "last_error": row[6],
    }


def _trip_nights(win_trip: str, win_nights_min: int, win_nights_max: int):
    if win_trip == "weekend":
        return 2, 4
    if win_trip == "5-7":
        return 5, 7
    return max(3, win_nights_min), min(10, win_nights_max)


def build_flyingblue_context(conn, tab, month, cabin, route_filter, win_origin, win_dest, win_trip, win_nights_min, win_nights_max):
    """Build template context for Flying Blue dashboard."""
    if not month:
        month = datetime.now().strftime("%Y-%m")
    if not cabin:
        cabin = "BUSINESS"
    if not win_trip:
            win_trip = "weekend"
    win_nights_min = int(win_nights_min) if win_nights_min is not None else 3
    win_nights_max = int(win_nights_max) if win_nights_max is not None else 10

    watch_routes = _watch_routes_with_extra(conn)
    data_freshness = _data_freshness(conn, month)
    blocked, blocked_until = is_blocked()
    state = read_state()
    has_cookies = bool((state.get("afkl_cookie_string") or "").strip())
    latest_job = _latest_job(conn)
    months_all = months_present(conn, cabin)
    if not months_all:
        months_all = [month]

    # Suggested routes (discovery)
    suggested_routes = []
    if months_all:
        discovery = discovery_multi_origin(conn, ["AMS", "PAR"], cabin, months_all[:3], limit_per_origin=10)
        for orig in ["AMS", "PAR"]:
            for r in discovery.get(orig, [])[:10]:
                suggested_routes.append(r)
    suggested_routes = suggested_routes[:20]

    # Calendar tab: year view for selected route
    flyingblue_year_calendar = None
    calendar_route_options = []
    if watch_routes:
        calendar_route_options = [(f"{r['origin']}-{r['destination']}", f"{r['origin']}→{r['destination']}") for r in watch_routes]
    if tab == "calendar" and route_filter:
        parts = route_filter.split("-")
        if len(parts) >= 2:
            o, d = parts[0].strip(), parts[1].strip()
            flyingblue_year_calendar = build_daily_year_calendar(conn, cabin, o, d, 365)

    # Windows tab
    win_origins = list({r[0] for r in get_routes_with_data(conn)})
    win_dests_for_origin = []
    if win_origin:
        cur = conn.execute(
            """SELECT DISTINCT destination FROM partner_award_calendar_fares
               WHERE source='AF' AND origin=? ORDER BY destination""",
            (win_origin,),
        )
        win_dests_for_origin = [r[0] for r in cur.fetchall()]
    windows_pairs = []
    if win_origin and win_dest and month:
        nights_min, nights_max = _trip_nights(win_trip, win_nights_min, win_nights_max)
        windows_pairs = get_round_trip_pairs(
            conn, win_origin, win_dest, cabin, month, nights_min, nights_max
        )

    return {
        "tab": tab or "routes",
        "month": month,
        "cabin": cabin,
        "route_filter": route_filter or "",
        "win_origin": win_origin or "AMS",
        "win_dest": win_dest or "",
        "win_trip": win_trip,
        "win_nights_min": win_nights_min,
        "win_nights_max": win_nights_max,
        "watch_routes": watch_routes,
        "data_freshness": data_freshness,
        "blocked_until": blocked_until[:19] if blocked_until else None,
        "has_cookies": has_cookies,
        "latest_job": latest_job,
        "suggested_routes": suggested_routes,
        "flyingblue_year_calendar": flyingblue_year_calendar,
        "calendar_route_options": calendar_route_options,
        "win_origins": win_origins,
        "win_dests_for_origin": win_dests_for_origin,
        "months_all": months_all,
        "windows_pairs": windows_pairs,
        "validation_warning": None,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Page routes
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/")
def home():
    return render_template("partner_awards_home.html")


@bp.route("/virgin")
def virgin():
    state = read_state()
    has_cookies = bool((state.get("virgin_cookie_string") or "").strip())
    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    conn = get_partner_conn()
    try:
        init_db(conn)
        watch_routes = list_watch_routes(conn, "virgin")
        return render_template(
            "partner_awards_virgin.html",
            watch_routes=watch_routes,
            lhr_routes=VIRGIN_LHR_ROUTES,
            has_cookies=has_cookies,
        )
    finally:
        conn.close()


@bp.route("/virgin/cookies", methods=["POST"])
def virgin_cookies():
    raw = (request.form.get("cookie_string") or "").strip()
    cookie_string = _extract_cookie_header(raw)
    write_state(virgin_cookie_string=cookie_string)
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        count = len(cookie_string.split("; ")) if cookie_string else 0
        msg = "Cookies saved." if cookie_string else "Cookies cleared."
        if cookie_string and "set-cookie:" in raw.lower():
            msg = f"Extracted {count} cookie(s) and saved."
        return jsonify({
            "ok": True,
            "has_cookies": bool(cookie_string),
            "message": msg,
            "preview": (cookie_string[:80] + "…") if len(cookie_string) > 80 else cookie_string,
        })
    flash("Cookies saved." if cookie_string else "Cookies cleared.", "success")
    return redirect(url_for("partner_awards_pages.virgin"))


@bp.route("/virgin/cookies/test", methods=["POST"])
def virgin_cookies_test():
    """Call Virgin SearchOffers API with stored cookie; return ok if we get JSON (not 444)."""
    state = read_state()
    cookie_string = (state.get("virgin_cookie_string") or "").strip()
    if not cookie_string:
        return jsonify({"ok": False, "message": "No cookie saved. Paste and save first."})
    url = "https://www.virginatlantic.com/flights/search/api/graphql"
    payload = {
        "operationName": "SearchOffers",
        "variables": {
            "request": {
                "pos": None,
                "parties": None,
                "customerDetails": [{"custId": "ADT_0", "ptc": "ADT"}],
                "flightSearchRequest": {
                    "searchOriginDestinations": [
                        {"origin": "LON", "destination": "NYC", "departureDate": "2026-10-24"}
                    ]
                },
            }
        },
        "extensions": {"clientLibrary": {"name": "@apollo/client", "version": "4.1.6"}},
        "query": "query SearchOffers($request: FlightOfferRequestInput!) { searchOffers(request: $request) { result { criteria { origin { code } destination { code } } calendar { from to fromPrices { fromDate price { awardPoints } } } } } }",
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Language": "en-GB,en;q=0.9",
        "Origin": "https://www.virginatlantic.com",
        "Referer": "https://www.virginatlantic.com/flights/search",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Cookie": cookie_string,
    }
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=15)
    except Exception as e:
        return jsonify({"ok": False, "message": "Request failed: " + str(e)})
    if r.status_code != 200:
        msg = "API returned %s." % r.status_code
        cookie_count = len([p for p in cookie_string.split("; ") if p.strip() and "=" in p]) if cookie_string else 0
        if r.status_code == 444:
            msg += " Sent %d cookie pair(s). 444 can mean Virgin’s WAF is blocking this request (e.g. TLS fingerprint: Python doesn’t look like Chrome). Try the same cookie in the script from this machine; if that also gets 444, the site may only accept real browsers." % cookie_count
        else:
            msg += " Cookie may be expired or invalid. (Sent %d pair(s).)" % cookie_count
        return jsonify({"ok": False, "message": msg})
    try:
        data = r.json()
        if data.get("errors"):
            return jsonify({"ok": False, "message": "GraphQL errors: " + str(data["errors"])[:200]})
        if data.get("data", {}).get("searchOffers", {}).get("result"):
            return jsonify({"ok": True, "message": "Cookie works. Got calendar/offers."})
        return jsonify({"ok": True, "message": "Cookie accepted (200). Result empty or partial."})
    except ValueError:
        return jsonify({"ok": False, "message": "API returned non-JSON (maybe blocked)."})


@bp.route("/flyingblue", methods=["GET"])
def flyingblue():
    tab = request.args.get("tab", "calendar")
    if tab == "routes":
        return redirect(url_for("partner_awards_pages.flyingblue_routes"))
    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    conn = get_partner_conn()
    try:
        init_db(conn)
        month = request.args.get("month") or datetime.now().strftime("%Y-%m")
        cabin = request.args.get("cabin", "BUSINESS")
        route_filter = request.args.get("route", "")
        win_origin = request.args.get("win_origin", "AMS")
        win_dest = request.args.get("win_dest", "")
        win_trip = request.args.get("win_trip", "weekend")
        win_nights_min = request.args.get("win_nights_min", type=int)
        win_nights_max = request.args.get("win_nights_max", type=int)
        ctx = build_flyingblue_context(
            conn, tab, month, cabin, route_filter,
            win_origin, win_dest, win_trip, win_nights_min, win_nights_max,
        )
        return render_template("partner_awards_flyingblue.html", **ctx)
    finally:
        conn.close()


@bp.route("/flyingblue/routes", methods=["GET"])
def flyingblue_routes():
    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    conn = get_partner_conn()
    try:
        init_db(conn)
        month = request.args.get("month") or datetime.now().strftime("%Y-%m")
        ctx = build_flyingblue_context(
            conn, "routes", month, "BUSINESS", "", "AMS", "", "weekend", 3, 10
        )
        return render_template("partner_awards_flyingblue_routes.html", **ctx)
    finally:
        conn.close()


@bp.route("/flyingblue/clear-block", methods=["POST"])
def flyingblue_clear_block():
    clear_blocked()
    flash("Block cleared. You can run scans again.", "success")
    return redirect(url_for("partner_awards_pages.flyingblue_routes"))


@bp.route("/flyingblue/cookies", methods=["POST"])
def flyingblue_cookies():
    cookie_string = _extract_cookie_header(request.form.get("cookie_string") or "")
    write_state(afkl_cookie_string=cookie_string)
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({
            "ok": True,
            "has_cookies": bool(cookie_string),
            "message": "Cookies saved." if cookie_string else "Cookies cleared.",
        })
    flash("Cookies saved." if cookie_string else "Cookies cleared.", "success")
    return redirect(url_for("partner_awards_pages.flyingblue_routes"))


@bp.route("/flyingblue/cookies/test", methods=["POST"])
def flyingblue_cookies_test():
    # Placeholder: real test would call AF/KLM with stored cookies
    return jsonify({"ok": True, "message": "Test not implemented in app; use runner with config."})


@bp.route("/flyingblue/status")
def flyingblue_status():
    conn = get_partner_conn()
    try:
        init_db(conn)
        job = _latest_job(conn)
        return jsonify({"job": job})
    finally:
        conn.close()


@bp.route("/flyingblue/run-batch", methods=["POST"])
def flyingblue_run_batch():
    conn = get_partner_conn()
    try:
        init_db(conn)
        conn.execute(
            """INSERT INTO partner_award_jobs (program, job_type, status, params_json, progress_json)
               VALUES ('flyingblue', 'open_dates_month', 'queued', '{}', '{}')""",
        )
        conn.commit()
        flash("Job queued. Run the worker: python -m partner_awards.jobs_worker", "success")
    finally:
        conn.close()
    return redirect(url_for("partner_awards_pages.flyingblue_routes"))


# ═══════════════════════════════════════════════════════════════════════════
# Watchlist
# ═══════════════════════════════════════════════════════════════════════════

def _extract_cookie_header(pasted: str) -> str:
    """
    Extract a Cookie header value (name=value; name2=value2; ...) from pasted text.
    Accepts: raw Cookie header, Set-Cookie line(s) (with or without "Set-Cookie:" prefix),
    or "Copy value" from DevTools which is the value part of one Set-Cookie.
    """
    text = (pasted or "").strip().replace("\r\n", "\n").replace("\r", "\n")
    if not text:
        return ""

    def take_name_value(line: str) -> str | None:
        """First token (until ; or newline) is name=value."""
        segment = line.split("\n")[0].strip().split(";")[0].strip()
        return segment if segment and "=" in segment else None

    # Explicit Set-Cookie: lines (one or more)
    if "set-cookie:" in text.lower():
        parts = []
        for block in re.split(r"Set-Cookie:\s*", text, flags=re.IGNORECASE):
            block = block.strip()
            if not block:
                continue
            seg = take_name_value(block)
            if seg:
                parts.append(seg)
        if parts:
            return "; ".join(parts)

    # "Copy value" from Set-Cookie (no "Set-Cookie:" prefix): e.g. "bm_s=YAAQ...; Domain=.x.com; Path=/; Secure"
    # One or more lines; each may have attributes after the first ";"
    parts = []
    for line in text.split("\n"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        first = line.split(";")[0].strip()
        if not first or "=" not in first:
            continue
        rest = line[len(first) :].lstrip()
        if rest.startswith(";"):
            rest = rest[1:].strip().lower()
            if any(
                rest.startswith(x)
                for x in ("domain=", "path=", "expires=", "max-age=", "secure", "httponly", "samesite=")
            ):
                parts.append(first)
    if parts:
        return "; ".join(parts)

    # Strip "Cookie:" prefix if present
    if text.lower().startswith("cookie:"):
        text = text[7:].strip()
    # DevTools sometimes pastes "cookie" as first line, then value, then other headers (priority, referer, ...)
    lines = text.split("\n")
    if lines and lines[0].strip().lower() == "cookie":
        lines = lines[1:]
        text = "\n".join(lines).strip()
    # If there are newlines, take only the first line that looks like a cookie value (contains "=" and "; ")
    if "\n" in text:
        for line in text.split("\n"):
            line = line.strip()
            if "=" in line and "; " in line:
                return " ".join(line.split())
        # No line with both; use first line that has "="
        for line in text.split("\n"):
            line = line.strip()
            if "=" in line:
                return " ".join(line.split())
    # Single line or already clean - normalize whitespace
    return " ".join(text.split())


def _watchlist_redirect(program: str = None):
    """Redirect to Virgin, Flying Blue Routes, or Flying Blue dashboard after watchlist action."""
    if program == "virgin":
        return redirect(url_for("partner_awards_pages.virgin"))
    referrer = request.referrer or ""
    if "/partner-awards/virgin" in referrer:
        return redirect(url_for("partner_awards_pages.virgin"))
    if "/partner-awards/flyingblue/routes" in referrer:
        return redirect(url_for("partner_awards_pages.flyingblue_routes"))
    return redirect(url_for("partner_awards_pages.flyingblue"))


@bp.route("/watchlist/add", methods=["POST"])
def watchlist_add():
    program = request.form.get("program", "flyingblue")
    origin = request.form.get("origin", "").strip().upper()[:4]
    destination = request.form.get("destination", "").strip().upper()[:4]
    if not origin or not destination:
        flash("Origin and destination required.", "error")
        return _watchlist_redirect(program)
    conn = get_partner_conn()
    try:
        init_db(conn)
        upsert_watch_route(conn, program, origin, destination, enabled=1)
        flash(f"Added {origin}→{destination}.", "success")
    except ValueError as e:
        flash(str(e), "error")
    finally:
        conn.close()
    return _watchlist_redirect(program)


@bp.route("/watchlist/toggle", methods=["POST"])
def watchlist_toggle():
    rid = request.form.get("id", type=int)
    enabled = request.form.get("enabled", "1") == "1"
    program = request.form.get("program")
    if rid is None:
        return _watchlist_redirect(program)
    conn = get_partner_conn()
    try:
        init_db(conn)
        set_watch_route_enabled(conn, rid, 1 if enabled else 0)
    finally:
        conn.close()
    return _watchlist_redirect(program)


@bp.route("/watchlist/remove", methods=["POST"])
def watchlist_remove():
    rid = request.form.get("id", type=int)
    program = request.form.get("program")
    if rid is None:
        return _watchlist_redirect(program)
    conn = get_partner_conn()
    try:
        init_db(conn)
        delete_watch_route(conn, rid)
        flash("Route removed.", "success")
    finally:
        conn.close()
    return _watchlist_redirect(program)


@bp.route("/watchlist/seed-recommended", methods=["POST"])
def watchlist_seed_recommended():
    conn = get_partner_conn()
    try:
        init_db(conn)
        added = 0
        for origin, destination in SEED_RECOMMENDED:
            try:
                upsert_watch_route(conn, "flyingblue", origin, destination, enabled=1)
                added += 1
            except ValueError:
                pass
        flash(f"Added {added} recommended routes. Enable/disable as needed, then Start Run.", "success")
    finally:
        conn.close()
    return redirect(url_for("partner_awards_pages.flyingblue") + "?tab=routes")


# ═══════════════════════════════════════════════════════════════════════════
# Jobs
# ═══════════════════════════════════════════════════════════════════════════

def _job_to_dict(row):
    try:
        progress = json.loads(row[5] or "{}")
    except (json.JSONDecodeError, TypeError):
        progress = {}
    return {
        "id": row[0],
        "job_type": row[1],
        "status": row[2],
        "created_at": row[3],
        "started_at": row[4],
        "progress": progress,
        "last_error": row[6],
    }


@bp.route("/jobs")
def jobs_list():
    conn = get_partner_conn()
    try:
        init_db(conn)
        cur = conn.execute(
            """SELECT id, job_type, status, created_at, started_at, progress_json, last_error
               FROM partner_award_jobs WHERE program='flyingblue' ORDER BY id DESC LIMIT 50"""
        )
        jobs = [_job_to_dict(r) for r in cur.fetchall()]
        return render_template("partner_awards_jobs.html", jobs=jobs)
    finally:
        conn.close()


@bp.route("/jobs/<int:job_id>")
def job_detail(job_id):
    conn = get_partner_conn()
    try:
        init_db(conn)
        cur = conn.execute(
            """SELECT id, job_type, status, created_at, started_at, finished_at, progress_json, last_error, params_json
               FROM partner_award_jobs WHERE id=?""",
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            return "Job not found", 404
        try:
            progress = json.loads(row[6] or "{}")
            params = json.loads(row[8] or "{}")
        except (json.JSONDecodeError, TypeError):
            progress = {}
            params = {}
        job = {
            "id": row[0],
            "job_type": row[1],
            "status": row[2],
            "created_at": row[3],
            "started_at": row[4],
            "finished_at": row[5],
            "progress": progress,
            "last_error": row[7],
            "params": params,
        }
        cur = conn.execute(
            """SELECT id, origin, destination, month, cabin, status, started_at, finished_at, last_error
               FROM partner_award_job_tasks WHERE job_id=? ORDER BY id""",
            (job_id,),
        )
        tasks = [
            {
                "id": r[0], "origin": r[1], "destination": r[2], "month": r[3],
                "cabin": r[4], "status": r[5], "started_at": r[6], "finished_at": r[7], "last_error": r[8],
            }
            for r in cur.fetchall()
        ]
        return render_template("partner_awards_job_detail.html", job=job, tasks=tasks)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Calendar (standalone page)
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/calendar")
def calendar_page():
    origin = request.args.get("origin", "AMS")
    destination = request.args.get("destination", "JNB")
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    cabin = request.args.get("cabin", "BUSINESS")
    blocked, blocked_until = is_blocked()
    conn = get_partner_conn()
    try:
        init_db(conn)
        # Minimal context for calendar page (heatmap build could be added here)
        return render_template(
            "partner_awards_calendar.html",
            origin=origin,
            destination=destination,
            month=month,
            cabin=cabin,
            blocked_until=blocked_until[:19] if blocked_until else None,
            month_grid=None,
            rows=[],
            routes_available=[],
            ingest_types=[],
            last_run=None,
            view_mode="best",
            host_used="",
            start_date="",
            end_date="",
        )
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Self-test
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/self-test")
def self_test():
    checks = []
    # DB exists and has tables
    try:
        conn = get_partner_conn()
        init_db(conn)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'partner_award%'")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        checks.append({"name": "Partner DB", "status": "PASS", "msg": f"Tables: {len(tables)}"})
    except Exception as e:
        checks.append({"name": "Partner DB", "status": "FAIL", "msg": str(e)})
    return render_template("partner_awards_self_test.html", checks=checks)
