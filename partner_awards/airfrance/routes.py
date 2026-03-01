"""
Flask routes for Partner Awards Air France API.
"""

import os
import sqlite3
from pathlib import Path

from flask import Blueprint, request, jsonify

from .service import calendar_scan, ingest_fixture, live_test, live_test_direct, live_test_playwright, sanity_check

# Separate DB for partner awards (no SAS tables)
PARTNER_DB_DIR = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards"))
PARTNER_DB_PATH = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
    PARTNER_DB_DIR, "partner_awards.sqlite"
)


def get_partner_conn():
    """Get SQLite connection for partner awards DB."""
    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(PARTNER_DB_PATH)


bp = Blueprint("partner_awards_airfrance", __name__, url_prefix="/partner-awards/airfrance")


@bp.route("/test-ingest", methods=["POST"])
def test_ingest():
    """
    Read fixture JSON, insert scan run + raw response + offers + cabins.
    Body: { origin?, destination?, depart_date?, cabin_requested? }
    """
    data = request.get_json(silent=True) or {}
    origin = data.get("origin", "PAR")
    destination = data.get("destination", "JNB")
    depart_date = data.get("depart_date", "2026-02-27")
    cabin_requested = data.get("cabin_requested", "ECONOMY")

    conn = get_partner_conn()
    try:
        result = ingest_fixture(
            conn,
            origin=origin,
            destination=destination,
            depart_date=depart_date,
            cabin_requested=cabin_requested,
        )
        return jsonify(result)
    finally:
        conn.close()


@bp.route("/offers", methods=["GET"])
def get_offers():
    """
    Query params: origin, destination, depart_date (optional), limit (default 50).
    Returns offers with segments + cabins aggregated by cabin_class.
    """
    origin = request.args.get("origin", "PAR")
    destination = request.args.get("destination", "JNB")
    depart_date = request.args.get("depart_date")
    limit = int(request.args.get("limit", 50))

    conn = get_partner_conn()
    conn.row_factory = sqlite3.Row
    try:
        conditions = ["o.source = 'AF'", "o.origin = ?", "o.destination = ?"]
        params = [origin, destination]

        if depart_date:
            conditions.append("o.depart_date = ?")
            params.append(depart_date)

        where = " AND ".join(conditions)
        params.append(limit)

        cur = conn.execute(
            f"""
            SELECT o.id, o.origin, o.destination, o.depart_date, o.stops, o.duration_minutes,
                   o.segments, o.carriers
            FROM partner_award_offers o
            WHERE {where}
            ORDER BY o.depart_date, o.stops, o.id
            LIMIT ?
            """,
            params,
        )
        rows = cur.fetchall()

        offers = []
        for r in rows:
            import json
            segments = json.loads(r["segments"]) if r["segments"] else []
            carriers = json.loads(r["carriers"]) if r["carriers"] else []

            cab_cur = conn.execute(
                "SELECT cabin_class, miles, tax, seats_available FROM partner_award_offer_cabins WHERE offer_id = ?",
                (r["id"],),
            )
            cabins = {}
            for c in cab_cur.fetchall():
                cabins[c["cabin_class"]] = {
                    "miles": c["miles"],
                    "tax": c["tax"],
                    "seats_available": c["seats_available"],
                }

            offers.append({
                "id": r["id"],
                "origin": r["origin"],
                "destination": r["destination"],
                "depart_date": r["depart_date"],
                "stops": r["stops"],
                "duration_minutes": r["duration_minutes"],
                "segments": segments,
                "carriers": carriers,
                "cabins": cabins,
            })

        return jsonify({"offers": offers})
    finally:
        conn.close()


@bp.route("/live-test-playwright", methods=["POST"])
def live_test_playwright_route():
    """
    Playwright-based live fetch (primary live path).
    Body: { origin?, destination?, depart_date?, cabin? }
    Respects cooldown: if blocked_until in future, refuses to run.
    """
    from .state import is_blocked
    blocked, until = is_blocked()
    if blocked:
        return jsonify({"ok": False, "error": f"Fetching paused until {until} due to blocking"}), 429
    data = request.get_json(silent=True) or {}
    origin = data.get("origin", "PAR")
    destination = data.get("destination", "JNB")
    depart_date = data.get("depart_date", "2026-02-27")
    cabin = data.get("cabin", "ECONOMY")

    conn = get_partner_conn()
    try:
        result = live_test_playwright(
            conn,
            origin=origin,
            destination=destination,
            depart_date=depart_date,
            cabin=cabin,
        )
        return jsonify(result)
    finally:
        conn.close()


@bp.route("/playwright-health", methods=["GET"])
def playwright_health_route():
    """Verify Playwright + Chromium is installed."""
    from .live_playwright import playwright_health_check
    return jsonify(playwright_health_check())


@bp.route("/live-test", methods=["POST"])
def live_test_route():
    """
    Minimal live test: CreateSearchContext → SearchResultAvailableOffersQuery.
    Body: { origin?, destination?, depart_date?, cabin? }
    """
    data = request.get_json(silent=True) or {}
    origin = data.get("origin", "PAR")
    destination = data.get("destination", "JNB")
    depart_date = data.get("depart_date", "2026-02-27")
    cabin = data.get("cabin", "ECONOMY")

    conn = get_partner_conn()
    try:
        result = live_test(
            conn,
            origin=origin,
            destination=destination,
            depart_date=depart_date,
            cabin=cabin,
        )
        return jsonify(result)
    finally:
        conn.close()


@bp.route("/sanity-check", methods=["GET"])
def sanity_check_route():
    """Connectivity sanity check: GET home + gql. If both hang, it's network/proxy."""
    result = sanity_check()
    return jsonify(result)


@bp.route("/live-test-direct", methods=["POST"])
def live_test_direct_route():
    """
    Diagnostic: skip CreateSearchContext, call only SearchResultAvailableOffersQuery
    with a fresh uuid. Isolates whether CreateSearchContext is the problematic call.
    """
    data = request.get_json(silent=True) or {}
    origin = data.get("origin", "PAR")
    destination = data.get("destination", "JNB")
    depart_date = data.get("depart_date", "2026-02-27")
    cabin = data.get("cabin", "ECONOMY")

    conn = get_partner_conn()
    try:
        result = live_test_direct(
            conn,
            origin=origin,
            destination=destination,
            depart_date=depart_date,
            cabin=cabin,
        )
        return jsonify(result)
    finally:
        conn.close()


@bp.route("/calendar-scan", methods=["POST"])
def calendar_scan_route():
    """
    Calendar → drilldown. Body: { origin?, destination?, start_date?, days?, cabins?, max_offer_days? }
    """
    data = request.get_json(silent=True) or {}
    origin = data.get("origin", "PAR")
    destination = data.get("destination", "JNB")
    start_date = data.get("start_date", "2026-02-27")
    days = int(data.get("days", 14))
    cabins = data.get("cabins", ["ECONOMY", "BUSINESS"])
    max_offer_days = int(data.get("max_offer_days", 5))

    conn = get_partner_conn()
    try:
        result = calendar_scan(
            conn,
            origin=origin,
            destination=destination,
            start_date=start_date,
            days=days,
            cabins=cabins,
            max_offer_days=max_offer_days,
        )
        return jsonify(result)
    finally:
        conn.close()


@bp.route("/best-offers", methods=["GET"])
def get_best_offers():
    """Query params: origin, destination, start_date (optional), days (optional). Returns best per date/cabin."""
    import json as _json
    origin = request.args.get("origin", "PAR")
    destination = request.args.get("destination", "JNB")
    start_date = request.args.get("start_date")
    days = int(request.args.get("days", 14))

    conn = get_partner_conn()
    conn.row_factory = sqlite3.Row
    try:
        conditions = ["source = 'AF'", "origin = ?", "destination = ?"]
        params = [origin, destination]
        if start_date:
            conditions.append("depart_date >= ?")
            params.append(start_date)
            from datetime import datetime, timedelta
            end = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=days)).date().isoformat()
            conditions.append("depart_date <= ?")
            params.append(end)

        where = " AND ".join(conditions)
        cur = conn.execute(
            f"SELECT depart_date, cabin_class, best_miles, best_tax, is_direct, duration_minutes, carrier FROM partner_award_best_offers WHERE {where} ORDER BY depart_date, cabin_class",
            params,
        )
        rows = cur.fetchall()
        out = [dict(r) for r in rows]
        return jsonify({"best_offers": out})
    finally:
        conn.close()


@bp.route("/import", methods=["POST"])
def import_route():
    """
    Manual import: upload .json (SearchResultAvailableOffersQuery response) or .har.
    Form fields: file, origin, destination, depart_date, cabin_requested (optional).
    """
    from datetime import datetime

    from .adapter import create_scan_run, init_db, parse_search_result_available_offers, store_raw_response, upsert_offers
    from .import_har import extract_from_har, extract_from_json

    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"ok": False, "error": "No file selected"}), 400

    origin = request.form.get("origin", "PAR")
    destination = request.form.get("destination", "JNB")
    depart_date = request.form.get("depart_date", "2026-02-27")
    cabin_requested = request.form.get("cabin_requested", "ECONOMY")

    body = f.read()
    warnings = []
    op_name = "SearchResultAvailableOffersQuery"

    if f.filename.lower().endswith(".har"):
        parsed, found_op, err = extract_from_har(body)
        if err:
            return jsonify({"ok": False, "error": err}), 400
        if found_op and found_op != op_name:
            warnings.append(f"Used {found_op} (SearchResultAvailableOffersQuery not found)")
        op_name = found_op or op_name
    elif f.filename.lower().endswith(".json"):
        parsed, err = extract_from_json(body)
        if err:
            return jsonify({"ok": False, "error": err}), 400
    else:
        return jsonify({"ok": False, "error": "File must be .json or .har"}), 400

    if not parsed:
        return jsonify({"ok": False, "error": "Could not extract response"}), 400

    conn = get_partner_conn()
    try:
        init_db(conn)
        depart_dt = datetime.strptime(depart_date, "%Y-%m-%d").date()
        scan_run_id = create_scan_run(
            conn,
            source="AF",
            ingest_type="har_import",
            origin=origin,
            destination=destination,
            cabin_requested=cabin_requested,
            depart_date=depart_dt,
        )
        store_raw_response(
            conn,
            scan_run_id=scan_run_id,
            source="AF",
            operation_name=op_name,
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin_requested,
            body=parsed,
        )
        offers = parse_search_result_available_offers(
            parsed,
            source="AF",
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin_requested,
        )
        count = upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)
        return jsonify({
            "ok": True,
            "operation_name": op_name,
            "inserted_offer_count": count,
            "scan_run_id": scan_run_id,
            "warnings": warnings if warnings else None,
        })
    finally:
        conn.close()


@bp.route("/raw", methods=["GET"])
def get_raw():
    """
    Return raw stored JSON for a scan run (for debugging).
    Query param: scan_run_id (required).
    Returns body or first 50KB if very large.
    """
    scan_run_id = request.args.get("scan_run_id")
    if not scan_run_id:
        return jsonify({"error": "scan_run_id required"}), 400

    try:
        scan_run_id = int(scan_run_id)
    except ValueError:
        return jsonify({"error": "scan_run_id must be integer"}), 400

    conn = get_partner_conn()
    try:
        cur = conn.execute(
            "SELECT body FROM partner_award_raw_responses WHERE scan_run_id = ? ORDER BY id DESC LIMIT 1",
            (scan_run_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "No raw response for scan_run_id"}), 404

        body_str = row[0]
        max_chars = 50 * 1024
        if len(body_str) > max_chars:
            body_str = body_str[:max_chars] + "\n... [truncated]"

        return jsonify({"scan_run_id": scan_run_id, "body": body_str, "truncated": len(row[0]) > max_chars})
    finally:
        conn.close()
