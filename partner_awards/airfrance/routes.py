"""
Flask routes for Partner Awards Air France API.
"""

import os
import sqlite3
from pathlib import Path

from flask import Blueprint, request, jsonify

from .service import ingest_fixture

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
