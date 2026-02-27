"""
Air France / KLM (AFKL) award offers parser + SQLite DB writer.

Parses SearchResultAvailableOffersQuery JSON response.
Input: data.availableOffers.offerItineraries[] (each has connections[], upsellCabinProducts[]).
Cabin pricing/availability from upsellCabinProducts[].connections[] per connection index.
See docs/airfrance-awards-api.md for live fetch (3 GraphQL calls).
"""

from __future__ import annotations

import json
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

# Schema path for init_db
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


@dataclass
class CabinInfo:
    cabin_class: str
    miles: Optional[int]
    miles_currency: Optional[str]
    tax: Optional[float]
    tax_currency: Optional[str]
    seats_available: Optional[int]
    fare_family: Optional[str]
    flight_details_path: Optional[str]


@dataclass
class Offer:
    source: str
    origin: str
    destination: str
    depart_date: date
    cabin_requested: Optional[str]
    itinerary_key: str
    stops: int
    duration_minutes: Optional[int]
    carriers: List[str]
    segments: List[Dict[str, Any]]
    cabins: Dict[str, CabinInfo]
    raw_offer_id: Optional[str] = None


def _safe_get(d: Dict[str, Any], *path: str) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def _parse_iso_dt(s: Optional[str]) -> Optional[str]:
    if not s or not isinstance(s, str):
        return None
    try:
        from datetime import datetime
        datetime.fromisoformat(s)
        return s
    except Exception:
        return s


def _extract_carrier_code(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        return obj.get("code")
    return None


def _build_segment(seg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize segment from API format (origin/destination, marketingFlight)."""
    origin = seg.get("origin") or {}
    dest = seg.get("destination") or {}
    mkt_flight = seg.get("marketingFlight") or {}
    op_flight = (mkt_flight.get("operatingFlight") or {}) if isinstance(mkt_flight, dict) else {}

    mkt_carrier = _extract_carrier_code(mkt_flight.get("carrier")) if isinstance(mkt_flight, dict) else None
    op_carrier = _extract_carrier_code(op_flight.get("carrier")) if isinstance(op_flight, dict) else None
    flight_no = mkt_flight.get("number") or op_flight.get("number") if isinstance(mkt_flight, dict) else None

    if flight_no is None:
        flight_no = seg.get("flightNumber") or seg.get("number")

    aircraft = seg.get("equipmentName") or (
        (seg.get("aircraft") or {}).get("code") if isinstance(seg.get("aircraft"), dict) else seg.get("aircraft")
    )

    return {
        "from": origin.get("code") if isinstance(origin, dict) else None,
        "to": dest.get("code") if isinstance(dest, dict) else None,
        "departure": _parse_iso_dt(seg.get("departureDateTime")),
        "arrival": _parse_iso_dt(seg.get("arrivalDateTime")),
        "marketingCarrier": mkt_carrier,
        "operatingCarrier": op_carrier,
        "flightNumber": flight_no,
        "dateVariation": seg.get("dateVariation"),
        "aircraft": aircraft,
    }


def _hash_itinerary(segments: List[Dict[str, Any]]) -> str:
    """Stable sha256 hash of segment sequence (carrier+flightNumber+from+to+times). Aligns with docs/adapters/airfrance_adapter.py."""
    parts: List[str] = []
    for seg in segments:
        parts.append("|".join([
            str(seg.get("marketingCarrier") or ""),
            str(seg.get("operatingCarrier") or ""),
            str(seg.get("flightNumber") or ""),
            str(seg.get("from") or ""),
            str(seg.get("to") or ""),
            str(seg.get("departure") or ""),
            str(seg.get("arrival") or ""),
        ]))
    raw = "||".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _extract_carriers(segments: List[Dict[str, Any]]) -> List[str]:
    carriers = []
    for s in segments:
        for k in ("operatingCarrier", "marketingCarrier"):
            c = s.get(k)
            if c and c not in carriers:
                carriers.append(c)
    return carriers


def _extract_cabins_from_upsell_for_connection(
    upsell_products: List[Dict[str, Any]],
    connection_index: int = 0,
) -> Dict[str, CabinInfo]:
    """Extract cabin info from upsellCabinProducts, using connection index for multi-leg itineraries."""
    cabins: Dict[str, CabinInfo] = {}

    for prod in upsell_products or []:
        conns = prod.get("connections") or []
        conn = conns[connection_index] if connection_index < len(conns) else (conns[0] if conns else None)
        if not conn:
            continue

        cabin_class = conn.get("cabinClass")
        if not cabin_class:
            continue

        price = conn.get("price") or {}
        tax = conn.get("tax") or {}
        miles = price.get("amount")
        tax_amt = tax.get("amount") if isinstance(tax, dict) else None
        seats = conn.get("numberOfSeatsAvailable")

        if miles is None and tax_amt is None and seats is None:
            continue

        fare_family = None
        if isinstance(conn.get("fareFamily"), dict):
            fare_family = conn["fareFamily"].get("code")

        flight_details_path = None
        if isinstance(conn.get("resourceIds"), dict):
            flight_details_path = conn["resourceIds"].get("flightDetails")

        cabins[cabin_class] = CabinInfo(
            cabin_class=cabin_class,
            miles=int(miles) if miles is not None else None,
            miles_currency=price.get("currencyCode") if isinstance(price, dict) else None,
            tax=float(tax_amt) if tax_amt is not None else None,
            tax_currency=tax.get("currencyCode") if isinstance(tax, dict) else None,
            seats_available=seats,
            fare_family=fare_family,
            flight_details_path=flight_details_path,
        )

    return cabins


def _find_connections_from_offer_itineraries(root: Dict[str, Any]) -> List[tuple]:
    """
    Extract (connection, upsell_products) from data.availableOffers.offerItineraries.
    Returns list of (connection_dict, upsell_products_list) for each connection.
    """
    result: List[tuple] = []
    itineraries = _safe_get(root, "data", "availableOffers", "offerItineraries")
    if not isinstance(itineraries, list):
        return result

    for it in itineraries:
        if not isinstance(it, dict):
            continue
        conns = it.get("connections") or []
        upsell = it.get("upsellCabinProducts") or []

        for i, conn in enumerate(conns):
            if not isinstance(conn, dict) or not isinstance(conn.get("segments"), list):
                continue
            result.append((conn, upsell, i))

    return result


def parse_search_result_available_offers(
    response_json: Dict[str, Any],
    *,
    source: str,
    origin: str,
    destination: str,
    depart_date: date,
    cabin_requested: Optional[str] = None,
) -> List[Offer]:
    """Parse SearchResultAvailableOffersQuery response -> list[Offer]."""
    offers_out: List[Offer] = []

    items = _find_connections_from_offer_itineraries(response_json)
    if not items:
        return offers_out

    for conn, upsell_products, conn_index in items:
        segments_raw = conn.get("segments") or []
        segments = [_build_segment(s) for s in segments_raw if isinstance(s, dict)]
        if not segments:
            continue

        itinerary_key = _hash_itinerary(segments)
        carriers = _extract_carriers(segments)
        stops = max(0, len(segments) - 1)

        duration = conn.get("duration")
        duration_minutes = int(duration) if isinstance(duration, (int, float)) else None

        cabins = _extract_cabins_from_upsell_for_connection(upsell_products, conn_index)

        offers_out.append(
            Offer(
                source=source,
                origin=origin,
                destination=destination,
                depart_date=depart_date,
                cabin_requested=cabin_requested,
                itinerary_key=itinerary_key,
                stops=stops,
                duration_minutes=duration_minutes,
                carriers=carriers,
                segments=segments,
                cabins=cabins,
                raw_offer_id=conn.get("_id"),
            )
        )

    return offers_out


# =========================
# SQLite DB functions
# =========================


def init_db(conn: sqlite3.Connection) -> None:
    """Create partner_award_* tables if they don't exist."""
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


def create_scan_run(
    conn: sqlite3.Connection,
    *,
    source: str,
    origin: Optional[str],
    destination: Optional[str],
    cabin_requested: Optional[str],
    depart_date: Optional[date],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO partner_award_scan_runs (source, origin, destination, cabin_requested, depart_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source, origin, destination, cabin_requested, depart_date.isoformat() if depart_date else None),
    )
    conn.commit()
    return cur.lastrowid


def store_raw_response(
    conn: sqlite3.Connection,
    *,
    scan_run_id: Optional[int],
    source: str,
    operation_name: str,
    origin: Optional[str],
    destination: Optional[str],
    depart_date: Optional[date],
    cabin_requested: Optional[str],
    body: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO partner_award_raw_responses
          (scan_run_id, source, operation_name, origin, destination, depart_date, cabin_requested, body)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            scan_run_id,
            source,
            operation_name,
            origin,
            destination,
            depart_date.isoformat() if depart_date else None,
            cabin_requested,
            json.dumps(body),
        ),
    )
    conn.commit()


def upsert_offers(
    conn: sqlite3.Connection,
    *,
    offers: List[Offer],
    scan_run_id: Optional[int] = None,
) -> int:
    """Upsert offers + cabin rows. Returns number of offers upserted."""
    if not offers:
        return 0

    for offer in offers:
        conn.execute(
            """
            INSERT INTO partner_award_offers (
              scan_run_id, source, origin, destination, depart_date, cabin_requested,
              itinerary_key, stops, duration_minutes, carriers, segments, raw_offer_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, origin, destination, depart_date, itinerary_key)
            DO UPDATE SET
              scan_run_id = excluded.scan_run_id,
              cabin_requested = excluded.cabin_requested,
              stops = excluded.stops,
              duration_minutes = excluded.duration_minutes,
              carriers = excluded.carriers,
              segments = excluded.segments,
              raw_offer_id = excluded.raw_offer_id,
              updated_at = datetime('now')
            """,
            (
                scan_run_id,
                offer.source,
                offer.origin,
                offer.destination,
                offer.depart_date.isoformat(),
                offer.cabin_requested,
                offer.itinerary_key,
                offer.stops,
                offer.duration_minutes,
                json.dumps(offer.carriers) if offer.carriers else None,
                json.dumps(offer.segments),
                offer.raw_offer_id,
            ),
        )

        cur = conn.execute(
            "SELECT id FROM partner_award_offers WHERE source=? AND origin=? AND destination=? AND depart_date=? AND itinerary_key=?",
            (offer.source, offer.origin, offer.destination, offer.depart_date.isoformat(), offer.itinerary_key),
        )
        row = cur.fetchone()
        offer_id = row[0] if row else None

        if offer_id:
            for cabin_class, ci in offer.cabins.items():
                conn.execute(
                    """
                    INSERT INTO partner_award_offer_cabins (
                      offer_id, cabin_class, miles, miles_currency, tax, tax_currency,
                      seats_available, fare_family, flight_details_path
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (offer_id, cabin_class)
                    DO UPDATE SET
                      miles = excluded.miles,
                      miles_currency = excluded.miles_currency,
                      tax = excluded.tax,
                      tax_currency = excluded.tax_currency,
                      seats_available = excluded.seats_available,
                      fare_family = excluded.fare_family,
                      flight_details_path = excluded.flight_details_path,
                      updated_at = datetime('now')
                    """,
                    (
                        offer_id,
                        cabin_class,
                        ci.miles,
                        ci.miles_currency,
                        ci.tax,
                        ci.tax_currency,
                        ci.seats_available,
                        ci.fare_family,
                        ci.flight_details_path,
                    ),
                )

    conn.commit()
    return len(offers)
