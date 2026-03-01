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


def _build_flight_product_by_id(flight_products: List[Dict[str, Any]], connection_index: int) -> Dict[str, Dict[str, Any]]:
    """Map connection _id -> {price, tax} from flightProducts.connections[connection_index]."""
    by_id: Dict[str, Dict[str, Any]] = {}
    for prod in flight_products or []:
        conns = prod.get("connections") or []
        conn = conns[connection_index] if connection_index < len(conns) else (conns[0] if conns else None)
        if not conn or not conn.get("_id"):
            continue
        price = conn.get("price") or {}
        tax = conn.get("tax") or {}
        by_id[conn["_id"]] = {
            "miles": price.get("amount") if isinstance(price, dict) else None,
            "miles_currency": price.get("currencyCode") if isinstance(price, dict) else None,
            "tax": tax.get("amount") if isinstance(tax, dict) else None,
            "tax_currency": tax.get("currencyCode") if isinstance(tax, dict) else None,
        }
    return by_id


def _extract_cabins_from_upsell_for_connection(
    upsell_products: List[Dict[str, Any]],
    flight_products: List[Dict[str, Any]],
    connection_index: int = 0,
) -> Dict[str, CabinInfo]:
    """
    Extract cabin info from both upsellCabinProducts and flightProducts.
    Merge: if same cabinClass, prefer entry with miles != null (or seats != null).
    Skip all-null cabins.
    """
    cabins: Dict[str, CabinInfo] = {}
    fp_by_id = _build_flight_product_by_id(flight_products, connection_index)

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

        # Merge from flightProducts if upsell has nulls
        conn_id = conn.get("_id")
        if conn_id and conn_id in fp_by_id:
            fp = fp_by_id[conn_id]
            if miles is None and fp.get("miles") is not None:
                miles = fp["miles"]
            if tax_amt is None and fp.get("tax") is not None:
                tax_amt = fp["tax"]

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
    Extract (connection, upsell_products, flight_products) from data.availableOffers.offerItineraries.
    Returns list of (connection_dict, upsell_products_list, flight_products_list, connection_index).
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
        flight_prods = it.get("flightProducts") or []

        for i, conn in enumerate(conns):
            if not isinstance(conn, dict) or not isinstance(conn.get("segments"), list):
                continue
            result.append((conn, upsell, flight_prods, i))

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

    for conn, upsell_products, flight_products, conn_index in items:
        segments_raw = conn.get("segments") or []
        segments = [_build_segment(s) for s in segments_raw if isinstance(s, dict)]
        if not segments:
            continue

        itinerary_key = _hash_itinerary(segments)
        carriers = _extract_carriers(segments)
        stops = max(0, len(segments) - 1)

        duration = conn.get("duration")
        duration_minutes = int(duration) if isinstance(duration, (int, float)) else None

        cabins = _extract_cabins_from_upsell_for_connection(
            upsell_products, flight_products, conn_index
        )

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
    # Migration: add ingest_type for existing DBs
    try:
        conn.execute(
            "ALTER TABLE partner_award_scan_runs ADD COLUMN ingest_type TEXT DEFAULT 'fixture'"
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e).lower():
            raise
    for col in ("host_used", "host_attempts"):
        try:
            conn.execute(f"ALTER TABLE partner_award_scan_runs ADD COLUMN {col} TEXT")
            conn.commit()
        except sqlite3.OperationalError as e2:
            if "duplicate column name" not in str(e2).lower():
                raise

    # Migration: calendar_fares - add host_used to unique, add updated_at
    try:
        cur = conn.execute("PRAGMA table_info(partner_award_calendar_fares)")
        cols = {r[1] for r in cur.fetchall()}
        if "updated_at" not in cols:
            conn.execute(
                """
                CREATE TABLE partner_award_calendar_fares_new (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  scan_run_id INTEGER REFERENCES partner_award_scan_runs(id),
                  host_used TEXT NOT NULL DEFAULT '',
                  source TEXT NOT NULL DEFAULT 'AF',
                  origin TEXT NOT NULL,
                  destination TEXT NOT NULL,
                  cabin_class TEXT NOT NULL,
                  depart_date TEXT NOT NULL,
                  miles INTEGER,
                  tax REAL,
                  created_at TEXT NOT NULL DEFAULT (datetime('now')),
                  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                  UNIQUE (source, origin, destination, cabin_class, depart_date, host_used)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO partner_award_calendar_fares_new
                  (id, scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax, created_at, updated_at)
                SELECT id, scan_run_id, COALESCE(host_used,''), source, origin, destination, cabin_class, depart_date, miles, tax, created_at, datetime('now')
                FROM partner_award_calendar_fares
                """
            )
            conn.execute("DROP TABLE partner_award_calendar_fares")
            conn.execute("ALTER TABLE partner_award_calendar_fares_new RENAME TO partner_award_calendar_fares")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_partner_award_calendar_fares_route ON partner_award_calendar_fares (source, origin, destination, depart_date)"
            )
            conn.commit()
    except sqlite3.OperationalError as e3:
        err = str(e3).lower()
        if "duplicate column name" in err or "no such table" in err:
            pass
        else:
            raise


def create_scan_run(
    conn: sqlite3.Connection,
    *,
    source: str,
    ingest_type: str = "fixture",
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    cabin_requested: Optional[str] = None,
    depart_date: Optional[date] = None,
    host_used: Optional[str] = None,
    host_attempts: Optional[str] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO partner_award_scan_runs (source, ingest_type, origin, destination, cabin_requested, depart_date, host_used, host_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (source, ingest_type, origin, destination, cabin_requested, depart_date.isoformat() if depart_date else None, host_used, host_attempts),
    )
    conn.commit()
    return cur.lastrowid


RAW_BODY_MAX_BYTES = 200 * 1024  # 200KB cap to avoid DB bloat


def _parse_lowest_fare_entries(
    body: Dict[str, Any],
    cabins: List[str],
) -> List[tuple]:
    """
    Extract (date, cabin, miles, tax) from LowestFareOffers response.
    Returns list of (date_str, cabin, miles, tax).
    """
    from datetime import datetime

    entries: List[tuple] = []
    data = body.get("data") or {}

    def _miles_from(v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, dict):
            m = v.get("miles")
            if m is not None:
                return int(m)
            p = v.get("price")
            if isinstance(p, dict):
                a = p.get("amount")
                if a is not None:
                    return int(a)
        return None

    def _tax_from(v: Any) -> Optional[float]:
        if not isinstance(v, dict):
            return None
        t = v.get("tax")
        if t is not None:
            try:
                return float(t)
            except (TypeError, ValueError):
                pass
        return None

    cabin_list = cabins if cabins else ["ECONOMY"]

    for key in ("lowestFareOffers", "searchResult", "calendar", "offers"):
        node = data.get(key)
        if not isinstance(node, dict):
            continue
        # Dict format: date_str -> {miles, tax}
        for k, v in (node.get("connections") or node.get("days") or node.get("dates") or {}).items():
            if isinstance(v, dict) and isinstance(k, str) and len(k) == 10:
                try:
                    datetime.strptime(k, "%Y-%m-%d")
                except ValueError:
                    continue
                miles = _miles_from(v)
                if miles is not None:
                    tax = _tax_from(v)
                    for cab in cabin_list:
                        entries.append((k, cab, miles, tax))
        # List format
        for conn in (node.get("connections") or []):
            if not isinstance(conn, dict):
                continue
            dt = conn.get("departureDate") or conn.get("date")
            miles = _miles_from(conn)
            if dt and miles is not None:
                date_str = str(dt)[:10]
                try:
                    datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    continue
                tax = _tax_from(conn)
                cab = conn.get("cabin") or cabin_list[0]
                if cab not in cabin_list:
                    cab = cabin_list[0]
                entries.append((date_str, cab, miles, tax))

    # Deduplicate: (date, cabin) -> keep first
    seen: Dict[tuple, bool] = {}
    unique = []
    for t in entries:
        k = (t[0], t[1])
        if k not in seen:
            seen[k] = True
            unique.append(t)
    return unique


def ingest_lowest_fares(
    conn: sqlite3.Connection,
    *,
    scan_run_id: int,
    payload: Dict[str, Any],
    origin: str,
    destination: str,
    cabins: List[str],
    host_used: Optional[str] = None,
    source: str = "AF",
) -> int:
    """
    Parse LowestFareOffers JSON and upsert into partner_award_calendar_fares.
    Returns number of rows upserted.
    """
    init_db(conn)
    entries = _parse_lowest_fare_entries(payload, cabins)
    host_val = (host_used or "").strip() or ""
    count = 0
    for date_str, cabin, miles, tax in entries:
        try:
            conn.execute(
                """
                INSERT INTO partner_award_calendar_fares
                  (scan_run_id, host_used, source, origin, destination, cabin_class, depart_date, miles, tax)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, origin, destination, cabin_class, depart_date, host_used)
                DO UPDATE SET scan_run_id=excluded.scan_run_id, miles=excluded.miles, tax=excluded.tax, updated_at=datetime('now')
                """,
                (scan_run_id, host_val, source, origin, destination, cabin, date_str, miles, tax),
            )
            count += 1
        except Exception:
            pass
    conn.commit()
    return count


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
    body_str = json.dumps(body)
    if len(body_str) > RAW_BODY_MAX_BYTES:
        body_str = body_str[:RAW_BODY_MAX_BYTES] + '\n/* TRUNCATED - original ' + str(len(body_str)) + ' bytes */'
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
            body_str,
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

    # Refresh best offers for affected route/dates
    if offers:
        dates = {offer.depart_date for offer in offers}
        origin = offers[0].origin
        destination = offers[0].destination
        source = offers[0].source
        for d in dates:
            _refresh_best_offers_for_date(conn, source=source, origin=origin, destination=destination, depart_date=d)

    return len(offers)


def _refresh_best_offers_for_date(
    conn: sqlite3.Connection,
    *,
    source: str,
    origin: str,
    destination: str,
    depart_date: date,
) -> None:
    """Recompute best offer per cabin for a given route/date from existing offers."""
    cur = conn.execute(
        """
        SELECT o.id, o.stops, o.duration_minutes, o.carriers,
               c.cabin_class, c.miles, c.tax
        FROM partner_award_offers o
        JOIN partner_award_offer_cabins c ON c.offer_id = o.id
        WHERE o.source=? AND o.origin=? AND o.destination=? AND o.depart_date=?
          AND c.miles IS NOT NULL
        ORDER BY c.cabin_class, c.miles ASC
        """,
        (source, origin, destination, depart_date.isoformat()),
    )
    rows = cur.fetchall()
    best: Dict[str, tuple] = {}
    for r in rows:
        cabin_class = r[4]
        if cabin_class not in best:
            carriers = json.loads(r[3]) if r[3] else []
            carrier = carriers[0] if carriers else None
            best[cabin_class] = (r[5], r[6], r[1], r[2], carrier, r[0])

    for cabin_class, (miles, tax, stops, duration, carrier, offer_id) in best.items():
        conn.execute(
            """
            INSERT INTO partner_award_best_offers
              (source, origin, destination, depart_date, cabin_class, best_miles, best_tax, is_direct, duration_minutes, carrier, offer_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, origin, destination, depart_date, cabin_class)
            DO UPDATE SET
              best_miles = excluded.best_miles,
              best_tax = excluded.best_tax,
              is_direct = excluded.is_direct,
              duration_minutes = excluded.duration_minutes,
              carrier = excluded.carrier,
              offer_id = excluded.offer_id,
              updated_at = datetime('now')
            """,
            (source, origin, destination, depart_date.isoformat(), cabin_class, miles, tax, 1 if stops == 0 else 0, duration, carrier, offer_id),
        )
    conn.commit()
