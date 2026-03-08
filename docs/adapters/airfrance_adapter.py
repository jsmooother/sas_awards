"""
airfrance_adapter.py

Air France / KLM (AFKL) award offers parser + Postgres schema + DB upsert writer.

Requires:
  pip install psycopg[binary]

Input:
  - JSON response body from SearchResultAvailableOffersQuery

Outputs:
  - Normalized itineraries with segments + cabin pricing/availability
  - Stored to Postgres tables (schema included)

Notes:
  - Cabin pricing+availability is taken from upsellCabinProducts[*].connections[*]
    because that’s where cabinClass / numberOfSeatsAvailable is present in the sample. :contentReference[oaicite:2]{index=2}
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row


# =========================
# Postgres schema
# =========================

SCHEMA_SQL = """
-- Scan runs (optional but recommended)
CREATE TABLE IF NOT EXISTS award_scan_runs (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,                 -- e.g. "AF"
  market TEXT NULL,                     -- e.g. "FR"
  language TEXT NULL,                   -- e.g. "en"
  origin TEXT NULL,                     -- e.g. "PAR"
  destination TEXT NULL,                -- e.g. "JNB"
  cabin_requested TEXT NULL,            -- e.g. "ECONOMY"
  depart_date DATE NULL,
  search_state_uuid UUID NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ NULL,
  status TEXT NOT NULL DEFAULT 'ok',    -- ok|blocked|error
  error TEXT NULL
);

-- Raw responses for forensic debugging / re-parsing later
CREATE TABLE IF NOT EXISTS award_raw_responses (
  id BIGSERIAL PRIMARY KEY,
  scan_run_id BIGINT REFERENCES award_scan_runs(id) ON DELETE SET NULL,
  source TEXT NOT NULL,
  operation_name TEXT NOT NULL,
  origin TEXT NULL,
  destination TEXT NULL,
  depart_date DATE NULL,
  cabin_requested TEXT NULL,
  search_state_uuid UUID NULL,
  retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  body JSONB NOT NULL
);

-- Normalized itinerary offers (one row per itinerary)
CREATE TABLE IF NOT EXISTS award_offers (
  id BIGSERIAL PRIMARY KEY,
  scan_run_id BIGINT REFERENCES award_scan_runs(id) ON DELETE SET NULL,
  source TEXT NOT NULL,                 -- "AF" / "KL"
  origin TEXT NOT NULL,                 -- e.g. "PAR" (search origin code)
  destination TEXT NOT NULL,            -- e.g. "JNB"
  depart_date DATE NOT NULL,            -- outbound date searched
  cabin_requested TEXT NULL,            -- the cabin you requested in the query (may be ECONOMY)
  itinerary_key TEXT NOT NULL,          -- stable hash of segments
  stops INT NOT NULL,
  duration_minutes INT NULL,
  carriers TEXT[] NULL,                 -- operating/marketing carriers observed
  segments JSONB NOT NULL,              -- list of segment objects
  raw_offer_id TEXT NULL,               -- _id from API, if present
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source, origin, destination, depart_date, itinerary_key)
);

-- Cabin pricing/availability per itinerary
CREATE TABLE IF NOT EXISTS award_offer_cabins (
  offer_id BIGINT REFERENCES award_offers(id) ON DELETE CASCADE,
  cabin_class TEXT NOT NULL,            -- ECONOMY|PREMIUM|BUSINESS|FIRST
  miles INT NULL,
  miles_currency TEXT NULL,             -- "MILES"
  tax NUMERIC NULL,
  tax_currency TEXT NULL,               -- "EUR"
  seats_available INT NULL,
  fare_family TEXT NULL,                -- e.g. FFYCLASSIC
  flight_details_path TEXT NULL,        -- /b/v3/flight-details/...
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (offer_id, cabin_class)
);

CREATE INDEX IF NOT EXISTS idx_award_offers_route_date
  ON award_offers (source, origin, destination, depart_date);

CREATE INDEX IF NOT EXISTS idx_award_offer_cabins_miles
  ON award_offer_cabins (cabin_class, miles);
"""


# =========================
# Normalized models
# =========================

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


# =========================
# Parsing helpers
# =========================

def _safe_get(d: Dict[str, Any], *path: str) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def _parse_iso_dt(s: Optional[str]) -> Optional[str]:
    # Keep as ISO string for DB (avoid timezone surprises). Validate lightly.
    if not s or not isinstance(s, str):
        return None
    try:
        datetime.fromisoformat(s)
        return s
    except Exception:
        return s  # keep raw if format differs slightly


def _hash_itinerary(segments: List[Dict[str, Any]]) -> str:
    """
    Stable itinerary hash from segment sequence (carrier+flightNumber+from+to+times).
    """
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


def _find_offer_connections(root: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    The JSON has 'connections' in a few places. We want the list where each item has:
      - 'segments' (list)
      - '_id' like 'OfferConnection:...'
      - 'duration' (minutes)
    We try likely paths then fallback to a shallow scan.
    """
    candidates: List[Any] = []

    # Likely paths (be permissive)
    for path in [
        ("data", "searchResult", "connections"),
        ("data", "searchResult", "availableOffers", "connections"),
        ("data", "searchResult", "offers", "connections"),
        ("data", "searchResult", "availableOffers"),
        ("data", "searchResult", "offers"),
    ]:
        v = _safe_get(root, *path)
        if isinstance(v, list):
            candidates.append(v)
        elif isinstance(v, dict) and isinstance(v.get("connections"), list):
            candidates.append(v["connections"])

    def looks_like_connection(x: Any) -> bool:
        return (
            isinstance(x, dict)
            and isinstance(x.get("segments"), list)
            and (isinstance(x.get("_id"), str) and "OfferConnection:" in x.get("_id", ""))
        )

    for cand in candidates:
        conns = [x for x in cand if looks_like_connection(x)]
        if conns:
            return conns

    # Fallback: scan top-level dict values for a list of such dicts
    def scan(obj: Any) -> Optional[List[Dict[str, Any]]]:
        if isinstance(obj, list):
            conns = [x for x in obj if looks_like_connection(x)]
            if conns:
                return conns
            for x in obj:
                r = scan(x)
                if r:
                    return r
        elif isinstance(obj, dict):
            for v in obj.values():
                r = scan(v)
                if r:
                    return r
        return None

    found = scan(root)
    return found or []


def _build_segment(seg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a segment into a compact JSONB object.
    Expected fields in sample: arrivalDateTime, departureDateTime, origin.code, destination.code,
    marketingCarrier.code, operatingCarrier.code, flightNumber. :contentReference[oaicite:3]{index=3}
    """
    origin = seg.get("origin") or {}
    dest = seg.get("destination") or {}
    mkt = seg.get("marketingCarrier") or {}
    op = seg.get("operatingCarrier") or {}

    # Some APIs embed flight number in different fields; be flexible.
    flight_no = seg.get("flightNumber") or seg.get("number") or seg.get("flight") or None

    return {
        "from": origin.get("code"),
        "to": dest.get("code"),
        "departure": _parse_iso_dt(seg.get("departureDateTime")),
        "arrival": _parse_iso_dt(seg.get("arrivalDateTime")),
        "marketingCarrier": mkt.get("code") if isinstance(mkt, dict) else mkt,
        "operatingCarrier": op.get("code") if isinstance(op, dict) else op,
        "flightNumber": flight_no,
        "dateVariation": seg.get("dateVariation"),
        "aircraft": (seg.get("aircraft") or {}).get("code") if isinstance(seg.get("aircraft"), dict) else seg.get("aircraft"),
    }


def _extract_cabins_from_upsell(upsell_products: List[Dict[str, Any]]) -> Dict[str, CabinInfo]:
    """
    Each upsellCabinProducts[*].connections[*] includes cabinClass, numberOfSeatsAvailable, price, tax, fareFamily.
    This is the best place to get cabin-level availability and pricing. :contentReference[oaicite:4]{index=4}
    """
    cabins: Dict[str, CabinInfo] = {}

    for prod in upsell_products or []:
        for conn in (prod.get("connections") or []):
            cabin_class = conn.get("cabinClass")
            if not cabin_class:
                continue

            price = conn.get("price") or {}
            tax = conn.get("tax") or {}

            miles = price.get("amount")
            miles_ccy = price.get("currencyCode")
            tax_amt = tax.get("amount") if isinstance(tax, dict) else None
            tax_ccy = tax.get("currencyCode") if isinstance(tax, dict) else None

            # Ignore "null pricing" cabins (seen in sample) :contentReference[oaicite:5]{index=5}
            if miles is None and tax_amt is None and conn.get("numberOfSeatsAvailable") is None:
                # keep out of cabins map
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
                miles_currency=miles_ccy,
                tax=float(tax_amt) if tax_amt is not None else None,
                tax_currency=tax_ccy,
                seats_available=conn.get("numberOfSeatsAvailable"),
                fare_family=fare_family,
                flight_details_path=flight_details_path,
            )

    return cabins


def parse_search_result_available_offers(
    response_json: Dict[str, Any],
    *,
    source: str,
    origin: str,
    destination: str,
    depart_date: date,
    cabin_requested: Optional[str] = None,
) -> List[Offer]:
    """
    Parse SearchResultAvailableOffersQuery response -> list[Offer].
    """
    offers_out: List[Offer] = []

    # Locate the offer connections array (each is an itinerary)
    conns = _find_offer_connections(response_json)
    if not conns:
        return offers_out

    # We need cabin products too. In the sample, flightProducts/upsellCabinProducts appear next to connections.
    # We'll find them by looking for a dict that contains both 'connections' and 'upsellCabinProducts'.
    # If not found, we’ll fallback to searching within each conn (less likely).
    search_result = _safe_get(response_json, "data", "searchResult")
    upsell_products = []
    if isinstance(search_result, dict):
        upsell_products = search_result.get("upsellCabinProducts") or []

    # Map cabin info by OfferFlightProductConnection._id if possible (best), else just per-offer global.
    # In the sample, upsellCabinProducts.connections[*]._id matches flightProducts.connections[*]._id,
    # but we primarily care about cabinClass-level values, so we can attach the full cabin map per offer.
    cabins_map = _extract_cabins_from_upsell(upsell_products)

    for conn in conns:
        segments_raw = conn.get("segments") or []
        segments = [_build_segment(s) for s in segments_raw if isinstance(s, dict)]
        itinerary_key = _hash_itinerary(segments)
        carriers = _extract_carriers(segments)
        stops = max(0, len(segments) - 1)

        duration = conn.get("duration")
        duration_minutes = int(duration) if isinstance(duration, (int, float)) else None

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
                cabins=cabins_map,          # attach all cabin data seen in response
                raw_offer_id=conn.get("_id"),
            )
        )

    return offers_out


# =========================
# DB functions
# =========================

def init_db(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def create_scan_run(
    conn: psycopg.Connection,
    *,
    source: str,
    market: Optional[str],
    language: Optional[str],
    origin: Optional[str],
    destination: Optional[str],
    cabin_requested: Optional[str],
    depart_date: Optional[date],
    search_state_uuid: Optional[str],
) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            INSERT INTO award_scan_runs (source, market, language, origin, destination, cabin_requested, depart_date, search_state_uuid)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (source, market, language, origin, destination, cabin_requested, depart_date, search_state_uuid),
        )
        scan_run_id = int(cur.fetchone()["id"])
    conn.commit()
    return scan_run_id


def store_raw_response(
    conn: psycopg.Connection,
    *,
    scan_run_id: Optional[int],
    source: str,
    operation_name: str,
    origin: Optional[str],
    destination: Optional[str],
    depart_date: Optional[date],
    cabin_requested: Optional[str],
    search_state_uuid: Optional[str],
    body: Dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO award_raw_responses
              (scan_run_id, source, operation_name, origin, destination, depart_date, cabin_requested, search_state_uuid, body)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                scan_run_id,
                source,
                operation_name,
                origin,
                destination,
                depart_date,
                cabin_requested,
                search_state_uuid,
                json.dumps(body),
            ),
        )
    conn.commit()


def upsert_offers(
    conn: psycopg.Connection,
    *,
    offers: List[Offer],
    scan_run_id: Optional[int] = None,
) -> int:
    """
    Upserts offers + cabin rows. Returns number of offers upserted.
    """
    if not offers:
        return 0

    with conn.cursor(row_factory=dict_row) as cur:
        for offer in offers:
            cur.execute(
                """
                INSERT INTO award_offers (
                  scan_run_id, source, origin, destination, depart_date, cabin_requested,
                  itinerary_key, stops, duration_minutes, carriers, segments, raw_offer_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT (source, origin, destination, depart_date, itinerary_key)
                DO UPDATE SET
                  scan_run_id = EXCLUDED.scan_run_id,
                  cabin_requested = EXCLUDED.cabin_requested,
                  stops = EXCLUDED.stops,
                  duration_minutes = EXCLUDED.duration_minutes,
                  carriers = EXCLUDED.carriers,
                  segments = EXCLUDED.segments,
                  raw_offer_id = EXCLUDED.raw_offer_id,
                  updated_at = now()
                RETURNING id
                """,
                (
                    scan_run_id,
                    offer.source,
                    offer.origin,
                    offer.destination,
                    offer.depart_date,
                    offer.cabin_requested,
                    offer.itinerary_key,
                    offer.stops,
                    offer.duration_minutes,
                    offer.carriers,
                    json.dumps(offer.segments),
                    offer.raw_offer_id,
                ),
            )
            offer_id = int(cur.fetchone()["id"])

            # Upsert cabin rows
            for cabin_class, ci in offer.cabins.items():
                cur.execute(
                    """
                    INSERT INTO award_offer_cabins (
                      offer_id, cabin_class, miles, miles_currency, tax, tax_currency,
                      seats_available, fare_family, flight_details_path
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (offer_id, cabin_class)
                    DO UPDATE SET
                      miles = EXCLUDED.miles,
                      miles_currency = EXCLUDED.miles_currency,
                      tax = EXCLUDED.tax,
                      tax_currency = EXCLUDED.tax_currency,
                      seats_available = EXCLUDED.seats_available,
                      fare_family = EXCLUDED.fare_family,
                      flight_details_path = EXCLUDED.flight_details_path,
                      updated_at = now()
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


# =========================
# Example usage (CLI style)
# =========================

def example_ingest_from_file(
    *,
    dsn: str,
    json_path: str,
    source: str = "AF",
    origin: str = "PAR",
    destination: str = "JNB",
    depart_date_str: str = "2026-02-27",
    cabin_requested: str = "ECONOMY",
    market: str = "FR",
    language: str = "en",
    search_state_uuid: Optional[str] = None,
) -> None:
    depart_dt = datetime.strptime(depart_date_str, "%Y-%m-%d").date()

    with open(json_path, "r", encoding="utf-8") as f:
        body = json.load(f)

    with psycopg.connect(dsn) as conn:
        init_db(conn)

        scan_run_id = create_scan_run(
            conn,
            source=source,
            market=market,
            language=language,
            origin=origin,
            destination=destination,
            cabin_requested=cabin_requested,
            depart_date=depart_dt,
            search_state_uuid=search_state_uuid,
        )

        store_raw_response(
            conn,
            scan_run_id=scan_run_id,
            source=source,
            operation_name="SearchResultAvailableOffersQuery",
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin_requested,
            search_state_uuid=search_state_uuid,
            body=body,
        )

        offers = parse_search_result_available_offers(
            body,
            source=source,
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin_requested,
        )

        upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)

        print(f"Inserted/updated {len(offers)} offers for {origin}->{destination} on {depart_dt}.")


if __name__ == "__main__":
    # Example:
    # example_ingest_from_file(
    #   dsn="postgresql://user:pass@localhost:5432/awards",
    #   json_path="/path/to/SearchResultAvailableOffersQuery.json"
    # )
    pass