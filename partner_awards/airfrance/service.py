"""
Partner Awards Air France service – orchestrates ingestion.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from .adapter import (
    create_scan_run,
    init_db,
    ingest_lowest_fares,
    parse_search_result_available_offers,
    store_raw_response,
    upsert_offers,
)

# Default fixture path (relative to project root)
DEFAULT_FIXTURE_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "airfrance" / "SearchResultAvailableOffersQuery.json"


def ingest_fixture(
    conn,
    *,
    origin: str = "PAR",
    destination: str = "JNB",
    depart_date: str = "2026-02-27",
    cabin_requested: str = "ECONOMY",
    fixture_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Ingest from fixture JSON file. Returns summary with inserted_offer_count and sample offers.

    TODO: Implement live fetch using the 3 GraphQL calls in docs/airfrance-awards-api.md:
      1. SharedSearchCreateSearchContextForSearchQuery (get searchStateUuid)
      2. SharedSearchLowestFareOffersForSearchQuery (calendar view)
      3. SearchResultAvailableOffersQuery (flight list; sha256Hash: 6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a)
    searchStateUuid is required for all reward queries.
    """
    path = fixture_path or DEFAULT_FIXTURE_PATH
    if not path.exists():
        return {
            "ok": False,
            "error": f"Fixture file not found: {path}",
            "inserted_offer_count": 0,
            "offers": [],
        }

    with open(path, "r", encoding="utf-8") as f:
        body = json.load(f)

    depart_dt = date.fromisoformat(depart_date)
    source = "AF"

    init_db(conn)

    scan_run_id = create_scan_run(
        conn,
        source=source,
        ingest_type="fixture",
        origin=origin,
        destination=destination,
        cabin_requested=cabin_requested,
        depart_date=depart_dt,
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

    count = upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)

    sample = _offers_to_dict(offers[:10])

    return {
        "ok": True,
        "inserted_offer_count": count,
        "offers": sample,
    }


def _offers_to_dict(offers: List) -> List[Dict[str, Any]]:
    """Convert Offer objects to JSON-serializable dicts."""
    out = []
    for o in offers:
        cabins = {}
        for k, v in o.cabins.items():
            cabins[k] = {
                "miles": v.miles,
                "tax": v.tax,
                "seats_available": v.seats_available,
                "fare_family": v.fare_family,
            }
        out.append({
            "source": o.source,
            "origin": o.origin,
            "destination": o.destination,
            "depart_date": o.depart_date.isoformat(),
            "stops": o.stops,
            "duration_minutes": o.duration_minutes,
            "segments": o.segments,
            "cabins": cabins,
        })
    return out


# GraphQL API constants (docs/airfrance-awards-api.md)
AF_GQL_URL = "https://wwws.airfrance.fr/gql/v1"
AF_BASE = "https://wwws.airfrance.fr"
AF_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": AF_BASE,
    "referer": f"{AF_BASE}/en/search/open-dates/0",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "afkl-travel-country": "FR",
    "afkl-travel-language": "en",
    "afkl-travel-market": "FR",
    "afkl-travel-host": "AF",
}
# KLM.se – SharedSearchLowestFareOffersForSearchQuery; can be tried without login (no CreateSearchContext).
# Set PARTNER_AWARDS_KLM_NO_LOGIN=1 to enable; disabled by default (often does not work in practice).
KLM_NO_LOGIN_ENABLED = os.environ.get("PARTNER_AWARDS_KLM_NO_LOGIN", "").strip() in ("1", "true", "yes")
KLM_GQL_URL = "https://www.klm.se/gql/v1"
KLM_BASE = "https://www.klm.se"
KLM_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": KLM_BASE,
    "referer": f"{KLM_BASE}/en/search/open-dates/0",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "afkl-travel-country": "SE",
    "afkl-travel-language": "en",
    "afkl-travel-market": "SE",
    "afkl-travel-host": "KL",
}
# Separate timeouts: connect 10s, read 60s (detect hang vs slow response)
HTTP_CONNECT_TIMEOUT = 10.0
HTTP_READ_TIMEOUT = 60.0
CREATE_CONTEXT_HASH = "54e5576492358745ae7ee183605ca00eee645cfcd2bc557fedc124cb32140f65"
LOWEST_FARE_HASH = "3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243"
AVAILABLE_OFFERS_HASH = "6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a"


def _timed_post(
    url: str,
    json_payload: Dict[str, Any],
    log_prefix: str = "",
    headers: Optional[Dict[str, str]] = None,
) -> tuple:
    """
    POST with httpx, separate connect/read timeouts.
    Returns (response_or_none, error_str_or_none, timing_dict).
    timing_dict: connect_ms, read_ms, total_ms, phase (where it hung if error).
    """
    log = logging.getLogger(__name__)
    h = headers if headers is not None else AF_HEADERS
    timing: Dict[str, Any] = {}
    t0 = time.perf_counter()
    try:
        with httpx.Client(
            timeout=httpx.Timeout(HTTP_READ_TIMEOUT, connect=HTTP_CONNECT_TIMEOUT),
            follow_redirects=True,
            trust_env=False,
        ) as client:
            r = client.post(url, headers=h, json=json_payload)
            t_done = time.perf_counter()
        timing["connect_ms"] = 0  # httpx doesn't expose connect vs read separately
        timing["total_ms"] = round((t_done - t0) * 1000)
        timing["read_ms"] = timing["total_ms"]
        timing["phase"] = "ok"
        if log_prefix:
            log.info("%s: total=%dms status=%d", log_prefix, timing["total_ms"], r.status_code)
        return r, None, timing
    except httpx.ConnectTimeout as e:
        timing["connect_ms"] = round((time.perf_counter() - t0) * 1000)
        timing["phase"] = "connect_timeout"
        timing["total_ms"] = timing["connect_ms"]
        log.warning("%s: CONNECT TIMEOUT after %dms", log_prefix or "POST", timing["connect_ms"])
        return None, str(e), timing
    except httpx.ReadTimeout as e:
        timing["connect_ms"] = round((time.perf_counter() - t0) * 1000)
        timing["phase"] = "read_timeout"
        timing["total_ms"] = round((time.perf_counter() - t0) * 1000)
        log.warning("%s: READ TIMEOUT (total=%dms)", log_prefix or "POST", timing["total_ms"])
        return None, str(e), timing
    except Exception as e:
        timing["phase"] = "error"
        timing["total_ms"] = round((time.perf_counter() - t0) * 1000)
        log.exception("%s: %s", log_prefix or "POST", e)
        return None, str(e), timing


def sanity_check() -> Dict[str, Any]:
    """
    Connectivity sanity check: GET home page and GET gql endpoint.
    If both hang, it's network/proxy/DNS—not login.
    """
    log = logging.getLogger(__name__)
    results: Dict[str, Any] = {"home": None, "gql": None}
    t0 = time.perf_counter()

    try:
        with httpx.Client(timeout=httpx.Timeout(15.0, connect=10.0), trust_env=False) as client:
            r = client.get(f"{AF_BASE}/en/")
        results["home"] = {
            "status": r.status_code,
            "duration_ms": round((time.perf_counter() - t0) * 1000),
            "ok": r.status_code == 200,
        }
        log.info("Sanity home: status=%d duration=%dms", r.status_code, results["home"]["duration_ms"])
    except httpx.ConnectTimeout as e:
        results["home"] = {"status": None, "error": "connect_timeout", "detail": str(e), "ok": False}
        log.warning("Sanity home: CONNECT TIMEOUT")
    except httpx.ReadTimeout as e:
        results["home"] = {"status": None, "error": "read_timeout", "detail": str(e), "ok": False}
        log.warning("Sanity home: READ TIMEOUT")
    except Exception as e:
        results["home"] = {"status": None, "error": type(e).__name__, "detail": str(e), "ok": False}
        log.exception("Sanity home: %s", e)

    t1 = time.perf_counter()
    try:
        with httpx.Client(timeout=httpx.Timeout(15.0, connect=10.0), trust_env=False) as client:
            r = client.get(AF_GQL_URL)
        results["gql"] = {
            "status": r.status_code,
            "duration_ms": round((time.perf_counter() - t1) * 1000),
            "ok": r.status_code in (200, 400, 405),
        }
        log.info("Sanity gql: status=%d duration=%dms", r.status_code, results["gql"]["duration_ms"])
    except httpx.ConnectTimeout as e:
        results["gql"] = {"status": None, "error": "connect_timeout", "detail": str(e), "ok": False}
        log.warning("Sanity gql: CONNECT TIMEOUT")
    except httpx.ReadTimeout as e:
        results["gql"] = {"status": None, "error": "read_timeout", "detail": str(e), "ok": False}
        log.warning("Sanity gql: READ TIMEOUT")
    except Exception as e:
        results["gql"] = {"status": None, "error": type(e).__name__, "detail": str(e), "ok": False}
        log.exception("Sanity gql: %s", e)

    results["total_ms"] = round((time.perf_counter() - t0) * 1000)
    results["proxy_env"] = {
        "HTTP_PROXY": os.environ.get("HTTP_PROXY", ""),
        "HTTPS_PROXY": os.environ.get("HTTPS_PROXY", ""),
        "ALL_PROXY": os.environ.get("ALL_PROXY", ""),
        "NO_PROXY": os.environ.get("NO_PROXY", ""),
    }
    return results


def live_test(
    conn,
    *,
    origin: str = "PAR",
    destination: str = "JNB",
    depart_date: str = "2026-02-27",
    cabin: str = "ECONOMY",
) -> Dict[str, Any]:
    """
    Minimal live test: CreateSearchContext → SearchResultAvailableOffersQuery.
    Single-threaded, 1–2s sleep between calls.
    Returns http status codes, inserted_offer_count, cabins_include_business, timing.
    """
    log = logging.getLogger(__name__)
    search_state_uuid = str(uuid.uuid4())
    depart_dt = date.fromisoformat(depart_date)
    source = "AF"

    init_db(conn)

    # 1. Create search context
    create_payload = {
        "operationName": "SharedSearchCreateSearchContextForSearchQuery",
        "variables": {"searchStateUuid": search_state_uuid},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": CREATE_CONTEXT_HASH}},
    }
    r1, err1, timing1 = _timed_post(AF_GQL_URL, create_payload, "CreateSearchContext")
    if err1:
        return {
            "ok": False,
            "create_context_status": None,
            "create_context_error": err1,
            "create_context_timing": timing1,
            "phase_hung": timing1.get("phase"),
            "offers_status": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }
    create_status = r1.status_code
    if create_status != 200:
        return {
            "ok": False,
            "create_context_status": create_status,
            "create_context_error": r1.text[:2000],
            "create_context_timing": timing1,
            "phase_hung": None,
            "offers_status": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }

    time.sleep(1.5)

    # 2. SearchResultAvailableOffersQuery
    offers_payload = {
        "operationName": "SearchResultAvailableOffersQuery",
        "variables": {
            "activeConnectionIndex": 0,
            "bookingFlow": "REWARD",
            "availableOfferRequestBody": {
                "commercialCabins": [cabin],
                "passengers": [{"id": 1, "type": "ADT"}],
                "requestedConnections": [
                    {
                        "origin": {"code": origin, "type": "CITY"},
                        "destination": {"code": destination, "type": "AIRPORT"},
                        "departureDate": depart_date,
                    },
                    {
                        "origin": {"code": destination, "type": "AIRPORT"},
                        "destination": {"code": origin, "type": "CITY"},
                        "dateInterval": None,
                    },
                ],
                "bookingFlow": "REWARD",
                "customer": {
                    "selectedTravelCompanions": [
                        {"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}
                    ]
                },
            },
            "searchStateUuid": search_state_uuid,
        },
        "extensions": {
            "persistedQuery": {"version": 1, "sha256Hash": AVAILABLE_OFFERS_HASH}
        },
    }
    r2, err2, timing2 = _timed_post(AF_GQL_URL, offers_payload, "SearchResultAvailableOffers")
    if err2:
        return {
            "ok": False,
            "create_context_status": create_status,
            "create_context_error": None,
            "create_context_timing": timing1,
            "offers_status": None,
            "offers_error": err2,
            "offers_timing": timing2,
            "phase_hung": timing2.get("phase"),
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }
    offers_status = r2.status_code
    if offers_status != 200:
        return {
            "ok": False,
            "create_context_status": create_status,
            "create_context_error": None,
            "create_context_timing": timing1,
            "offers_status": offers_status,
            "offers_error": r2.text[:2000],
            "offers_timing": timing2,
            "phase_hung": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }
    body = r2.json()

    # 3. Store raw + parse + upsert
    scan_run_id = create_scan_run(
        conn,
        source=source,
        ingest_type="httpx_legacy",
        origin=origin,
        destination=destination,
        cabin_requested=cabin,
        depart_date=depart_dt,
    )
    store_raw_response(
        conn,
        scan_run_id=scan_run_id,
        source=source,
        operation_name="SearchResultAvailableOffersQuery",
        origin=origin,
        destination=destination,
        depart_date=depart_dt,
        cabin_requested=cabin,
        body=body,
    )
    offers = parse_search_result_available_offers(
        body,
        source=source,
        origin=origin,
        destination=destination,
        depart_date=depart_dt,
        cabin_requested=cabin,
    )
    count = upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)

    has_business = any(
        "BUSINESS" in o.cabins and (o.cabins["BUSINESS"].miles is not None or o.cabins["BUSINESS"].seats_available is not None)
        for o in offers
    )

    # Quick COUNT for stability checks
    cur = conn.execute(
        "SELECT COUNT(*) FROM partner_award_offers WHERE origin=? AND destination=? AND depart_date=?",
        (origin, destination, depart_date),
    )
    offers_in_db_for_date = cur.fetchone()[0]

    return {
        "ok": True,
        "create_context_status": create_status,
        "create_context_timing": timing1,
        "offers_status": offers_status,
        "offers_timing": timing2,
        "inserted_offer_count": count,
        "offers_in_db_for_date": offers_in_db_for_date,
        "cabins_include_business": has_business,
        "scan_run_id": scan_run_id,
    }


def live_test_playwright(
    conn,
    *,
    origin: str = "PAR",
    destination: str = "JNB",
    depart_date: str = "2026-02-27",
    cabin: str = "ECONOMY",
) -> Dict[str, Any]:
    """
    Playwright-based live fetch: homepage warmup → CreateSearchContext → SearchResultAvailableOffersQuery.
    Primary live path; httpx live-test is legacy/unstable.
    """
    from datetime import datetime

    from .live_playwright import fetch_homepage, post_gql

    log = logging.getLogger(__name__)
    search_state_uuid = str(uuid.uuid4())
    depart_dt = datetime.strptime(depart_date, "%Y-%m-%d").date()
    source = "AF"

    init_db(conn)

    # Step 0: homepage warmup (request-only; page.goto fails with HTTP2 protocol error on some networks)
    home_status, home_timing_ms, home_err = fetch_homepage()
    if home_err or home_status != 200:
        return {
            "ok": False,
            "error": f"Homepage unreachable: {home_err or f'status {home_status}'}",
            "homepage_status": home_status,
            "homepage_timing_ms": home_timing_ms,
            "create_context_status": None,
            "create_context_timing_ms": None,
            "offers_status": None,
            "offers_timing_ms": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
            "scan_run_id": None,
        }

    # Create scan_run before we have offers (we'll store both raw responses)
    scan_run_id = create_scan_run(
        conn,
        source=source,
        ingest_type="playwright_live",
        origin=origin,
        destination=destination,
        cabin_requested=cabin,
        depart_date=depart_dt,
    )

    def _retry_gql(payload: Dict, op_name: str) -> tuple:
        status, data, timing_ms, err = post_gql(payload, op_name)
        if err:
            return status, data, timing_ms, err
        if status in (403, 429, 503):
            log.warning("%s returned %s, retrying after 3s", op_name, status)
            time.sleep(3)
            return post_gql(payload, op_name)
        return status, data, timing_ms, err

    # Step 1: CreateSearchContext
    create_payload = {
        "operationName": "SharedSearchCreateSearchContextForSearchQuery",
        "variables": {"searchStateUuid": search_state_uuid},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": CREATE_CONTEXT_HASH}},
    }
    ctx_status, ctx_data, ctx_timing_ms, ctx_err = _retry_gql(create_payload, "CreateSearchContext")
    if ctx_data:
        store_raw_response(
            conn,
            scan_run_id=scan_run_id,
            source=source,
            operation_name="SharedSearchCreateSearchContextForSearchQuery",
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin,
            body=ctx_data,
        )
    if ctx_err or ctx_status != 200:
        return {
            "ok": False,
            "error": ctx_err or f"CreateSearchContext status {ctx_status}",
            "homepage_status": home_status,
            "homepage_timing_ms": home_timing_ms,
            "create_context_status": ctx_status,
            "create_context_timing_ms": ctx_timing_ms,
            "offers_status": None,
            "offers_timing_ms": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
            "scan_run_id": scan_run_id,
        }

    time.sleep(1.5)

    # Step 2: SearchResultAvailableOffersQuery
    offers_payload = {
        "operationName": "SearchResultAvailableOffersQuery",
        "variables": {
            "activeConnectionIndex": 0,
            "bookingFlow": "REWARD",
            "availableOfferRequestBody": {
                "commercialCabins": [cabin],
                "passengers": [{"id": 1, "type": "ADT"}],
                "requestedConnections": [
                    {"origin": {"code": origin, "type": "CITY"}, "destination": {"code": destination, "type": "AIRPORT"}, "departureDate": depart_date},
                    {"origin": {"code": destination, "type": "AIRPORT"}, "destination": {"code": origin, "type": "CITY"}, "dateInterval": None},
                ],
                "bookingFlow": "REWARD",
                "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
            },
            "searchStateUuid": search_state_uuid,
        },
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": AVAILABLE_OFFERS_HASH}},
    }
    off_status, off_data, off_timing_ms, off_err = _retry_gql(offers_payload, "SearchResultAvailableOffers")
    if off_data:
        store_raw_response(
            conn,
            scan_run_id=scan_run_id,
            source=source,
            operation_name="SearchResultAvailableOffersQuery",
            origin=origin,
            destination=destination,
            depart_date=depart_dt,
            cabin_requested=cabin,
            body=off_data,
        )
    if off_err or off_status != 200:
        return {
            "ok": False,
            "error": off_err or f"SearchResultAvailableOffers status {off_status}",
            "homepage_status": home_status,
            "homepage_timing_ms": home_timing_ms,
            "create_context_status": ctx_status,
            "create_context_timing_ms": ctx_timing_ms,
            "offers_status": off_status,
            "offers_timing_ms": off_timing_ms,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
            "scan_run_id": scan_run_id,
        }

    offers = parse_search_result_available_offers(
        off_data,
        source=source,
        origin=origin,
        destination=destination,
        depart_date=depart_dt,
        cabin_requested=cabin,
    )
    count = upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)
    cur = conn.execute(
        "SELECT COUNT(*) FROM partner_award_offers WHERE origin=? AND destination=? AND depart_date=?",
        (origin, destination, depart_date),
    )
    offers_in_db_for_date = cur.fetchone()[0]
    has_business = any(
        "BUSINESS" in o.cabins and (o.cabins["BUSINESS"].miles is not None or o.cabins["BUSINESS"].seats_available is not None)
        for o in offers
    )
    return {
        "ok": True,
        "homepage_status": home_status,
        "homepage_timing_ms": home_timing_ms,
        "create_context_status": ctx_status,
        "create_context_timing_ms": ctx_timing_ms,
        "offers_status": off_status,
        "offers_timing_ms": off_timing_ms,
        "inserted_offer_count": count,
        "offers_in_db_for_date": offers_in_db_for_date,
        "cabins_include_business": has_business,
        "scan_run_id": scan_run_id,
    }


def live_test_direct(
    conn,
    *,
    origin: str = "PAR",
    destination: str = "JNB",
    depart_date: str = "2026-02-27",
    cabin: str = "ECONOMY",
) -> Dict[str, Any]:
    """
    Diagnostic: skip CreateSearchContext, call only SearchResultAvailableOffersQuery
    with a fresh uuid. Isolates whether CreateSearchContext is the problematic call.
    """
    from datetime import datetime

    search_state_uuid = str(uuid.uuid4())
    depart_dt = datetime.strptime(depart_date, "%Y-%m-%d").date()
    source = "AF"

    init_db(conn)

    offers_payload = {
        "operationName": "SearchResultAvailableOffersQuery",
        "variables": {
            "activeConnectionIndex": 0,
            "bookingFlow": "REWARD",
            "availableOfferRequestBody": {
                "commercialCabins": [cabin],
                "passengers": [{"id": 1, "type": "ADT"}],
                "requestedConnections": [
                    {"origin": {"code": origin, "type": "CITY"}, "destination": {"code": destination, "type": "AIRPORT"}, "departureDate": depart_date},
                    {"origin": {"code": destination, "type": "AIRPORT"}, "destination": {"code": origin, "type": "CITY"}, "dateInterval": None},
                ],
                "bookingFlow": "REWARD",
                "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
            },
            "searchStateUuid": search_state_uuid,
        },
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": AVAILABLE_OFFERS_HASH}},
    }
    r, err, timing = _timed_post(AF_GQL_URL, offers_payload, "SearchResultAvailableOffers(direct)")
    if err:
        return {
            "ok": False,
            "skipped_create_context": True,
            "offers_status": None,
            "offers_error": err,
            "offers_timing": timing,
            "phase_hung": timing.get("phase"),
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }
    if r.status_code != 200:
        return {
            "ok": False,
            "skipped_create_context": True,
            "offers_status": r.status_code,
            "offers_error": r.text[:2000],
            "offers_timing": timing,
            "phase_hung": None,
            "inserted_offer_count": 0,
            "offers_in_db_for_date": 0,
            "cabins_include_business": False,
        }
    body = r.json()
    scan_run_id = create_scan_run(conn, source=source, ingest_type="httpx_legacy", origin=origin, destination=destination, cabin_requested=cabin, depart_date=depart_dt)
    store_raw_response(conn, scan_run_id=scan_run_id, source=source, operation_name="SearchResultAvailableOffersQuery", origin=origin, destination=destination, depart_date=depart_dt, cabin_requested=cabin, body=body)
    offers = parse_search_result_available_offers(body, source=source, origin=origin, destination=destination, depart_date=depart_dt, cabin_requested=cabin)
    count = upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)
    cur = conn.execute(
        "SELECT COUNT(*) FROM partner_award_offers WHERE origin=? AND destination=? AND depart_date=?",
        (origin, destination, depart_date),
    )
    offers_in_db_for_date = cur.fetchone()[0]
    has_business = any(
        "BUSINESS" in o.cabins and (o.cabins["BUSINESS"].miles is not None or o.cabins["BUSINESS"].seats_available is not None)
        for o in offers
    )
    return {
        "ok": True,
        "skipped_create_context": True,
        "offers_status": r.status_code,
        "offers_timing": timing,
        "inserted_offer_count": count,
        "offers_in_db_for_date": offers_in_db_for_date,
        "cabins_include_business": has_business,
        "scan_run_id": scan_run_id,
    }


def _parse_lowest_fare_dates(body: Dict[str, Any], cabins: List[str], max_days: int) -> List[str]:
    """
    Extract candidate departure dates from LowestFareOffers response.
    Returns up to max_days dates with lowest miles (or first available).
    """
    from datetime import datetime, timedelta

    candidates: List[tuple] = []
    # Try common paths for calendar data
    data = body.get("data") or {}
    for key in ("lowestFareOffers", "searchResult", "calendar", "offers"):
        node = data.get(key)
        if not isinstance(node, dict):
            continue
        # Look for date -> miles structure
        for k, v in (node.get("connections") or node.get("days") or node.get("dates") or {}).items():
            if isinstance(v, dict):
                miles = v.get("miles") or v.get("price", {}).get("amount") if isinstance(v.get("price"), dict) else None
                if miles is not None and isinstance(k, str) and len(k) == 10:
                    try:
                        datetime.strptime(k, "%Y-%m-%d")
                        candidates.append((k, int(miles)))
                    except ValueError:
                        pass
        if isinstance(node.get("connections"), list):
            for conn in node["connections"]:
                if isinstance(conn, dict):
                    dt = conn.get("departureDate") or conn.get("date")
                    miles = conn.get("miles") or (conn.get("price") or {}).get("amount") if isinstance(conn.get("price"), dict) else None
                    if dt and miles is not None:
                        candidates.append((str(dt)[:10], int(miles)))
        for item in (node.get("lowestOffers") or []):
            if isinstance(item, dict):
                dt = item.get("flightDate")
                miles = item.get("displayPrice") or item.get("totalPrice") or (item.get("price") or {}).get("amount") if isinstance(item.get("price"), dict) else None
                if dt and miles is not None:
                    candidates.append((str(dt)[:10], int(miles)))

    if not candidates:
        return []
    seen = set()
    unique = [(d, m) for d, m in candidates if d not in seen and not seen.add(d)]
    unique.sort(key=lambda x: (x[1], x[0]))
    return [d for d, _ in unique[:max_days]]


def calendar_scan(
    conn,
    *,
    origin: str = "PAR",
    destination: str = "JNB",
    start_date: str = "2026-02-27",
    days: int = 14,
    cabins: Optional[List[str]] = None,
    max_offer_days: int = 5,
) -> Dict[str, Any]:
    """
    Calendar → drilldown: CreateSearchContext → LowestFareOffers → AvailableOffers for candidate dates.
    """
    log = logging.getLogger(__name__)
    cabins = cabins or ["ECONOMY"]
    search_state_uuid = str(uuid.uuid4())
    from datetime import datetime, timedelta
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = start_dt + timedelta(days=days)
    end_date = end_dt.isoformat()
    date_interval = f"{start_date}/{end_date}"

    init_db(conn)

    # 1. CreateSearchContext
    create_payload = {
        "operationName": "SharedSearchCreateSearchContextForSearchQuery",
        "variables": {"searchStateUuid": search_state_uuid},
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": CREATE_CONTEXT_HASH}},
    }
    try:
        r1, err1, _ = _timed_post(AF_GQL_URL, create_payload, "CreateSearchContext(calendar)")
        if err1 or not r1:
            return {"ok": False, "error": "CreateSearchContext failed", "detail": err1 or "no response"}
        if r1.status_code != 200:
            return {"ok": False, "error": f"CreateSearchContext {r1.status_code}", "detail": r1.text[:2000]}
    except Exception as e:
        return {"ok": False, "error": "CreateSearchContext failed", "detail": str(e)}

    time.sleep(1.5)

    # 2. LowestFareOffers
    lowest_payload = {
        "operationName": "SharedSearchLowestFareOffersForSearchQuery",
        "variables": {
            "lowestFareOffersRequest": {
                "bookingFlow": "REWARD",
                "withUpsellCabins": True,
                "passengers": [{"id": 1, "type": "ADT"}],
                "commercialCabins": cabins,
                "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
                "type": "DAY",
                "requestedConnections": [
                    {"departureDate": start_date, "dateInterval": date_interval, "origin": {"type": "CITY", "code": origin}, "destination": {"type": "AIRPORT", "code": destination}},
                    {"dateInterval": None, "origin": {"type": "AIRPORT", "code": destination}, "destination": {"type": "CITY", "code": origin}},
                ],
            },
            "activeConnection": 0,
            "searchStateUuid": search_state_uuid,
            "bookingFlow": "REWARD",
        },
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": LOWEST_FARE_HASH}},
    }
    try:
        r2, err2, _ = _timed_post(AF_GQL_URL, lowest_payload, "LowestFareOffers")
        if err2 or not r2:
            return {"ok": False, "error": "LowestFareOffers failed", "detail": err2 or "no response"}
        if r2.status_code != 200:
            return {"ok": False, "error": f"LowestFareOffers {r2.status_code}", "detail": r2.text[:2000]}
        cal_body = r2.json()
    except Exception as e:
        return {"ok": False, "error": "LowestFareOffers failed", "detail": str(e)}

    candidate_dates = _parse_lowest_fare_dates(cal_body, cabins, max_offer_days)
    if not candidate_dates:
        candidate_dates = [start_date]  # fallback to first day

    total_inserted = 0
    for depart_date in candidate_dates[:max_offer_days]:
        time.sleep(1.5)
        depart_dt = datetime.strptime(depart_date, "%Y-%m-%d").date()
        offers_payload = {
            "operationName": "SearchResultAvailableOffersQuery",
            "variables": {
                "activeConnectionIndex": 0,
                "bookingFlow": "REWARD",
                "availableOfferRequestBody": {
                    "commercialCabins": cabins,
                    "passengers": [{"id": 1, "type": "ADT"}],
                    "requestedConnections": [
                        {"origin": {"code": origin, "type": "CITY"}, "destination": {"code": destination, "type": "AIRPORT"}, "departureDate": depart_date},
                        {"origin": {"code": destination, "type": "AIRPORT"}, "destination": {"code": origin, "type": "CITY"}, "dateInterval": None},
                    ],
                    "bookingFlow": "REWARD",
                    "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
                },
                "searchStateUuid": search_state_uuid,
            },
            "extensions": {"persistedQuery": {"version": 1, "sha256Hash": AVAILABLE_OFFERS_HASH}},
        }
        try:
            r3, err3, _ = _timed_post(AF_GQL_URL, offers_payload, f"AvailableOffers({depart_date})")
            if err3 or not r3 or r3.status_code != 200:
                log.warning("AvailableOffers %s failed: %s", depart_date, err3 or (f"status={r3.status_code}" if r3 else "no response"))
                continue
            body = r3.json()
        except Exception as e:
            log.warning("AvailableOffers %s failed: %s", depart_date, e)
            continue

        scan_run_id = create_scan_run(conn, source="AF", ingest_type="httpx_legacy", origin=origin, destination=destination, cabin_requested=cabins[0], depart_date=depart_dt)
        store_raw_response(conn, scan_run_id=scan_run_id, source="AF", operation_name="SearchResultAvailableOffersQuery", origin=origin, destination=destination, depart_date=depart_dt, cabin_requested=cabins[0], body=body)
        offers = parse_search_result_available_offers(body, source="AF", origin=origin, destination=destination, depart_date=depart_dt, cabin_requested=cabins[0])
        total_inserted += upsert_offers(conn, offers=offers, scan_run_id=scan_run_id)

    return {
        "ok": True,
        "candidate_dates": candidate_dates[:max_offer_days],
        "dates_fetched": len(candidate_dates[:max_offer_days]),
        "inserted_offer_count": total_inserted,
    }


def calendar_scan_klm_no_login(
    conn,
    *,
    origin: str,
    destination: str,
    date_interval: str,
    cabins: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Fetch calendar (LowestFareOffers only) from KLM.se without login.
    Disabled by default; set PARTNER_AWARDS_KLM_NO_LOGIN=1 to enable.
    """
    if not KLM_NO_LOGIN_ENABLED:
        return {
            "ok": False,
            "error": "KLM no-login is disabled",
            "detail": "Set PARTNER_AWARDS_KLM_NO_LOGIN=1 to enable (may not work in practice).",
        }
    log = logging.getLogger(__name__)
    cabins = cabins or ["BUSINESS"]
    search_state_uuid = str(uuid.uuid4())
    init_db(conn)

    # Origin/destination: support AIRPORT or CITY; KLM often uses origin AIRPORT, destination CITY
    def _loc(t: str, code: str) -> Dict[str, str]:
        return {"type": t, "code": code}

    # First leg: outbound (e.g. AMS -> BKK). Second: return (e.g. BKK -> AMS)
    lowest_payload = {
        "operationName": "SharedSearchLowestFareOffersForSearchQuery",
        "variables": {
            "lowestFareOffersRequest": {
                "bookingFlow": "REWARD",
                "withUpsellCabins": True,
                "passengers": [{"id": 1, "type": "ADT"}],
                "commercialCabins": cabins,
                "fareOption": None,
                "type": "MONTH",
                "requestedConnections": [
                    {
                        "dateInterval": date_interval,
                        "origin": _loc("AIRPORT", origin),
                        "destination": _loc("CITY", destination),
                    },
                    {
                        "dateInterval": None,
                        "origin": _loc("CITY", destination),
                        "destination": _loc("AIRPORT", origin),
                    },
                ],
            },
            "activeConnection": 0,
            "searchStateUuid": search_state_uuid,
            "bookingFlow": "REWARD",
        },
        "extensions": {"persistedQuery": {"version": 1, "sha256Hash": LOWEST_FARE_HASH}},
    }

    r, err, timing = _timed_post(
        KLM_GQL_URL,
        lowest_payload,
        "LowestFareOffers(KLM-no-login)",
        headers=KLM_HEADERS,
    )
    if err or not r:
        return {"ok": False, "error": "LowestFareOffers failed", "detail": err or "no response", "timing": timing}
    if r.status_code != 200:
        return {"ok": False, "error": f"LowestFareOffers {r.status_code}", "detail": (r.text or "")[:2000], "timing": timing}
    try:
        cal_body = r.json()
    except Exception as e:
        return {"ok": False, "error": "Invalid JSON", "detail": str(e), "timing": timing}

    scan_run_id = create_scan_run(
        conn,
        source="AF",
        ingest_type="klm_no_login",
        origin=origin,
        destination=destination,
        cabin_requested=cabins[0],
        depart_date=None,
    )
    store_raw_response(
        conn,
        scan_run_id=scan_run_id,
        source="AF",
        operation_name="SharedSearchLowestFareOffersForSearchQuery",
        origin=origin,
        destination=destination,
        depart_date=None,
        cabin_requested=cabins[0],
        body=cal_body,
    )
    inserted = ingest_lowest_fares(
        conn,
        scan_run_id=scan_run_id,
        payload=cal_body,
        origin=origin,
        destination=destination,
        cabins=cabins,
        host_used="klm.se",
        source="AF",
    )

    return {
        "ok": True,
        "inserted_calendar_fares": inserted,
        "scan_run_id": scan_run_id,
        "host": "klm.se",
        "timing": timing,
    }
