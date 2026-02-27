"""
Partner Awards Air France service – orchestrates ingestion.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from .adapter import (
    create_scan_run,
    init_db,
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
