#!/usr/bin/env python3
"""
Import JSON outputs from partner_awards_remote_runner into partner_awards.sqlite.
Usage:
  python -m partner_awards.airfrance.import_folder --path /path/to/outputs/AF/PAR-JNB/2026-02-27
Derives origin/destination/date/cabin from folder structure or filename.
Does NOT require Flask to be running.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from partner_awards.airfrance.adapter import (
    create_scan_run,
    init_db,
    ingest_lowest_fares,
    parse_search_result_available_offers,
    store_raw_response,
    upsert_offers,
)
from partner_awards.airfrance.routes import PARTNER_DB_DIR, PARTNER_DB_PATH


def _is_available_offers_response(data: dict) -> bool:
    """Check if JSON looks like SearchResultAvailableOffersQuery response (wrapped or raw)."""
    if not isinstance(data, dict):
        return False
    body = data.get("body") if "body" in data else data
    if not isinstance(body, dict):
        return False
    offers = (body.get("data") or {}).get("availableOffers") or {}
    if not isinstance(offers, dict):
        return False
    return "offerItineraries" in offers


def _parse_route_date_from_path(path: Path) -> tuple[str, str, str] | None:
    """
    Parse outputs/AF/PAR-JNB/2026-02-27 or AF/PAR-JNB/2026-02-27.
    Returns (origin, destination, date) or None.
    """
    parts = path.parts
    for i, p in enumerate(parts):
        if p == "AF" and i + 2 < len(parts):
            route = parts[i + 1]
            date_str = parts[i + 2]
            m = re.match(r"^([A-Z]{2,4})-([A-Z]{2,4})$", route)
            if m and re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                return m.group(1), m.group(2), date_str
    return None


def _parse_route_month_from_path(path: Path) -> tuple[str, str, str] | None:
    """
    Parse outputs/AF/AMS-JNB/2026-03 (month folder).
    Returns (origin, destination, month_yyyy_mm) or None.
    """
    parts = path.parts
    for i, p in enumerate(parts):
        if p == "AF" and i + 2 < len(parts):
            route = parts[i + 1]
            month_str = parts[i + 2]
            m = re.match(r"^([A-Z]{2,4})-([A-Z]{2,4})$", route)
            if m and re.match(r"^\d{4}-\d{2}$", month_str):
                return m.group(1), m.group(2), month_str
    return None


def _parse_cabin_from_filename(name: str) -> str:
    """Extract cabin from available_offers_ECONOMY_20260227_123456.json"""
    m = re.search(r"available_offers_([A-Z]+)_", name)
    return m.group(1) if m else "ECONOMY"


def _read_meta(folder: Path) -> tuple[str | None, str | None]:
    """Read .meta.json from folder. Returns (host_used, host_attempts_json) or (None, None)."""
    meta = _read_meta_full(folder)
    if not meta:
        return None, None
    host_used = meta.get("host_used")
    host_attempts = json.dumps(meta.get("host_attempts", [])) if meta.get("host_attempts") else None
    return host_used, host_attempts


def _read_meta_full(folder: Path) -> dict | None:
    """Read full .meta.json. Returns dict or None."""
    meta_path = folder / ".meta.json"
    if not meta_path.exists():
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _meta_search(meta: dict | None) -> dict:
    """Extract search block from meta. Supports schema v1 (search.*) and legacy (top-level)."""
    if not meta:
        return {}
    search = meta.get("search") or {}
    if not isinstance(search, dict):
        search = {}
    # Legacy: origin/destination/cabins at top level
    out = dict(search)
    if meta.get("origin") and not out.get("origin"):
        out["origin"] = meta["origin"]
    if meta.get("destination") and not out.get("destination"):
        out["destination"] = meta["destination"]
    if meta.get("cabins") and not out.get("cabins"):
        out["cabins"] = meta["cabins"]
    if meta.get("start_date") and not out.get("start_date"):
        out["start_date"] = meta["start_date"]
    if meta.get("end_date") and not out.get("end_date"):
        out["end_date"] = meta["end_date"]
    return out


def _is_lowest_fares_from_file(filepath: Path) -> bool:
    """Peek at file to see if it's LowestFareOffers (supports wrapped format)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _is_lowest_fares_response(data) if isinstance(data, dict) else False
    except Exception:
        return False


def _parse_cabins_from_lowest_fares_filename(name: str) -> list[str]:
    """Extract cabins from lowest_fares_ECONOMY_BUSINESS_20260301_120000.json or lowest_fares_MONTH_BUSINESS_xxx.json"""
    m = re.search(r"lowest_fares_(?:MONTH_)?([A-Z_]+)_\d", name)
    if m:
        cabins = [c for c in m.group(1).split("_") if c and c != "MONTH" and c in ("ECONOMY", "PREMIUM", "BUSINESS")]
        return cabins if cabins else ["ECONOMY"]
    return ["ECONOMY"]


def ingest_file(
    conn: sqlite3.Connection,
    filepath: Path,
    origin: str,
    destination: str,
    depart_date: str,
    cabin: str,
    host_used: str | None = None,
    host_attempts: str | None = None,
) -> tuple[bool, int, int | None]:
    """
    Ingest a single JSON file (AvailableOffers). Returns (ok, inserted_count, scan_run_id).
    Supports wrapped {"body": {...}} format.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not _is_available_offers_response(data):
        return False, 0, None

    body = data.get("body", data) if isinstance(data, dict) and "body" in data else data
    depart_dt = datetime.strptime(depart_date, "%Y-%m-%d").date()
    source = "AF"

    init_db(conn)
    scan_run_id = create_scan_run(
        conn,
        source=source,
        ingest_type="remote_runner",
        origin=origin,
        destination=destination,
        cabin_requested=cabin,
        depart_date=depart_dt,
        host_used=host_used,
        host_attempts=host_attempts,
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
    return True, count, scan_run_id


def _unwrap_payload(data: dict) -> tuple[dict, str | None]:
    """Extract body from wrapped JSON. Returns (body, operationName)."""
    if "body" in data:
        return data["body"], data.get("operationName")
    return data, None


def _is_lowest_fares_response(data: dict) -> bool:
    """Check if JSON is LowestFareOffers (wrapped or raw)."""
    body, op = _unwrap_payload(data)
    if op in ("SharedSearchLowestFareOffersForSearchQuery", "SharedSearchLowestFareOffersByResourceIdForSearchQuery"):
        return True
    return isinstance(body, dict) and "data" in body and isinstance(
        (body.get("data") or {}).get("lowestFareOffers"), dict
    )


def ingest_lowest_fares_file(
    conn: sqlite3.Connection,
    filepath: Path,
    folder: Path,
    origin: str,
    destination: str,
    host_used: str | None = None,
    host_attempts: str | None = None,
    scan_run_id: int | None = None,
) -> tuple[bool, int, int | None]:
    """
    Ingest a lowest_fares_*.json file. Returns (ok, inserted_count, scan_run_id).
    If scan_run_id provided, reuses it (one scan_run per folder).
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return False, 0, None
    body, _ = _unwrap_payload(data)
    payload = body if body else data
    if not isinstance(payload, dict) or "data" not in payload:
        return False, 0, None
    if not _is_lowest_fares_response(data):
        return False, 0, None

    meta = _read_meta_full(folder)
    search = _meta_search(meta)
    origin = search.get("origin") or (meta or {}).get("origin") or origin
    destination = search.get("destination") or (meta or {}).get("destination") or destination
    # Prefer cabin from filename so mixed BUSINESS/PREMIUM folder uses correct cabin per file
    cabins_from_file = _parse_cabins_from_lowest_fares_filename(filepath.name)
    cabins = cabins_from_file if (cabins_from_file and cabins_from_file != ["ECONOMY"]) else (search.get("cabins") or (meta or {}).get("cabins") or cabins_from_file)
    if not isinstance(cabins, list):
        cabins = [cabins] if cabins else ["ECONOMY"]

    init_db(conn)
    if scan_run_id is None:
        scan_run_id = create_scan_run(
            conn,
            source="AF",
            ingest_type="remote_runner",
            origin=origin,
            destination=destination,
            cabin_requested=",".join(cabins),
            depart_date=None,
            host_used=host_used,
            host_attempts=host_attempts,
        )
        store_raw_response(
            conn,
            scan_run_id=scan_run_id,
            source="AF",
            operation_name="SharedSearchLowestFareOffersForSearchQuery",
            origin=origin,
            destination=destination,
            depart_date=None,
            cabin_requested=",".join(cabins),
            body=payload,
        )

    count = ingest_lowest_fares(
        conn,
        scan_run_id=scan_run_id,
        payload=payload,
        origin=origin,
        destination=destination,
        cabins=cabins,
        host_used=host_used,
    )
    return True, count, scan_run_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Import remote runner JSON outputs into partner_awards DB")
    parser.add_argument("--path", required=True, help="Path to outputs/AF/PAR-JNB/2026-02-27 or parent")
    args = parser.parse_args()

    path = Path(args.path).resolve()
    if not path.exists():
        print(f"Error: path not found: {path}")
        return 1

    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(PARTNER_DB_PATH)

    total_files = 0
    total_inserted = 0
    scan_run_ids: list[int] = []

    try:
        # If path is a directory like outputs/AF/PAR-JNB/2026-02-27 or outputs/AF/AMS-JNB/2026-03
        if path.is_dir():
            route_date = _parse_route_date_from_path(path)
            route_month = _parse_route_month_from_path(path)
            if route_date:
                origin, destination, depart_date = route_date
                host_used, host_attempts = _read_meta(path)
                meta = _read_meta_full(path)
                search = _meta_search(meta)
                origin = search.get("origin") or (meta or {}).get("origin") or origin
                destination = search.get("destination") or (meta or {}).get("destination") or destination
                scan_run_id = None
                for f in sorted(path.glob("*.json")):
                    if f.name == ".meta.json":
                        continue
                    if f.name.startswith("lowest_fares_") or _is_lowest_fares_from_file(f):
                        ok, count, sid = ingest_lowest_fares_file(conn, f, path, origin, destination, host_used, host_attempts, scan_run_id)
                        if ok:
                            if scan_run_id is None:
                                scan_run_id = sid
                            conn.commit()
                            total_files += 1
                            total_inserted += count
                            if sid:
                                scan_run_ids.append(sid)
                            print(f"  {f.name}: {count} calendar fares (scan_run_id={sid})")
                        continue
                    cabin = _parse_cabin_from_filename(f.name)
                    ok, count, sid = ingest_file(conn, f, origin, destination, depart_date, cabin, host_used, host_attempts)
                    if ok:
                        conn.commit()
                        total_files += 1
                        total_inserted += count
                        if sid:
                            scan_run_ids.append(sid)
                        print(f"  {f.name}: {count} offers (scan_run_id={sid})")
                    else:
                        print(f"  {f.name}: skipped (not AvailableOffers format)")
            elif route_month:
                origin, destination, month_str = route_month
                host_used, host_attempts = _read_meta(path)
                meta = _read_meta_full(path)
                search = _meta_search(meta)
                origin = search.get("origin") or (meta or {}).get("origin") or origin
                destination = search.get("destination") or (meta or {}).get("destination") or destination
                scan_run_id = None
                for f in sorted(path.glob("*.json")):
                    if f.name == ".meta.json":
                        continue
                    if f.name.startswith("lowest_fares_") or _is_lowest_fares_from_file(f):
                        ok, count, sid = ingest_lowest_fares_file(conn, f, path, origin, destination, host_used, host_attempts, scan_run_id)
                        if ok:
                            if scan_run_id is None:
                                scan_run_id = sid
                            conn.commit()
                            total_files += 1
                            total_inserted += count
                            if sid:
                                scan_run_ids.append(sid)
                            print(f"  {f.name}: {count} calendar fares (scan_run_id={sid})")
                # Month folders typically have no available_offers
            else:
                # Recurse: path might be outputs/ or outputs/AF/
                for sub in sorted(path.rglob("*.json")):
                    parent = sub.parent
                    route_date = _parse_route_date_from_path(parent)
                    route_month = _parse_route_month_from_path(parent)
                    meta = _read_meta_full(parent)
                    has_meta = (parent / ".meta.json").exists()
                    if route_date or route_month or has_meta:
                        search = _meta_search(meta)
                        route = route_date or route_month
                        o, d = (route or ("", "", ""))[0], (route or ("", "", ""))[1]
                        dd = (route_date or ("", "", ""))[2] if route_date else ""
                        origin = search.get("origin") or (meta or {}).get("origin") or o
                        destination = search.get("destination") or (meta or {}).get("destination") or d
                        depart_date = dd or search.get("start_date") or ((search.get("end_date") or "")[:10] if search.get("end_date") else "")
                        if not origin or not destination:
                            continue
                        if sub.name.startswith("lowest_fares_") or _is_lowest_fares_from_file(sub):
                            ok, count, sid = ingest_lowest_fares_file(conn, sub, parent, origin, destination, *_read_meta(parent))
                            if ok:
                                conn.commit()
                                total_files += 1
                                total_inserted += count
                                if sid:
                                    scan_run_ids.append(sid)
                                print(f"  {sub.relative_to(path)}: {count} calendar fares (scan_run_id={sid})")
                            continue
                        cabin = _parse_cabin_from_filename(sub.name)
                        host_used, host_attempts = _read_meta(parent)
                        ok, count, sid = ingest_file(conn, sub, origin, destination, depart_date, cabin, host_used, host_attempts)
                        if ok:
                            conn.commit()
                            total_files += 1
                            total_inserted += count
                            if sid:
                                scan_run_ids.append(sid)
                            print(f"  {sub.relative_to(path)}: {count} offers (scan_run_id={sid})")
        else:
            # Single file: try to derive from parent path
            parent = path.parent
            route_date = _parse_route_date_from_path(parent)
            if not route_date:
                print("Error: cannot derive origin/destination/date from path. Use outputs/AF/PAR-JNB/2026-02-27/")
                return 1
            origin, destination, depart_date = route_date
            cabin = _parse_cabin_from_filename(path.name)
            host_used, host_attempts = _read_meta(parent)
            ok, count, sid = ingest_file(conn, path, origin, destination, depart_date, cabin, host_used, host_attempts)
            if ok:
                conn.commit()
                total_files = 1
                total_inserted = count
                if sid:
                    scan_run_ids.append(sid)
                print(f"  {path.name}: {count} offers (scan_run_id={sid})")
            else:
                print("Error: file is not SearchResultAvailableOffersQuery format")
                return 1
    finally:
        conn.close()

    print(f"\nSummary: {total_files} files, {total_inserted} offers ingested. scan_run_ids: {scan_run_ids}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
