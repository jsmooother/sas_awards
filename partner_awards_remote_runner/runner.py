#!/usr/bin/env python3
"""
Remote Fetch Runner for Air France Partner Awards.
Deploy on VPS (Hetzner/OVH/DO) to fetch GraphQL responses blocked from local networks.
Outputs JSON files ingestible by partner_awards/airfrance/import_folder.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure we can import from this package
sys.path.insert(0, str(Path(__file__).resolve().parent))

from airfrance_client_pw import (
    AirFrancePlaywrightClient,
    build_available_offers,
    build_create_context,
    build_lowest_fares,
    _parse_cookie_string,
)

DEFAULT_HOSTS = [
    {"name": "KLM-SE", "base_url": "https://www.klm.se", "brand": "KL", "country": "SE", "language": "en", "market": "SE"},
    {"name": "AF-US", "base_url": "https://wwws.airfrance.us", "brand": "AF", "country": "US", "language": "en", "market": "US"},
]

DEFAULT_CONFIG = {
    "hosts_to_try": DEFAULT_HOSTS,
    "url_booking_flow": "LEISURE",
    "timeout_ms": 60000,
    "pacing_ms": [800, 1800],
    "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "output_dir": "outputs",
    "warmup_timeout_ms": 60000,
    "gql_timeout_ms": 60000,
    "max_retries": 2,
    "retry_backoff_sec": 3,
    "retry_on_status": [403, 429, 503],
    "pacing_delay_sec": [1, 3],
    "force_http1": False,
}


def _load_config() -> Dict[str, Any]:
    cfg_path = Path(__file__).parent / "config.json"
    config = dict(DEFAULT_CONFIG)
    if cfg_path.exists():
        with open(cfg_path, encoding="utf-8") as f:
            config.update(json.load(f))
    if "hosts_to_try" not in config or not config["hosts_to_try"]:
        config["hosts_to_try"] = DEFAULT_HOSTS
    config["user_agent"] = os.environ.get("AF_USER_AGENT", config.get("user_agent", DEFAULT_CONFIG["user_agent"]))
    config["output_dir"] = os.environ.get("AF_OUTPUT_DIR", config.get("output_dir", "outputs"))
    return config


def _load_cookies(cfg: Dict, base_url: str = "https://www.klm.se") -> List[Dict[str, Any]]:
    """Load cookies from config: cookies (list), cookie_string (curl -b), or cookies_file (JSON path).
    For authenticated Flying Blue: export cookies from DevTools when logged in."""
    cookies = cfg.get("cookies")
    if isinstance(cookies, list) and cookies:
        return cookies
    cookie_str = cfg.get("cookie_string") or os.environ.get("AF_COOKIE_STRING")
    if cookie_str:
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        domain = "." + parsed.netloc.lstrip("www.") if parsed.netloc else ".klm.se"
        return _parse_cookie_string(cookie_str, domain=domain)
    cookies_path = cfg.get("cookies_file") or os.environ.get("AF_COOKIES_FILE")
    if cookies_path:
        fp = Path(cookies_path).expanduser().resolve()
        if fp.exists():
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and "cookies" in data:
                return data["cookies"]
    return []


def _url_params_from_host(host: Dict[str, Any], cfg: Dict) -> Dict[str, str]:
    return {
        "bookingFlow": cfg.get("url_booking_flow", "LEISURE"),
        "brand": host.get("brand", "KL"),
        "country": host.get("country", "SE"),
        "language": host.get("language", "en"),
        "market": host.get("market", "SE"),
    }


def _headers_from_host(host: Dict[str, Any], cfg: Dict) -> Dict[str, str]:
    base = host["base_url"].rstrip("/")
    from urllib.parse import urlparse
    parsed = urlparse(base)
    host_header = parsed.netloc or "www.klm.se"
    return {
        "origin": base,
        "referer": f"{base}/en/search/open-dates/0",
        "content-type": "application/json",
        "accept": "application/json",
        "user-agent": cfg.get("user_agent", DEFAULT_CONFIG["user_agent"]),
        "afkl-travel-country": host.get("country", "SE"),
        "afkl-travel-language": host.get("language", "en"),
        "afkl-travel-market": host.get("market", "SE"),
        "afkl-travel-host": host.get("brand", "KL"),
        "x-aviato-host": host_header,
        "country": host.get("country", "SE"),
        "language": host.get("language", "en"),
    }


def _is_retriable(status: int, error: Optional[str], cfg: Dict) -> bool:
    """True if we should try next host (timeout, HTTP2, 403/429/503, invalid json)."""
    if status in cfg.get("retry_on_status", [403, 429, 503]):
        return True
    if status == 0 and error:
        err_lower = error.lower()
        if "timeout" in err_lower or "err_http2" in err_lower or "protocol" in err_lower:
            return True
    return False


def _pacing_ms(cfg: Dict) -> float:
    """Jitter sleep in seconds from pacing_ms [min, max]."""
    pacing = cfg.get("pacing_ms") or [800, 1800]
    if isinstance(pacing, list) and len(pacing) >= 2:
        import random
        return (pacing[0] + random.random() * (pacing[1] - pacing[0])) / 1000
    return 1.0


def _parse_lowest_fare_dates(body: Dict[str, Any], max_days: int) -> List[str]:
    """Extract candidate departure dates from LowestFareOffers response."""
    candidates: List[tuple] = []
    data = body.get("data") or {}
    for key in ("lowestFareOffers", "searchResult", "calendar", "offers"):
        node = data.get(key)
        if not isinstance(node, dict):
            continue
        for k, v in (node.get("connections") or node.get("days") or node.get("dates") or {}).items():
            if isinstance(v, dict):
                miles = v.get("miles") or (v.get("price") or {}).get("amount") if isinstance(v.get("price"), dict) else None
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


def _has_lowest_fare_connections(j: Optional[Dict]) -> bool:
    """True if LowestFareOffers response has non-empty connections/days/dates/lowestOffers."""
    data = (j or {}).get("data") or {}
    offers = data.get("lowestFareOffers") or data
    if not isinstance(offers, dict):
        return False
    conns = offers.get("connections") or offers.get("days") or offers.get("dates")
    if conns and isinstance(conns, dict):
        return True
    lowest = offers.get("lowestOffers")
    return bool(lowest and isinstance(lowest, list) and len(lowest) > 0)


def _host_attempt_entry(host: Dict, ok: bool, status: int, timing_ms: Optional[int], error: Optional[str]) -> Dict:
    """Build host_attempts entry per spec: name, base_url, ok, status, timing_ms, error."""
    return {
        "name": host.get("name", host.get("base_url", "?")),
        "base_url": host.get("base_url", ""),
        "ok": ok,
        "status": status,
        "timing_ms": timing_ms or 0,
        "error": error or "",
    }


async def _try_hosts_warmup(cfg: Dict, log_path: Optional[Path]) -> Optional[tuple]:
    """Try each host until warmup succeeds. Returns (client, host_used, host_attempts) or None.
    On all-fail: writes cooldown state and returns None."""
    attempts: List[Dict] = []
    hosts = list(cfg.get("hosts_to_try", DEFAULT_HOSTS))
    prefer = cfg.get("cookie_prefer_host")
    if prefer and _load_cookies(cfg):
        # Put preferred host first when we have cookies from that domain
        hosts = [h for h in hosts if h.get("name") == prefer] + [h for h in hosts if h.get("name") != prefer]
    base_url = hosts[0].get("base_url", "https://www.klm.se") if hosts else "https://www.klm.se"
    all_cookies = _load_cookies(cfg, base_url=base_url)
    for host in hosts:
        name = host.get("name", host.get("base_url", "?"))
        # Only attach cookies when host matches cookie origin (e.g. AF-US cookies -> AF-US host)
        host_cookies = all_cookies if (prefer and name == prefer) else []
        headers = _headers_from_host(host, cfg)
        client = AirFrancePlaywrightClient(
            user_agent=cfg["user_agent"],
            headers_base=headers,
            base_url=host["base_url"],
            timeout_ms=cfg.get("warmup_timeout_ms", 60000),
            force_http1=cfg.get("force_http1", False),
            cookies=host_cookies,
        )
        try:
            result = await client.warmup()
            ok = result.get("ok", False)
            ms = result.get("request_ms") or result.get("advanced_ms")
            attempts.append(_host_attempt_entry(host, ok, 200 if ok else 0, ms, None if ok else "warmup failed"))
            if ok:
                _log_line(f"Warmup succeeded on {name}", log_path)
                return (client, name, attempts)
        except Exception as e:
            err_str = str(e)[:200]
            attempts.append(_host_attempt_entry(host, False, 0, None, err_str))
        await client.close()

    # All hosts failed: write cooldown state
    consecutive = int(cfg.get("_consecutive_blocked", 0))
    minutes = 60 if consecutive >= 2 else 30
    try:
        import sys
        proj_root = Path(__file__).resolve().parent.parent
        if str(proj_root) not in sys.path:
            sys.path.insert(0, str(proj_root))
        from partner_awards.airfrance.state import set_blocked
        set_blocked(minutes=minutes, reason="warmup failed on all hosts", host="")
    except Exception:
        pass
    _log_line(f"All hosts failed. Cooldown set: {minutes} min.", log_path)
    return None


async def _warmup_test_impl(cfg: Dict, log_path: Optional[Path]) -> None:
    """Run warmup only, try all hosts, print results."""
    result = await _try_hosts_warmup(cfg, log_path)
    if result:
        client, host_used, attempts = result
        _log_line(f"host_used: {host_used}", log_path)
        _log_line(f"host_attempts: {attempts}", log_path)
        await client.close()
    else:
        _log_line("Warmup failed on all hosts", log_path)


def _pacing_delay(cfg: Dict) -> float:
    """Jitter sleep in seconds. Prefer pacing_ms, fallback to pacing_delay_sec."""
    if cfg.get("pacing_ms"):
        return _pacing_ms(cfg)
    delays = cfg.get("pacing_delay_sec") or [1, 3]
    if isinstance(delays, list) and len(delays) >= 2:
        return delays[0] + (delays[1] - delays[0]) * (os.urandom(1)[0] / 255)
    return float(delays[0]) if isinstance(delays, (list, tuple)) and delays else 1.5


def _verify_output_impl(path: Path) -> int:
    """Verify output folder: .meta.json exists, each JSON has meta_ref and body, origin/destination/cabins."""
    if not path.exists():
        print(f"Error: path not found: {path}")
        return 1
    ok = True
    if path.is_file():
        paths = [path]
        folders = [path.parent]
    else:
        paths = list(path.rglob("*.json"))
        folders = list({p.parent for p in paths})
    for folder in folders:
        meta_path = folder / ".meta.json"
        if not meta_path.exists():
            # Skip if no JSON files in folder
            if not any(f for f in paths if f.parent == folder and f.name != ".meta.json"):
                continue
            print(f"FAIL: {meta_path} missing")
            ok = False
            continue
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            print(f"FAIL: {meta_path} invalid: {e}")
            ok = False
            continue
        search = meta.get("search") or meta
        for k in ("origin", "destination", "cabins"):
            src = search.get(k) if k != "cabins" else search.get("cabins")
            if k == "origin" and not src:
                src = meta.get("origin")
            if k == "destination" and not src:
                src = meta.get("destination")
            if not src and k == "cabins":
                src = meta.get("cabins")
            if not src:
                print(f"FAIL: {meta_path} missing {k}")
                ok = False
    for fp in paths:
        if fp.name == ".meta.json":
            continue
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"FAIL: {fp} invalid JSON: {e}")
            ok = False
            continue
        if not isinstance(data, dict):
            print(f"FAIL: {fp} not a dict")
            ok = False
            continue
        if "meta_ref" not in data or "body" not in data:
            # Legacy format: raw body at top level (e.g. {"data": {...}})
            if "data" in data:
                print(f"WARN: {fp} legacy format (no meta_ref/body)")
            else:
                print(f"FAIL: {fp} missing meta_ref or body")
                ok = False
    if ok:
        print("OK: verification passed")
    return 0 if ok else 1


def _log_line(msg: str, log_path: Optional[Path] = None):
    ts = datetime.now().isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    if log_path:
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


PERSISTED_HASH_LOWEST_FARES = "3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243"
PERSISTED_HASH_AVAILABLE_OFFERS = "6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a"


def _write_meta(
    out_dir: Path,
    host_used: str,
    host_attempts: List[Dict],
    cfg: Dict,
    origin: Optional[str] = None,
    destination: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cabins: Optional[List[str]] = None,
    search_type: str = "DAY",
    operation_name: str = "SharedSearchLowestFareOffersForSearchQuery",
    sha256_hash: str = PERSISTED_HASH_LOWEST_FARES,
) -> None:
    """Write .meta.json as source of truth for ingest."""
    run_id = str(uuid.uuid4())
    days = 0
    if start_date and end_date:
        try:
            from datetime import datetime as _dt
            s = _dt.strptime(start_date, "%Y-%m-%d").date()
            e = _dt.strptime(end_date, "%Y-%m-%d").date()
            days = (e - s).days + 1
        except Exception:
            pass

    meta: Dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "host_used": host_used,
        "host_attempts": host_attempts,
        "search": {
            "origin": origin or "",
            "destination": destination or "",
            "start_date": start_date or "",
            "end_date": end_date or "",
            "days": days,
            "cabins": cabins or ["ECONOMY"],
            "type": search_type,
        },
        "operation": {
            "operationName": operation_name,
            "sha256Hash": sha256_hash,
            "url_booking_flow": cfg.get("url_booking_flow", "LEISURE"),
        },
    }
    with open(out_dir / ".meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


async def _run_once_impl(
    cfg: Dict,
    output_base: Path,
    origin: str,
    destination: str,
    date_str: str,
    cabin: str,
    dry_run: bool,
    log_path: Optional[Path],
) -> int:
    """Run single route/date fetch. Returns number of files written."""
    if dry_run:
        out_dir = output_base / "AF" / f"{origin}-{destination}" / date_str
        _log_line(f"DRY-RUN: would write to {out_dir}", log_path)
        return 0

    warmup_result = await _try_hosts_warmup(cfg, log_path)
    if not warmup_result:
        return 0
    client, host_used, host_attempts = warmup_result
    host = next((h for h in cfg["hosts_to_try"] if h.get("name") == host_used), {})
    url_params = _url_params_from_host(host, cfg)

    try:
        await asyncio.sleep(_pacing_delay(cfg))
        search_uuid = str(uuid.uuid4())

        create_res = await client.gql_post(
            "SharedSearchCreateSearchContextForSearchQuery",
            build_create_context(search_uuid),
            url_params=url_params,
            max_retries=cfg.get("max_retries", 1),
        )
        _log_line(f"CreateContext: status={create_res.get('status')} ms={create_res.get('timing_ms')}", log_path)
        if not create_res["ok"]:
            _log_line(f"CreateContext failed: {create_res.get('error', '')[:200]}", log_path)
            return 0

        await asyncio.sleep(_pacing_delay(cfg))

        offers_res = await client.gql_post(
            "SearchResultAvailableOffersQuery",
            build_available_offers(origin, destination, date_str, cabin, search_uuid),
            url_params=url_params,
            max_retries=cfg.get("max_retries", 1),
        )
        _log_line(f"AvailableOffers: status={offers_res.get('status')} ms={offers_res.get('timing_ms')}", log_path)
        if not offers_res["ok"] or not offers_res.get("json"):
            _log_line(f"AvailableOffers failed: {offers_res.get('error', '')[:200]}", log_path)
            return 0

        out_dir = output_base / "AF" / f"{origin}-{destination}" / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"available_offers_{cabin}_{ts}.json"
        out_path = out_dir / fname
        wrapped = {"meta_ref": "./.meta.json", "operationName": "SearchResultAvailableOffersQuery", "body": offers_res["json"]}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(wrapped, f, indent=2, ensure_ascii=False)
        _write_meta(out_dir, host_used, host_attempts, cfg, origin=origin, destination=destination, cabins=[cabin],
            operation_name="SearchResultAvailableOffersQuery", sha256_hash=PERSISTED_HASH_AVAILABLE_OFFERS)
        _log_line(f"Wrote {out_path}", log_path)
        return 1
    finally:
        await client.close()


async def _calendar_scan_impl(
    cfg: Dict,
    output_base: Path,
    origin: str,
    destination: str,
    start_date: str,
    days: int,
    cabins: List[str],
    max_offer_days: int,
    dry_run: bool,
    log_path: Optional[Path],
) -> int:
    """Calendar scan: CreateContext → LowestFares → [optional] AvailableOffers drilldown.
    max_offer_days=0: calendar-only mode, output LowestFareOffers JSON only."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = start_dt + timedelta(days=days)
    end_date = end_dt.isoformat()
    calendar_only = max_offer_days <= 0

    if dry_run:
        mode = "calendar-only" if calendar_only else f"calendar+drilldown (max {max_offer_days} days)"
        _log_line(f"DRY-RUN: calendar {origin}→{destination} {start_date} to {end_date} cabins={cabins} ({mode})", log_path)
        return 0

    warmup_result = await _try_hosts_warmup(cfg, log_path)
    if not warmup_result:
        return 0
    client, host_used, host_attempts = warmup_result
    host = next((h for h in cfg["hosts_to_try"] if h.get("name") == host_used), {})
    url_params = _url_params_from_host(host, cfg)

    written = 0
    consecutive_blocked = 0
    try:
        await asyncio.sleep(_pacing_delay(cfg))
        search_uuid = str(uuid.uuid4())

        create_res = await client.gql_post(
            "SharedSearchCreateSearchContextForSearchQuery",
            build_create_context(search_uuid),
            url_params=url_params,
            max_retries=cfg.get("max_retries", 1),
        )
        if not create_res["ok"]:
            _log_line(f"CreateContext failed: {create_res.get('error', '')[:200]}", log_path)
            return 0

        await asyncio.sleep(_pacing_delay(cfg))

        candidate_dates: List[str] = []
        lowest_body: Optional[Dict] = None
        for cab in cabins:
            lowest_res = await client.gql_post(
                "SharedSearchLowestFareOffersForSearchQuery",
                build_lowest_fares(origin, destination, start_date, end_date, cabins, search_uuid),
                url_params=url_params,
                max_retries=cfg.get("max_retries", 1),
            )
            _log_line(f"LowestFares ({cab}): status={lowest_res.get('status')}", log_path)
            if lowest_res["ok"] and lowest_res.get("json") and not _has_lowest_fare_connections(lowest_res["json"]):
                _log_line(f"LowestFares empty, retrying with AIRPORT/AIRPORT for {origin}-{destination}", log_path)
                await asyncio.sleep(_pacing_delay(cfg))
                retry_res = await client.gql_post(
                    "SharedSearchLowestFareOffersForSearchQuery",
                    build_lowest_fares(
                        origin, destination, start_date, end_date, cabins, search_uuid,
                        origin_type="AIRPORT", destination_type="AIRPORT",
                    ),
                    url_params=url_params,
                    max_retries=cfg.get("max_retries", 1),
                )
                if retry_res["ok"] and retry_res.get("json") and _has_lowest_fare_connections(retry_res["json"]):
                    lowest_res = retry_res
            if lowest_res["ok"] and lowest_res.get("json"):
                lowest_body = lowest_res["json"]
                consecutive_blocked = 0
                if not calendar_only:
                    dates = _parse_lowest_fare_dates(lowest_res["json"], max_offer_days)
                    for d in dates:
                        if d not in candidate_dates:
                            candidate_dates.append(d)
            elif _is_retriable(lowest_res.get("status", 0), lowest_res.get("error", ""), cfg):
                consecutive_blocked += 1
                if consecutive_blocked >= 2:
                    _log_line("Blocked twice in a row, stopping early", log_path)
                    break
            await asyncio.sleep(_pacing_delay(cfg))

        # Output calendar (LowestFareOffers) JSON
        out_dir = output_base / "AF" / f"{origin}-{destination}" / start_date
        out_dir.mkdir(parents=True, exist_ok=True)
        if lowest_body:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            cab_str = "_".join(cabins)
            fname = f"lowest_fares_{cab_str}_{ts}.json"
            out_path = out_dir / fname
            wrapped = {"meta_ref": "./.meta.json", "operationName": "SharedSearchLowestFareOffersForSearchQuery", "body": lowest_body}
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(wrapped, f, indent=2, ensure_ascii=False)
            _write_meta(out_dir, host_used, host_attempts, cfg, origin=origin, destination=destination, start_date=start_date, end_date=end_date, cabins=cabins)
            _log_line(f"Wrote {out_path}", log_path)
            written += 1

        if calendar_only:
            return written

        if not candidate_dates:
            candidate_dates = [start_date]
        _log_line(f"Candidate dates: {candidate_dates[:max_offer_days]}", log_path)

        for depart_date in candidate_dates[:max_offer_days]:
            await asyncio.sleep(_pacing_delay(cfg))
            for cab in cabins:
                offers_res = await client.gql_post(
                    "SearchResultAvailableOffersQuery",
                    build_available_offers(origin, destination, depart_date, cab, search_uuid),
                    url_params=url_params,
                    max_retries=cfg.get("max_retries", 1),
                )
                if offers_res["ok"] and offers_res.get("json"):
                    odir = output_base / "AF" / f"{origin}-{destination}" / depart_date
                    odir.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    fname = f"available_offers_{cab}_{ts}.json"
                    opath = odir / fname
                    wrapped = {"meta_ref": "./.meta.json", "operationName": "SearchResultAvailableOffersQuery", "body": offers_res["json"]}
                    with open(opath, "w", encoding="utf-8") as f:
                        json.dump(wrapped, f, indent=2, ensure_ascii=False)
                    _write_meta(odir, host_used, host_attempts, cfg, origin=origin, destination=destination, cabins=[cab],
                        operation_name="SearchResultAvailableOffersQuery", sha256_hash=PERSISTED_HASH_AVAILABLE_OFFERS)
                    _log_line(f"Wrote {opath}", log_path)
                    written += 1
                    consecutive_blocked = 0
                elif _is_retriable(offers_res.get("status", 0), offers_res.get("error", ""), cfg):
                    consecutive_blocked += 1
                    if consecutive_blocked >= 2:
                        _log_line("Blocked twice in a row, stopping drilldown", log_path)
                        break
    finally:
        await client.close()

    return written


async def _open_dates_month_impl(
    cfg: Dict,
    output_base: Path,
    origin: str,
    destination: str,
    month: str,
    cabins: List[str],
    dry_run: bool,
    log_path: Optional[Path],
) -> int:
    """Fetch a full month of LowestFareOffers in MONTH mode (AMS→JNB open dates style)."""
    import calendar as cal_mod
    # month: "2026-03"
    year, m = int(month[:4]), int(month[5:7])
    start_date = f"{year:04d}-{m:02d}-01"
    # MONTH type: use 12-month window like the open-dates page (e.g. 2026-03-01/2027-02-28)
    end_m = m + 11
    end_year = year + (end_m - 1) // 12
    end_m = ((end_m - 1) % 12) + 1
    end_day = cal_mod.monthrange(end_year, end_m)[1]
    end_date = f"{end_year:04d}-{end_m:02d}-{end_day:02d}"

    if dry_run:
        _log_line(f"DRY-RUN: open-dates-month {origin}→{destination} {month} cabins={cabins}", log_path)
        return 0

    warmup_result = await _try_hosts_warmup(cfg, log_path)
    if not warmup_result:
        return 0
    client, host_used, host_attempts = warmup_result
    host = next((h for h in cfg["hosts_to_try"] if h.get("name") == host_used), {})
    url_params = _url_params_from_host(host, cfg)

    try:
        await asyncio.sleep(_pacing_delay(cfg))
        search_uuid = str(uuid.uuid4())

        create_res = await client.gql_post(
            "SharedSearchCreateSearchContextForSearchQuery",
            build_create_context(search_uuid),
            url_params=url_params,
            max_retries=cfg.get("max_retries", 1),
        )
        if not create_res["ok"]:
            _log_line(f"CreateContext failed: {create_res.get('error', '')[:200]}", log_path)
            return 0

        await asyncio.sleep(_pacing_delay(cfg))

        lowest_res = await client.gql_post(
            "SharedSearchLowestFareOffersForSearchQuery",
            build_lowest_fares(origin, destination, start_date, end_date, cabins, search_uuid, interval_type="MONTH"),
            url_params=url_params,
            max_retries=cfg.get("max_retries", 1),
        )
        _log_line(f"LowestFares MONTH: status={lowest_res.get('status')}", log_path)

        # Fallback: if empty, retry with AIRPORT/AIRPORT (some routes e.g. AMS-CPT need both as airports)
        if lowest_res["ok"] and lowest_res.get("json") and not _has_lowest_fare_connections(lowest_res["json"]):
            for retry_name, retry_url_params, retry_kw in [
                ("AIRPORT/AIRPORT", url_params, {"origin_type": "AIRPORT", "destination_type": "AIRPORT"}),
                ("bookingFlow=REWARD", {**url_params, "bookingFlow": "REWARD"}, {"origin_type": "CITY", "destination_type": "AIRPORT"}),
                ("DAY+omit_departure_date", url_params, {"interval_type": "DAY", "omit_departure_date": True, "origin_type": "AIRPORT", "destination_type": "AIRPORT"}),
            ]:
                _log_line(f"LowestFares empty, retrying with {retry_name} for {origin}-{destination}", log_path)
                await asyncio.sleep(_pacing_delay(cfg))
                kw = {"interval_type": "MONTH", "origin_type": "CITY", "destination_type": "AIRPORT", **retry_kw}
                retry_res = await client.gql_post(
                    "SharedSearchLowestFareOffersForSearchQuery",
                    build_lowest_fares(origin, destination, start_date, end_date, cabins, search_uuid, **kw),
                    url_params=retry_url_params,
                    max_retries=cfg.get("max_retries", 1),
                )
                if retry_res["ok"] and retry_res.get("json") and _has_lowest_fare_connections(retry_res["json"]):
                    lowest_res = retry_res
                    _log_line(f"Retry succeeded ({retry_name}): got connections for {origin}-{destination}", log_path)
                    break

        if not lowest_res["ok"] or not lowest_res.get("json"):
            _log_line(f"LowestFares failed: {lowest_res.get('error', '')[:200]}", log_path)
            return 0

        out_dir = output_base / "AF" / f"{origin}-{destination}" / month
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        cab_str = "_".join(cabins)
        fname = f"lowest_fares_MONTH_{cab_str}_{ts}.json"
        out_path = out_dir / fname
        wrapped = {"meta_ref": "./.meta.json", "operationName": "SharedSearchLowestFareOffersForSearchQuery", "body": lowest_res["json"]}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(wrapped, f, indent=2, ensure_ascii=False)
        _write_meta(out_dir, host_used, host_attempts, cfg, origin=origin, destination=destination,
            start_date=start_date, end_date=end_date, cabins=cabins, search_type="MONTH")
        _log_line(f"Wrote {out_path}", log_path)
        return 1
    finally:
        await client.close()


def main():
    parser = argparse.ArgumentParser(description="Air France Remote Fetch Runner")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # run-once
    p_once = sub.add_parser("run-once", help="Fetch single route/date")
    p_once.add_argument("--origin", required=True)
    p_once.add_argument("--destination", required=True)
    p_once.add_argument("--date", required=True, help="YYYY-MM-DD")
    p_once.add_argument("--cabin", default="ECONOMY")
    p_once.add_argument("--dry-run", action="store_true")

    # warmup-test
    sub.add_parser("warmup-test", help="Test warmup (page.goto + request.get) without fetching offers")

    # verify-output
    p_verify = sub.add_parser("verify-output", help="Verify output folder: meta.json, wrapped JSON")
    p_verify.add_argument("--path", "-p", required=True, help="Path to outputs/AF/...")

    # open-dates-month
    p_month = sub.add_parser("open-dates-month", help="Fetch full month (MONTH mode) for open dates calendar")
    p_month.add_argument("--origin", required=True)
    p_month.add_argument("--destination", required=True)
    p_month.add_argument("--month", required=True, help="YYYY-MM e.g. 2026-03")
    p_month.add_argument("--cabins", default="BUSINESS")
    p_month.add_argument("--dry-run", action="store_true")

    # calendar-scan
    p_cal = sub.add_parser("calendar-scan", help="Calendar scan over date range")
    p_cal.add_argument("--origin", required=True)
    p_cal.add_argument("--destination", required=True)
    p_cal.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_cal.add_argument("--days", type=int, default=14)
    p_cal.add_argument("--cabins", default="ECONOMY,BUSINESS")
    p_cal.add_argument("--max-offer-days", type=int, default=5)
    p_cal.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    cfg = _load_config()
    output_base = Path(cfg["output_dir"]).resolve()
    output_base.mkdir(parents=True, exist_ok=True)
    log_path = output_base / f"run_{datetime.now().strftime('%Y%m%d')}.log"

    # verify-output: check meta + wrapped JSON
    if args.cmd == "verify-output":
        return _verify_output_impl(Path(args.path).resolve())

    # Cooldown gate: refuse scan if blocked
    if args.cmd in ("run-once", "calendar-scan", "open-dates-month"):
        try:
            proj_root = Path(__file__).resolve().parent.parent
            if str(proj_root) not in sys.path:
                sys.path.insert(0, str(proj_root))
            from partner_awards.airfrance.state import is_blocked
            blocked, until = is_blocked()
            if blocked:
                _log_line(f"Fetching paused until {until} due to blocking", log_path)
                return 2
        except Exception:
            pass

    if args.cmd == "warmup-test":
        asyncio.run(_warmup_test_impl(cfg, log_path))
        return 0

    if args.cmd == "run-once":
        n = asyncio.run(
            _run_once_impl(
                cfg, output_base,
                args.origin, args.destination, args.date, args.cabin,
                args.dry_run, log_path,
            )
        )
    elif args.cmd == "open-dates-month":
        cabins = [c.strip() for c in args.cabins.split(",") if c.strip()] or ["BUSINESS"]
        n = asyncio.run(
            _open_dates_month_impl(cfg, output_base, args.origin, args.destination, args.month, cabins, args.dry_run, log_path)
        )
    else:
        cabins = [c.strip() for c in args.cabins.split(",") if c.strip()] or ["ECONOMY"]
        n = asyncio.run(
            _calendar_scan_impl(
                cfg, output_base,
                args.origin, args.destination, args.start, args.days,
                cabins, args.max_offer_days,
                args.dry_run, log_path,
            )
        )

    _log_line(f"Done. Files written: {n}", log_path)
    if n > 0:
        try:
            from partner_awards.airfrance.state import clear_blocked
            clear_blocked()
        except Exception:
            pass
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
