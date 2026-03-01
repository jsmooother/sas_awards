"""
Playwright client for Air France GraphQL API.
Designed for VPS deployment where AF edge is reachable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Persisted query hashes (docs/airfrance-awards-api.md)
PERSISTED_HASHES = {
    "SharedSearchCreateSearchContextForSearchQuery": "54e5576492358745ae7ee183605ca00eee645cfcd2bc557fedc124cb32140f65",
    "SharedSearchLowestFareOffersForSearchQuery": "3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243",
    "SearchResultAvailableOffersQuery": "6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a",
}

DEFAULT_BASE = "https://www.klm.se"
HOME_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


def build_create_context(search_state_uuid: str) -> Dict[str, Any]:
    return {
        "operationName": "SharedSearchCreateSearchContextForSearchQuery",
        "variables": {"searchStateUuid": search_state_uuid},
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": PERSISTED_HASHES["SharedSearchCreateSearchContextForSearchQuery"],
            }
        },
    }


def build_available_offers(
    origin: str,
    destination: str,
    depart_date: str,
    cabin: str,
    search_state_uuid: str,
) -> Dict[str, Any]:
    return {
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
                "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
            },
            "searchStateUuid": search_state_uuid,
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": PERSISTED_HASHES["SearchResultAvailableOffersQuery"],
            }
        },
    }


def build_lowest_fares(
    origin: str,
    destination: str,
    start_date: str,
    end_date: str,
    cabins: List[str],
    search_state_uuid: str,
    interval_type: str = "DAY",
) -> Dict[str, Any]:
    """Build LowestFareOffers payload. interval_type: DAY or MONTH."""
    date_interval = f"{start_date}/{end_date}"
    cabin_list = cabins if isinstance(cabins, list) else [cabins]
    return {
        "operationName": "SharedSearchLowestFareOffersForSearchQuery",
        "variables": {
            "lowestFareOffersRequest": {
                "bookingFlow": "REWARD",
                "withUpsellCabins": True,
                "passengers": [{"id": 1, "type": "ADT"}],
                "commercialCabins": cabin_list,
                "customer": {"selectedTravelCompanions": [{"passengerId": 1, "travelerKey": 0, "travelerSource": "PROFILE"}]},
                "type": interval_type,
                "requestedConnections": [
                    {
                        "departureDate": start_date,
                        "dateInterval": date_interval,
                        "origin": {"type": "CITY", "code": origin},
                        "destination": {"type": "AIRPORT", "code": destination},
                    },
                    {
                        "dateInterval": None,
                        "origin": {"type": "AIRPORT", "code": destination},
                        "destination": {"type": "CITY", "code": origin},
                    },
                ],
            },
            "activeConnection": 0,
            "searchStateUuid": search_state_uuid,
            "bookingFlow": "REWARD",
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": PERSISTED_HASHES["SharedSearchLowestFareOffersForSearchQuery"],
            }
        },
    }


class AirFrancePlaywrightClient:
    """Playwright-based Air France/KLM GraphQL client for VPS deployment."""

    def __init__(
        self,
        user_agent: str,
        headers_base: Dict[str, str],
        base_url: str = DEFAULT_BASE,
        timeout_ms: int = 60_000,
        retry_backoff_sec: int = 3,
        retry_on_status: Optional[List[int]] = None,
        force_http1: bool = False,
    ):
        self.user_agent = user_agent
        self.headers_base = headers_base
        self.base_url = base_url.rstrip("/")
        self.gql_url = f"{self.base_url}/gql/v1"
        self.timeout_ms = timeout_ms
        self.retry_backoff_sec = retry_backoff_sec
        self.retry_on_status = retry_on_status or [403, 429, 503]
        self.force_http1 = force_http1
        self._context = None
        self._browser = None
        self._playwright = None

    async def _ensure_context(self):
        """Lazy-init browser and context."""
        if self._context is not None:
            return
        from playwright.async_api import async_playwright

        launch_args = []
        if self.force_http1:
            launch_args = ["--disable-http2", "--disable-quic"]
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True, args=launch_args)
        self._context = await self._browser.new_context(
            user_agent=self.user_agent,
            ignore_https_errors=True,
            extra_http_headers=self.headers_base,
        )

    async def close(self):
        """Release resources."""
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._context = self._browser = self._playwright = None

    async def warmup(self) -> Dict[str, Any]:
        """
        Request-only warmup. Probes /en/ and /en/search/advanced.
        Success = both return 200. Used by runner to gate scans.
        """
        result = {"request_ok": False, "request_ms": None, "advanced_ok": False, "advanced_ms": None}

        await self._ensure_context()
        req_headers = {**self.headers_base, "accept": HOME_ACCEPT}

        t0 = time.perf_counter()
        try:
            req = await self._context.request.get(
                f"{self.base_url}/en/",
                headers=req_headers,
                timeout=self.timeout_ms,
            )
            result["request_ok"] = req.status == 200
        except Exception as e:
            log.warning("Warmup /en/ failed: %s", e)
        result["request_ms"] = round((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        try:
            req = await self._context.request.get(
                f"{self.base_url}/en/search/advanced",
                headers=req_headers,
                timeout=self.timeout_ms,
            )
            result["advanced_ok"] = req.status == 200
        except Exception as e:
            log.warning("Warmup /en/search/advanced failed: %s", e)
        result["advanced_ms"] = round((time.perf_counter() - t0) * 1000)

        both_ok = result["request_ok"] and result["advanced_ok"]
        return {"ok": both_ok, **result}

    async def gql_post(
        self,
        operation_name: str,
        payload: Dict[str, Any],
        url_params: Optional[Dict[str, str]] = None,
        max_retries: int = 1,
    ) -> Dict[str, Any]:
        """
        POST GraphQL with required query params and headers.
        Returns { ok, status, timing_ms, json, error }.
        """
        params = url_params or {}
        params.setdefault("bookingFlow", "LEISURE")
        params.setdefault("brand", "AF")
        params.setdefault("country", self.headers_base.get("afkl-travel-country", "FR"))
        params.setdefault("language", self.headers_base.get("afkl-travel-language", "en"))
        params["operationName"] = operation_name

        query_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        url = f"{self.gql_url}?{query_str}"

        await self._ensure_context()

        body = json.dumps(payload).encode("utf-8")
        attempt = 0
        last_status = None
        last_error = None

        while attempt <= max_retries:
            t0 = time.perf_counter()
            try:
                req = await self._context.request.post(
                    url,
                    data=body,
                    headers={
                        **self.headers_base,
                        "content-type": "application/json",
                        "accept": "application/json",
                    },
                    timeout=self.timeout_ms,
                )
                elapsed_ms = round((time.perf_counter() - t0) * 1000)
                last_status = req.status

                if req.status == 200:
                    raw = await req.body()
                    try:
                        data = json.loads(raw.decode("utf-8"))
                        return {"ok": True, "status": 200, "timing_ms": elapsed_ms, "json": data, "error": None}
                    except json.JSONDecodeError:
                        return {"ok": False, "status": req.status, "timing_ms": elapsed_ms, "json": None, "error": "Invalid JSON response"}

                if req.status in self.retry_on_status and attempt < max_retries:
                    log.warning("%s returned %s, retrying in %ds", operation_name, req.status, self.retry_backoff_sec)
                    await asyncio.sleep(self.retry_backoff_sec)
                    attempt += 1
                    continue

                text = (await req.body()).decode("utf-8", errors="replace")[:500]
                return {"ok": False, "status": req.status, "timing_ms": elapsed_ms, "json": None, "error": text}

            except Exception as e:
                elapsed_ms = round((time.perf_counter() - t0) * 1000)
                last_error = str(e)
                if attempt < max_retries:
                    log.warning("%s failed: %s, retrying in %ds", operation_name, e, self.retry_backoff_sec)
                    await asyncio.sleep(self.retry_backoff_sec)
                    attempt += 1
                    continue
                return {"ok": False, "status": 0, "timing_ms": elapsed_ms, "json": None, "error": last_error}

        return {"ok": False, "status": last_status or 0, "timing_ms": 0, "json": None, "error": last_error or "Max retries exceeded"}
