"""
Playwright-based live fetch for Air France GraphQL.
Uses Chromium browser stack to bypass TLS fingerprinting / tarpit.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

from .service import AF_BASE, AF_GQL_URL, AF_HEADERS, CREATE_CONTEXT_HASH, AVAILABLE_OFFERS_HASH

log = logging.getLogger(__name__)

# Multi-host failover: try KLM-SE first, then AF-US
HOSTS_TO_TRY = [
    ("https://www.klm.se", "KLM-SE"),
    ("https://wwws.airfrance.us", "AF-US"),
    (AF_BASE, "AF-FR"),
]

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_TIMEOUT_MS = 60_000


def _run_async(coro):
    """Run async coroutine from sync context."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# Browser-like accept for HTML pages (not application/json)
HOME_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"


_last_working_base: Optional[str] = None


async def _fetch_homepage_async() -> Tuple[int, int, Optional[str]]:
    """GET homepage via APIRequestContext. Tries hosts in order (KLM-SE, AF-US, AF-FR).
    Uses HTML accept header. Sets _last_working_base for post_gql."""
    global _last_working_base
    from playwright.async_api import async_playwright

    for base_url, name in HOSTS_TO_TRY:
        t0 = time.perf_counter()
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True, args=["--disable-http2", "--disable-quic"])
                context = await browser.new_context(
                    user_agent=USER_AGENT,
                    ignore_https_errors=True,
                    extra_http_headers={"accept": HOME_ACCEPT, "user-agent": USER_AGENT},
                )
                try:
                    req = await context.request.get(
                        f"{base_url.rstrip('/')}/en/",
                        timeout=REQUEST_TIMEOUT_MS,
                    )
                    body = await req.body()
                    status = req.status
                    elapsed_ms = round((time.perf_counter() - t0) * 1000)
                    if status == 200:
                        _last_working_base = base_url.rstrip("/")
                        log.info("Playwright homepage (%s): status=%d elapsed=%dms", name, status, elapsed_ms)
                        return status, elapsed_ms, None
                    log.warning("Playwright homepage (%s): status=%d", name, status)
                finally:
                    await context.close()
                    await browser.close()
        except Exception as e:
            elapsed_ms = round((time.perf_counter() - t0) * 1000)
            log.warning("Playwright homepage (%s) failed: %s", name, str(e)[:80])
    return 0, 0, "All hosts failed"


async def _post_gql_async(
    payload: Dict[str, Any],
    log_name: str = "gql",
) -> Tuple[int, Optional[Dict], int, Optional[str]]:
    """POST GraphQL. Uses _last_working_base from warmup, or AF_GQL_URL fallback."""


async def _fetch_homepage_via_navigation_async() -> Tuple[int, int, Optional[str]]:
    """Real browser navigation to homepage. Returns (status, timing_ms, error).
    If this works but request.get hangs, edge treats fetch API differently than navigation."""
    from playwright.async_api import async_playwright

    t0 = time.perf_counter()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                ignore_https_errors=True,
            )
            try:
                page = await context.new_page()
                resp = await page.goto(
                    f"{AF_BASE}/en/",
                    wait_until="domcontentloaded",
                    timeout=REQUEST_TIMEOUT_MS,
                )
                status = resp.status if resp else 0
                elapsed_ms = round((time.perf_counter() - t0) * 1000)
                log.info("Playwright homepage (page.goto): status=%d elapsed=%dms", status, elapsed_ms)
                return status, elapsed_ms, None
            finally:
                await context.close()
                await browser.close()
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        log.exception("Playwright homepage (page.goto) failed: %s", e)
        return 0, elapsed_ms, str(e)


async def _post_gql_async(
    payload: Dict[str, Any],
    log_name: str = "gql",
) -> Tuple[int, Optional[Dict], int, Optional[str]]:
    """POST GraphQL. Uses _last_working_base from warmup, or AF_GQL_URL fallback."""
    import json as _json
    from playwright.async_api import async_playwright

    base = _last_working_base or AF_BASE
    gql_url = f"{base.rstrip('/')}/gql/v1"
    headers = dict(AF_HEADERS)
    headers["origin"] = base.rstrip("/")
    headers["referer"] = f"{base.rstrip('/')}/en/search/open-dates/0"
    t0 = time.perf_counter()
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--disable-http2", "--disable-quic"])
            context = await browser.new_context(
                user_agent=USER_AGENT,
                ignore_https_errors=True,
                extra_http_headers=headers,
            )
            try:
                req = await context.request.post(
                    gql_url,
                    data=payload,
                    timeout=REQUEST_TIMEOUT_MS,
                )
                body = await req.body()
                status = req.status
                elapsed_ms = round((time.perf_counter() - t0) * 1000)
                log.info("Playwright %s: status=%d elapsed=%dms bytes=%d", log_name, status, elapsed_ms, len(body))
                try:
                    data = _json.loads(body.decode("utf-8"))
                except Exception:
                    data = None
                return status, data, elapsed_ms, None
            finally:
                await context.close()
                await browser.close()
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        log.exception("Playwright %s failed: %s", log_name, e)
        return 0, None, elapsed_ms, str(e)


def fetch_homepage() -> Tuple[int, int, Optional[str]]:
    """Sync wrapper. Returns (status, timing_ms, error). Uses request.get with HTML accept."""
    return _run_async(_fetch_homepage_async())


def fetch_homepage_via_navigation() -> Tuple[int, int, Optional[str]]:
    """Sync wrapper. Real browser page.goto warmup. Returns (status, timing_ms, error)."""
    return _run_async(_fetch_homepage_via_navigation_async())


def post_gql(payload: Dict[str, Any], log_name: str = "gql") -> Tuple[int, Optional[Dict], int, Optional[str]]:
    """Sync wrapper. Returns (status, json_body, timing_ms, error)."""
    return _run_async(_post_gql_async(payload, log_name))


def playwright_health_check() -> Dict[str, Any]:
    """Verify Playwright + Chromium is installed."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return {"ok": True, "message": "Playwright Chromium ready"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
