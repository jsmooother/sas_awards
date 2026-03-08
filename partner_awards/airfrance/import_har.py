"""
HAR / JSON import for Partner Awards.
Extracts SearchResultAvailableOffersQuery response from HAR or raw JSON.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


def extract_from_json(body: bytes) -> Tuple[Optional[Dict], Optional[str]]:
    """Parse raw JSON. Returns (parsed_dict, error)."""
    try:
        data = json.loads(body.decode("utf-8"))
        if isinstance(data, dict):
            return data, None
        return None, "Expected JSON object"
    except json.JSONDecodeError as e:
        return None, str(e)
    except UnicodeDecodeError as e:
        return None, str(e)


def extract_from_har(body: bytes) -> Tuple[Optional[Dict], Optional[str], Optional[str]]:
    """
    Parse HAR, find SearchResultAvailableOffersQuery response.
    Returns (parsed_response_dict, operation_name_found, error).
    Prefer SearchResultAvailableOffersQuery; fallback to first gql/v1 entry.
    """
    try:
        har = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        return None, None, str(e)

    if not isinstance(har, dict):
        return None, None, "HAR root must be object"
    entries = har.get("log", {}).get("entries", [])
    if not isinstance(entries, list):
        return None, None, "HAR log.entries not found"

    candidates: List[Tuple[Dict, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        req = entry.get("request") or {}
        resp = entry.get("response") or {}
        url = req.get("url") or ""
        if "gql/v1" not in url:
            continue
        post_data = req.get("postData", {})
        text = post_data.get("text") if isinstance(post_data, dict) else None
        if text:
            try:
                payload = json.loads(text)
                op = payload.get("operationName") if isinstance(payload, dict) else None
                if op:
                    content = resp.get("content") or {}
                    if not isinstance(content, dict):
                        continue
                    ct = content.get("text")
                    if ct is None:
                        continue
                    if content.get("encoding") == "base64":
                        try:
                            ct = base64.b64decode(ct).decode("utf-8")
                        except Exception:
                            continue
                    try:
                        parsed = json.loads(ct)
                        candidates.append((parsed, op))
                    except json.JSONDecodeError:
                        continue
            except json.JSONDecodeError:
                continue

    if not candidates:
        return None, None, "No gql/v1 entries with response content found in HAR"

    for parsed, op in candidates:
        if op == "SearchResultAvailableOffersQuery":
            return parsed, op, None
    parsed, op = candidates[0]
    return parsed, op, None
