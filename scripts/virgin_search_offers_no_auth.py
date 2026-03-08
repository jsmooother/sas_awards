#!/usr/bin/env python3
"""
Try Virgin Atlantic SearchOffers API with optional cookies.

Without cookie: usually gets 444 Access Denied (WAF blocks).
With cookie: pass the full Cookie header from a logged-in browser session.

  export VIRGIN_COOKIE="session=abc; other=def"
  python scripts/virgin_search_offers_no_auth.py

  # or
  python scripts/virgin_search_offers_no_auth.py "session=abc; other=def"

Run from repo root.
"""
import json
import os
import sys
import requests

URL = "https://www.virginatlantic.com/flights/search/api/graphql"

# One-way LON -> NYC, single date (we'll see if calendar comes back)
variables = {
    "request": {
        "pos": None,
        "parties": None,
        "customerDetails": [{"custId": "ADT_0", "ptc": "ADT"}],
        "flightSearchRequest": {
            "searchOriginDestinations": [
                {"origin": "LON", "destination": "NYC", "departureDate": "2026-10-24"}
            ]
        },
    }
}

# Minimal query: just criteria + calendar to see if we get any data without auth
query = """
query SearchOffers($request: FlightOfferRequestInput!) {
  searchOffers(request: $request) {
    result {
      criteria {
        origin { code cityName }
        destination { code cityName }
        departing
      }
      calendar {
        from
        to
        fromPrices {
          fromDate
          price {
            amount
            awardPoints
            currency
            minimumPriceInWeek
            minimumPriceInMonth
          }
        }
      }
    }
  }
}
"""

payload = {
    "operationName": "SearchOffers",
    "variables": variables,
    "extensions": {"clientLibrary": {"name": "@apollo/client", "version": "4.1.6"}},
    "query": query.strip(),
}

def main():
    cookie = os.environ.get("VIRGIN_COOKIE") or (sys.argv[1] if len(sys.argv) > 1 else None)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Language": "en-GB,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.virginatlantic.com",
        "Referer": "https://www.virginatlantic.com/flights/search",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    if cookie:
        headers["Cookie"] = cookie.strip()
        print("POST", URL)
        print("With Cookie header (length %d chars)" % len(cookie))
    else:
        print("POST", URL)
        print("No cookie (set VIRGIN_COOKIE or pass cookie string as first arg)")
    print()
    r = requests.post(URL, json=payload, headers=headers, timeout=30)
    print("Status:", r.status_code)
    print()
    try:
        data = r.json()
        print("Response (JSON):")
        print(json.dumps(data, indent=2)[:4000])
        if len(json.dumps(data)) > 4000:
            print("\n... (truncated)")
        # Quick summary
        if "data" in data and data.get("data") and "searchOffers" in data["data"]:
            res = data["data"]["searchOffers"].get("result")
            if res:
                cal = res.get("calendar") or {}
                from_prices = (cal.get("fromPrices") or [])[:5]
                print("\n--- Summary: criteria + calendar slice ---")
                print("criteria:", res.get("criteria"))
                print("calendar.from:", cal.get("from"), "calendar.to:", cal.get("to"))
                print("fromPrices (first 5):", from_prices)
            else:
                print("\n--- result is null ---")
        if "errors" in data:
            print("\n--- GraphQL errors ---")
            print(data["errors"])
    except Exception as e:
        print("Response text:", r.text[:2000])
        print("Parse error:", e)

if __name__ == "__main__":
    main()
