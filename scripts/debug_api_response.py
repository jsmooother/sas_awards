#!/usr/bin/env python3
"""
Debug script: fetch raw SAS API response and dump full structure.
Run: python scripts/debug_api_response.py

Compares direct=true vs direct=false and shows exactly what the API returns.
"""
import json
import requests
import sys

BASE_URL = "https://www.sas.se/bff/award-finder/destinations/v1"
MARKET = "se-sv"
ORIGINS = ["ARN", "CPH"]
PASSENGERS = 1

def fetch_availability(origin, dest_code, direct=False):
    params = {
        "market": MARKET,
        "origin": origin,
        "destinations": dest_code,
        "selectedMonth": "",
        "passengers": PASSENGERS,
        "direct": str(direct).lower(),
        "availability": "true",
        "selectedFlightClass": "",
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data[0] if data else None


def main():
    # Test ARN -> BER (Berlin) - same route as the user's screenshot
    origin = "ARN"
    dest = "BER"

    print("=" * 60)
    print(f"Fetching {origin} -> {dest}")
    print("=" * 60)

    for direct in [False, True]:
        label = "direct=FALSE (include connecting)" if not direct else "direct=TRUE (direct only)"
        print(f"\n--- {label} ---\n")
        try:
            rec = fetch_availability(origin, dest, direct=direct)
            if not rec:
                print("No data returned")
                continue
            # Pretty-print full response
            print(json.dumps(rec, indent=2, ensure_ascii=False))

            av = rec.get("availability", {})
            out_count = len(av.get("outbound", []))
            in_count = len(av.get("inbound", []))
            print(f"\nSummary: {out_count} outbound dates, {in_count} inbound dates")

            # Check structure of first outbound entry
            out_list = av.get("outbound", [])
            if out_list:
                first = out_list[0]
                print(f"\nFirst outbound entry keys: {list(first.keys())}")
                if isinstance(first.get("date"), dict):
                    print("  ^ date is a dict - might have nested structure!")
                for i, x in enumerate(out_list[:3]):
                    print(f"  [{i}] {x}")
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print("Done. Compare the two outputs above.")


if __name__ == "__main__":
    main()
