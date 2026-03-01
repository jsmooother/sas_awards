# ⚠️ Critical Implementation Notes

1. searchStateUuid is required for all reward queries.
2. Persisted query hashes may change without notice.
3. If calls fail, re-capture ONLY:
   - operationName
   - sha256Hash
4. Cookies are not required for backend replication.
5. Implement rate limiting (AF likely has anti-bot protection).

---

# Air France Award Search – GraphQL API Documentation

## Overview

Air France (Flying Blue reward bookings) uses persisted GraphQL queries via:

```
POST https://wwws.airfrance.fr/gql/v1
```

All reward searches require:

* `operationName`
* `extensions.persistedQuery.sha256Hash`
* `variables`
* Valid `searchStateUuid`

The award flow consists of **3 core operations**:

1. Create/refresh search context
2. Fetch lowest fares (calendar view)
3. Fetch available offers (flight list for a selected date)

---

# Required Headers (Minimal Working Set)

Use only these unless debugging:

```http
accept: application/json
content-type: application/json
afkl-travel-country: FR
afkl-travel-language: en
afkl-travel-market: FR
afkl-travel-host: AF
```

Do NOT include:

* Cookies
* sec-ch headers
* trace headers
* analytics headers

---

# 1️⃣ Create Search Context

## Purpose

Initializes or refreshes `searchStateUuid`.

## Operation

```
SharedSearchCreateSearchContextForSearchQuery
```

## Persisted Query Hash

```
54e5576492358745ae7ee183605ca00eee645cfcd2bc557fedc124cb32140f65
```

## Example Request

```json
{
  "operationName": "SharedSearchCreateSearchContextForSearchQuery",
  "variables": {
    "searchStateUuid": "UUID-GOES-HERE"
  },
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "54e5576492358745ae7ee183605ca00eee645cfcd2bc557fedc124cb32140f65"
    }
  }
}
```

## Notes

* `searchStateUuid` can be generated per session.
* This UUID must be reused in subsequent calls.

---

# 2️⃣ Lowest Fare Offers (Calendar View)

## Purpose

Returns lowest mileage per day within date interval.

## Operation

```
SharedSearchLowestFareOffersForSearchQuery
```

## Persisted Query Hash

```
3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243
```

## Example Request

```json
{
  "operationName": "SharedSearchLowestFareOffersForSearchQuery",
  "variables": {
    "lowestFareOffersRequest": {
      "bookingFlow": "REWARD",
      "withUpsellCabins": true,
      "passengers": [
        { "id": 1, "type": "ADT" }
      ],
      "commercialCabins": ["ECONOMY"],
      "customer": {
        "selectedTravelCompanions": [
          {
            "passengerId": 1,
            "travelerKey": 0,
            "travelerSource": "PROFILE"
          }
        ]
      },
      "type": "DAY",
      "requestedConnections": [
        {
          "departureDate": "2026-02-27",
          "dateInterval": "2026-02-27/2026-03-05",
          "origin": { "type": "CITY", "code": "PAR" },
          "destination": { "type": "AIRPORT", "code": "JNB" }
        },
        {
          "dateInterval": null,
          "origin": { "type": "AIRPORT", "code": "JNB" },
          "destination": { "type": "CITY", "code": "PAR" }
        }
      ]
    },
    "activeConnection": 0,
    "searchStateUuid": "UUID-GOES-HERE",
    "bookingFlow": "REWARD"
  },
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "3129e42881c15d2897fe99c294497f2cfa8f2133109dd93ed6cad720633b0243"
    }
  }
}
```

## Fields to Parameterize

* `origin.code`
* `destination.code`
* `departureDate`
* `dateInterval`
* `commercialCabins`
* `passengers`
* `searchStateUuid`

## Output

Returns:

* Dates
* Lowest miles required
* Often tax/surcharge component

---

# 3️⃣ Available Offers (Flight List for Specific Date)

## Purpose

Returns specific flight options for a selected departure date.

## Operation

```
SearchResultAvailableOffersQuery
```

## Persisted Query Hash

```
6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a
```

## Example Request

```json
{
  "operationName": "SearchResultAvailableOffersQuery",
  "variables": {
    "activeConnectionIndex": 0,
    "bookingFlow": "REWARD",
    "availableOfferRequestBody": {
      "commercialCabins": ["ECONOMY"],
      "passengers": [
        { "id": 1, "type": "ADT" }
      ],
      "requestedConnections": [
        {
          "origin": { "code": "PAR", "type": "CITY" },
          "destination": { "code": "JNB", "type": "AIRPORT" },
          "departureDate": "2026-02-27"
        },
        {
          "origin": { "code": "JNB", "type": "AIRPORT" },
          "destination": { "code": "PAR", "type": "CITY" },
          "dateInterval": "2026-03-03/2026-03-09"
        }
      ],
      "bookingFlow": "REWARD",
      "customer": {
        "selectedTravelCompanions": [
          {
            "passengerId": 1,
            "travelerKey": 0,
            "travelerSource": "PROFILE"
          }
        ]
      }
    },
    "searchStateUuid": "UUID-GOES-HERE"
  },
  "extensions": {
    "persistedQuery": {
      "version": 1,
      "sha256Hash": "6c2316d35d088fdd0d346203ec93cec7eea953752ff2fc18a759f9f2ba7b690a"
    }
  }
}
```

## Output

Returns:

* Direct + connecting flights
* Seats left indicators
* Cabin availability
* Miles + surcharges

---

# Recommended Nightly Scan Flow

```
1. Generate UUID
2. Call CreateSearchContext
3. Call LowestFareOffers for route + month interval
4. Store lowest fare data
5. Optionally call AvailableOffers for selected candidate dates
6. Store full itinerary data
```

---

# What To Do If It Breaks

If requests start failing:

1. Check if persisted query hash changed.
2. Capture new GraphQL call from DevTools:

   * Network → Fetch/XHR → filter `gql/v1`
   * Copy Payload only
3. Update:

   * `operationName`
   * `sha256Hash`

No need to re-analyze whole HAR.

---

# Key Observations

* Everything runs through persisted GraphQL queries.
* `searchStateUuid` is mandatory.
* Cookies are not required for backend replication.
* Anti-bot systems may block high-volume scraping.

---

# File Version

Document created for:

```
Route: PAR → JNB
Cabin: ECONOMY
Booking flow: REWARD
Market: FR
Language: en
```

---
