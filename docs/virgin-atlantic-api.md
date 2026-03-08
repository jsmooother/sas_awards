# Virgin Atlantic Reward Search – API Reference

## Overview

Virgin Atlantic reward flights use:

- **GraphQL** for search and calendar (login required for award data).
- **REST** for airport lookups by IATA code (no login needed).

Constraints observed:

- **Login required** – reward availability and points come from authenticated session (cookies / session).
- **One destination per search** – each `SearchOffers` request is for a single origin → destination.
- **Calendar in one call** – the `SearchOffers` response includes `calendar.fromPrices` and `priceGrid.departures` for a date range; likely one request can cover a month or similar range (confirm by inspecting `variables.request` date fields).

**No-data without browser session:** A direct POST to the GraphQL endpoint with no cookies (and no prior visit to the site) returns **444 Access Denied** (HTML), not JSON. So we cannot get calendar or offers without either a full browser session (visit the site first, then replay from that session) or cookies/session from a logged-in user. Scripts that call the API from a server or fresh run are blocked at the edge (WAF). See `scripts/virgin_search_offers_no_auth.py` for the test.

**Recommended approach: one-way only, then reverse**

Skip the round-trip UI flow (SearchOffers → select outbound → SearchOffersNextSlice). Use **only SearchOffers** with one-way legs:

1. **Outbound:** `SearchOffers` with `searchOriginDestinations: [{ origin: "LON", destination: "NYC", departureDate: "2026-10-24" }]` (or a date range if the API supports it).
2. **Return:** same operation with the route reversed: `searchOriginDestinations: [{ origin: "NYC", destination: "LON", departureDate: "2026-10-31" }]`.

No `basketId`, no `offerId`, no SearchOffersNextSlice. We get calendar + price grid for each direction independently. In our own code we can then:

- Build a **month calendar** per route (e.g. LON→NYC and NYC→LON for a given month).
- For **weekend / round-trip** views: pair outbound dates with return dates (e.g. out Fri, back Mon) and sum points or show combined availability.

One call per (origin, destination, date or date range) – easy to automate and no session state.

---

## Base URLs

| Purpose        | URL |
|----------------|-----|
| GraphQL (search, calendar, account) | `https://www.virginatlantic.com/flights/search/api/graphql` |
| Airport by IATA code               | `https://www.virginatlantic.com/travelplus/search-panel-api/airports/by-code?iataCode={CODE}` |

Examples:

- Origin: `by-code?iataCode=LON`
- Destination: `by-code?iataCode=NYC` (city code; JFK etc. may be in response or separate)

---

## GraphQL Endpoint

**Request URL:** `https://www.virginatlantic.com/flights/search/api/graphql`  
**Method:** `POST`  
**Content-Type:** `application/json`

Request body shape:

```json
{
  "operationName": "<OperationName>",
  "variables": { ... },
  "extensions": { "clientLibrary": { "name": "@apollo/client", "version": "4.1.6" } },
  "query": "..."
}
```

---

## Operations

### 1. SearchOffers (reward search + calendar)

Used for reward flight search and calendar/lowest-price data.

- **operationName:** `SearchOffers`
- **variables:** `{ "request": <FlightOfferRequestInput> }`

The `request` object is only partially known; it includes at least:

- `pos`: null (or POS/source info)
- `parties`: null (or passenger count)

**Captured request shape:**

```json
{
  "pos": null,
  "parties": null,
  "customerDetails": [{ "custId": "ADT_0", "ptc": "ADT" }],
  "flightSearchRequest": {
    "searchOriginDestinations": [
      { "origin": "LON", "destination": "NYC", "departureDate": "2026-10-24" }
    ]
  }
}
```

- **One way:** single object in `searchOriginDestinations` (origin, destination, departureDate).
- **Round trip:** likely two objects in `searchOriginDestinations` (outbound and return); if you did a return search, expand `flightSearchRequest` and paste the full `searchOriginDestinations` array.
- **Date range:** the response includes `calendar.fromPrices` and `priceGrid.departures` for a range; the request may also support a date range (e.g. `toDate` or a second date). If you have a search that showed many dates, expand `flightSearchRequest` fully and note any other date/range fields.

Response (high level) includes:

- `result.criteria` – origin, destination, departing.
- `result.calendar` – `from`, `to`, `fromPrices[]` with `fromDate` and `price` (e.g. `awardPoints`, `minimumPriceInWeek`, `minimumPriceInMonth`, `remaining`, `direct`).
- `result.priceGrid` – `departures[]` with `departing` and `prices[]` (amount, currency, awardPoints).
- `result.slice` – flight segments and fares (availability, cabin, award points, tax).

So one `SearchOffers` call can return both calendar and price-grid data for a range of dates (likely up to ~1 month per call; confirm via the date fields in `request`).

The response also includes **`result.basketId`** – required for the next step when the user selects a flight (round-trip flow).

### 2. SearchOffersNextSlice (return leg after selecting outbound)

Called **after the user has chosen an outbound flight** to load the next slice (return options). Same endpoint as SearchOffers.

**Note:** For automation we can skip this and use one-way SearchOffers only (outbound + reversed route); see “Recommended approach: one-way only” above.

- **operationName:** `SearchOffersNextSlice`
- **variables:** `{ "request": <SearchOffersNextSliceRequestInput> }`

**Captured request shape:**

| Field | Value / meaning |
|-------|------------------|
| `basketId` | From `SearchOffers` response `result.basketId` (e.g. `"7530ce41-feb4-4e5c-862d-9b8e3e375913"`). |
| `currentTripIndexId` | `1` for the return leg (0 = outbound, 1 = return). |
| `offerId` | ID of the selected outbound offer from the previous slice (e.g. `"0caf9216-be49-4744-b02e-3b031c28dc92|4dd510b1-39c6-4750-9cbb-fb1394b48571-FL-2"`). |

**Round-trip flow:**

1. **SearchOffers** (round-trip search) → get calendar + first slice (outbound options); response has `basketId` and `slice` with `flightsAndFares[].fareId` / offer identifiers.
2. User selects an outbound flight → take the corresponding `offerId` (or the composite id from the selected fare).
3. **SearchOffersNextSlice** with `basketId`, `currentTripIndexId: 1`, `offerId` → get return leg options and `priceGrid` for return dates.

Response includes `result.slice` (return flights/fares) and `priceGrid` (return departures/prices). No `calendar` in this response (calendar is from SearchOffers).

### 3. AccountMemberDetails (logged-in user)

Used when the user is logged in (Flying Club).

- **operationName:** `AccountMemberDetails`
- **variables:** `{}`
- **query:** requests `accountMemberDetails(vouchersCount: 0, activitiesCount: 0, flightsCount: 0) { flyingClubMemberDetails { tierCode, fCMembershipNumber, totalVirginPoints, firstName } }`

Useful to verify that the session has reward access; if this returns 401 or no points, reward APIs may not return award data.

---

## Airport by-code (REST)

**GET**  
`https://www.virginatlantic.com/travelplus/search-panel-api/airports/by-code?iataCode={IATA}`

- **IATA** = 3-letter code, e.g. `LON`, `NYC`, `JFK`, `MIA`.
- No auth required for basic lookups.
- Use to resolve city vs airport (e.g. NYC vs JFK) and get names for display.

---

## Auth (for reward data)

- Reward search and calendar require a **logged-in session**.
- In DevTools, for a successful `SearchOffers` call that returns award points:
  - Copy **Request Headers**: at least `Cookie`, and any `Authorization` or `x-*` token headers.
- Reuse the same headers when calling the GraphQL endpoint from a script or backend (same caveats as Flying Blue: cookies expire, may need to refresh).

---

## Help: Getting the cookie so the API works

### 1. Log in and trigger a reward search

1. Open **https://www.virginatlantic.com** in Chrome (or another browser).
2. Log in to your **Flying Club** account (or create one).
3. Go to **Book** → **Reward flights** (or "Use your points") and run a search, e.g. London → New York, one-way, any date.
4. Wait until the results (or calendar) load.

### 2. Capture the Cookie from a working request

1. Open **Developer Tools** (F12) → **Network** tab.
2. Filter by **Fetch/XHR** (or type `graphql` in the filter box).
3. Find the request named **`graphql`** that has **SearchOffers** (click it and check **Payload** → `operationName: "SearchOffers"`).
4. Click that request → **Headers** tab.
5. **Request Headers:** Find **`Cookie`** (one long line). Right‑click its value → **Copy value**. That’s the best option.
6. **Or Response Headers:** You’ll see 4 **`Set-Cookie`** lines. Copy all 4 and paste into the app; it will extract the cookie. See §3 for the four names.

### 3. Which cookie(s) are needed?

The **Response** headers for the graphql request include 4 `Set-Cookie` headers. Their names (so you can copy the right thing):

| Cookie name | Likely role |
|-------------|-------------|
| `com.virginholidays.att` | Tracking |
| `com.virginatlantic.edge.id` | Edge/session id |
| `bm_s` | Akamai Bot Manager (long value) – often required to avoid 444 |
| `bm_sv` | Akamai Bot Manager – often required to avoid 444 |

We don’t know the minimal set. **Recommended:**

1. **Easiest:** In the same **graphql** request, open **Request Headers** (not Response). Copy the full **`Cookie`** value (one long line). That’s what the browser actually sends and usually works.
2. **Alternative:** Paste all 4 **Response** `Set-Cookie` lines into the app; it will extract the cookie parts and use them.
3. **Optional minimal set:** In DevTools → Application → Cookies → virginatlantic.com, try including only `bm_s`, `bm_sv`, and `com.virginatlantic.edge.id` and test. If you get 444, add the rest.

### 4. Test with the script

From the repo root:

```bash
# Option A: environment variable
export VIRGIN_COOKIE="your_full_cookie_string_here"
python scripts/virgin_search_offers_no_auth.py

# Option B: pass as argument
python scripts/virgin_search_offers_no_auth.py "your_full_cookie_string_here"
```

If the script prints **Status: 200** and JSON with `data.searchOffers.result`, the cookie is sufficient.

### 5. Security

- Do not commit cookie strings to git or paste them in public places.
- Store them in a local config or env file that is in `.gitignore`.
- Cookies usually expire after hours or days; when the API returns 444 or empty data again, log in and re-capture.

### 6. 444 and implementation plan

**Observed:** Even with a valid, full Request Cookie (e.g. 80+ name=value pairs) and browser-like headers, a direct `POST` from Python (`requests`) to the GraphQL endpoint returns **444 Access Denied** (HTML). This holds on residential IPs as well. Virgin’s WAF (Akamai) is almost certainly blocking on **client fingerprint** (e.g. TLS/JA3), not on the cookie or IP alone.

**Current app behaviour:**

- Cookie can be pasted as: (a) full Request Cookie (one long line), (b) the 4 Response `Set-Cookie` lines, or (c) DevTools copy that includes the header name and trailing headers (`cookie` / `priority` / `referer`). The app extracts and stores only the cookie value.
- “Test connection” sends the stored cookie with browser-like headers; it typically still gets 444 from this server.

**Plan when we pick up Virgin scanning later:** Use **Playwright** (or similar browser automation) to perform the SearchOffers request in a real browser context, so the TLS fingerprint and request shape match a normal user. Cookie handling (save/extract/test UI) can stay as-is; the actual API calls for calendar/offers should go through Playwright with the saved cookie injected into the browser context.

---

## Request variables (SearchOffers) – captured shape

| Field | Value / shape |
|-------|----------------|
| `pos` | null |
| `parties` | null |
| `customerDetails` | `[{ "custId": "ADT_0", "ptc": "ADT" }]` (one adult) |
| `flightSearchRequest.searchOriginDestinations` | Array of legs. **One-way:** one object `{ origin, destination, departureDate }`. **Round trip:** two objects (outbound + return). |

Example one-way leg: `origin: "LON"`, `destination: "NYC"`, `departureDate: "2026-10-24"` (YYYY-MM-DD).

So: **one destination per call** = one leg (or one round-trip pair) in `searchOriginDestinations`. The response `calendar.fromPrices` may cover a date range; check `calendar.from` / `calendar.to` in the response to see how many days per call.

---

## Next steps for implementation

1. **Virgin scanning:** Implement using **Playwright** so GraphQL requests run in a real browser context and avoid 444 (see §6 above). Reuse saved cookie by injecting into the browser context.
2. **Optional:** Expand `flightSearchRequest` fully (any other keys beside `searchOriginDestinations`?) and, for a round-trip search, capture the second element of `searchOriginDestinations`.
3. **Confirm date range** – try a search that spans a full month and see if `request` has `from`/`to` or similar; confirm that the response `calendar.fromPrices` covers that range (so we can do “1 month per call”).
4. **Optional:** Save a full request/response sample (redact cookies) for automated tests or replay.

With this request shape we can add a Virgin client (e.g. under `partner_awards/virgin/`) that uses Playwright and maps responses to the same calendar/offer structures used for Flying Blue.
