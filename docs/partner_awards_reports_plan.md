# Flying Blue Reports – UX Redesign Plan

## Current State

- **API flow**: `open-dates-month` → CreateContext + LowestFareOffers (12‑month window) → returns ~12 sample dates with miles. Optionally: SearchResultAvailableOffersQuery for specific dates.
- **Scope section**: Month + Cabin dropdown; applies differently per report (year view = 365 days, heatmap = selected month).
- **Reports**: Single page with tabs (Summary, Year view, Discovery, Heatmap). Data from watchlist routes only.
- **Watchlist**: User adds routes manually; batch scan runs open-dates-month for enabled routes × 12 months × 2 cabins. **Include returns**: when set, the worker also queues the return leg (e.g. BKK→AMS for AMS→BKK). Same API and ingest; we just run with `(origin=destination, destination=origin)` so return data is stored and Windows/round-trip views work.

## Proposed Changes

### 1. API Flow Optimization

**Phase A – Monthly only (current behavior)**  
1. `CreateContext` + `LowestFareOffers` (MONTH, 12‑month window) → sample dates + miles  
2. Store in DB as calendar fares (no AvailableOffers yet)

**Phase B – Drilldown only for “green” dates (new)**  
- From LowestFareOffers, keep dates where miles ≤ threshold (e.g. global min per route, or ≤ 1.2× min)  
- Run `SearchResultAvailableOffersQuery` **only** for those dates  
- Reduces API calls and focus on likely bookable days

Runner options:

- `open-dates-month` (existing): monthly view only  
- `open-dates-month --drilldown --threshold 1.2`: monthly + drilldown for dates ≤ 1.2× route min

### 2. Scope Section

- **Year view**: No scope needed – always 365 days, best miles across all routes.  
- **Month‑specific views**: Month + Cabin still useful for heatmap and route calendars.  
- **Recommendation**:  
  - Remove scope from Overview (always full year).  
  - Keep Month + Cabin on Heatmap and per‑route calendar.  
  - Option: Move scope into each report card instead of a global bar.

### 3. Four Report Pages (Calendar‑centric)

| Page | Purpose | Main content | Scope |
|------|---------|--------------|-------|
| **1. Overview** | Entry point, “where to look” | Top 10 routes by best miles + green days; links to drill‑downs | Full year, no filters |
| **2. From AMS** | AMS‑origin routes | Calendar grid per route or heatmap; AMS→JNB, AMS→CPT, etc. | Month optional |
| **3. From PAR** | PAR‑origin routes | Same as AMS | Month optional |
| **4. Outbound + Inbound Pairs** | Round‑trip discovery (SAS‑style) | Routes where both directions have availability; weekend‑style pairs | Month optional |

Navigation: Sidebar: Overview | From AMS | From PAR | Pairs. Each page has its own calendar view.

### 4. Route Strategy – 5–10 Routes per Origin

**Goal**: Maintain ~5–10 routes from each AMS and PAR to have enough data for analysis and UX iteration.

**Suggested initial routes**

From **AMS** (10 routes):

- JNB, CPT (Africa)
- ADD, NBO, CAI (Africa)
- LHR, CDG (Europe)
- BKK, SIN (Asia)
- EZE (long‑haul)

From **PAR** (10 routes):

- JNB, CPT, ADD, NBO (Africa)
- MRU, RUN (Indian Ocean)
- CDG (intra‑EU)
- BKK, SIN (Asia)
- EZE, SCL (Americas)

**Selection criteria**

1. Destinations with both outbound and inbound options  
2. Mix of short‑haul and long‑haul  
3. Popular Flying Blue destinations  
4. Complement SAS routes where relevant  

**Implementation options**

- **A. Curated list**: Hardcode “recommended routes” in config; dashboard/runner uses this by default.  
- **B. Auto‑add from Discovery**: Run Discovery on a broad set of origins/destinations; add top N per origin to watchlist.  
- **C. Config file**: `partner_awards_remote_runner/recommended_routes.json` – list of (origin, dest) for batch scans. Job worker can use watchlist + recommended to fill gaps.

**Recommended**: B + C. Config lists recommended routes; job worker ensures watchlist includes at least 5–10 from AMS and PAR (adding if missing).

### 5. Implementation Phases

**Phase 1 – Data foundation (1–2 days)**  
- Add `recommended_routes.json` with 10 AMS + 10 PAR routes  
- Job worker: “expand watchlist” to include recommended when below threshold  
- Run batch scan for recommended routes to populate DB  

**Phase 2 – Report structure (2–3 days)**  
- Split reports into 4 routes: `/partner-awards/flyingblue/reports/overview`, `/from-ams`, `/from-par`, `/pairs`  
- Overview: Top routes, links to others  
- From AMS / From PAR: Calendar grids, heatmap‑style view, reuse existing components  
- Pairs: Round‑trip view (requires new backend logic for outbound+inbound matching)  

**Phase 3 – UX polish (1–2 days)**  
- Scope: Remove from Overview; scope per report where needed  
- Calendar views aligned across reports  
- Responsive layout  

**Phase 4 – Drilldown optimization (1 day)**  
- Add `--drilldown` / `--threshold` to `open-dates-month` or new `open-dates-drilldown` command  
- Job worker: optional drilldown for green days only  

### 6. Open Questions

1. **Pairs page**: Do we already store direction per fare, or must we infer round‑trip pairs from two one‑way calendar fares?  
2. **Recommended routes**: Should users be able to override/add routes, or is the list fixed?  
3. **Scope default**: For heatmap/calendar views, default to current month or first month with data?  

---

*Document created from UX redesign discussion. Update as decisions are made.*
