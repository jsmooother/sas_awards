# routes/v1 API – Evaluation

## Summary

**Recommendation: Use `routes/v1` only on-demand in the Weekend modal.** Do not add it to the batch data fetcher.

---

## Current API Usage (destinations/v1)

| Step | Calls per run |
|------|----------------|
| `get_all_destinations(origin)` × 2 origins | 2 |
| `fetch_availability(origin, dest)` × ~100 dests × 2 origins | ~200 |
| **Total per run** | **~202** |
| Runs per day (every 6h) | 4 |
| **Calls per day** | **~800** |

---

## Option A: Add routes/v1 to Batch Fetcher

To fetch per-flight data for all routes and dates:

| Factor | Value |
|--------|-------|
| Origins | 2 (ARN, CPH) |
| Destinations | ~100 |
| Dates in range | ~365 |
| Calls per (origin, dest, date) | 2 (outbound + inbound) |
| **Total per run** | 2 × 100 × 365 × 2 ≈ **146,000** |

**Impact:** ~180× increase in API calls. Very likely to attract attention from SAS.

**Verdict: Reject**

---

## Option B: routes/v1 Only On-Demand (Weekend Modal)

When a user clicks a weekend pair to open the detail modal:

| Action | Calls |
|--------|-------|
| User clicks row | 0 (we have data in DB) |
| Modal opens, fetches per-flight detail | 2 (outbound date + inbound date) |

| Metric | Estimate |
|--------|----------|
| Modal opens per day | 10–50 |
| Extra calls per day | 20–100 |
| Total calls per day | ~820–900 |
| Increase vs today | ~2–12% |

**Benefits:**
- Same usage pattern as sas.se (user selects date → API called)
- Low extra volume
- No change to cron/batch fetcher
- Better UX: flight times (10:10, 12:20, etc.) in the modal

**Verdict: Accept**

---

## Option C: Hybrid – routes/v1 to Fill Date Gaps

Use `routes/v1` to fill dates missing from `destinations/v1`:

- We only discover gaps when comparing our results to what we expect.
- To fill gaps we must call `routes/v1` for those dates.
- We don’t have a reliable way to know gaps without calling many dates.
- Same scalability issue as Option A.

**Verdict: Reject**

---

## Implementation (Option B)

1. **Backend:** Add `/api/weekend-routes` (or similar) that calls `routes/v1` for a given (origin, destination, date) and returns the JSON.
2. **Modal:** When the user opens the weekend detail modal, call this endpoint for outbound and inbound dates instead of or in addition to the current weekend-detail logic.
3. **No batch changes:** `update_sas_awards.py` stays as is.

Proxy via our backend so the browser never calls SAS directly. That gives us control over rate and behavior.

---

## Conclusion

| Approach | Calls added | Risk | Recommendation |
|----------|-------------|------|-----------------|
| Batch fetcher | +146,000/run | High | No |
| On-demand modal | +20–100/day | Low | Yes |
| Gap filling | Similar to batch | High | No |

**Use `routes/v1` only on-demand in the Weekend modal.** Keep the batch fetcher on `destinations/v1` only.
