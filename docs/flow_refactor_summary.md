# Refactor Summary: 2-Page Architecture

## Before → After

**Before:** 7 page routes + 6 report sub-routes + 2 API endpoints = 15 URL handlers, 9 templates, 1400-line app.py

**After:** 2 page routes + 4 API endpoints + 13 legacy redirects = 19 URL handlers, 3 templates, 200-line app.py

## Architecture

```
GET /           → dashboard.html   (unified filter+results+detail modal)
GET /reports    → reports.html     (5-tab: Region, City, Business, Weekend, New)
GET /api/detail → JSON             (generalized route detail)
GET /api/routes → JSON             (SAS routes/v1 proxy)
```

All SQL lives in `queries.py`. Region mapping in `regions.py`. App.py is routes-only.

## Old endpoints → New locations

| Old URL | New | Status |
|---|---|---|
| `/` | `/` | Rewritten (dashboard with filters) |
| `/search` | `/?city=...` | 301 redirect |
| `/all` | `/reports?tab=region` | 301 redirect |
| `/business` | `/reports?tab=business` | 301 redirect |
| `/plus` | `/reports?tab=region&cabin=plus` | 301 redirect |
| `/weekend` | `/reports?tab=weekend` | 301 redirect |
| `/new` | `/reports?tab=new` | 301 redirect |
| `/flow` | `/` | 301 redirect |
| `/reports/business-by-date` | `/reports?tab=business` | 301 redirect |
| `/reports/new-business` | `/reports?tab=new` | 301 redirect |
| `/reports/plus-europe` | `/reports?tab=region` | 301 redirect |
| `/reports/weekend-trips` | `/reports?tab=weekend` | 301 redirect |
| `/reports/summary` | `/reports` | 301 redirect |
| `/reports/us-calendar` | `/reports?tab=region` | 301 redirect |
| `/api/weekend-detail` | `/api/weekend-detail` | Kept (legacy alias) |
| `/api/weekend-routes` | `/api/weekend-routes` | Kept (legacy alias) |

## Files changed

| File | Action | Lines |
|---|---|---|
| `queries.py` | Rewritten | ~280 |
| `app.py` | Rewritten | ~200 |
| `templates/base.html` | Sidebar simplified to 2 items | Modified |
| `templates/dashboard.html` | Created | ~210 |
| `templates/reports.html` | Created | ~175 |
| `tests/test_app.py` | Created | ~130 |
| `AGENTS.md` | Updated nav docs | Modified |
| 9 old templates | Deleted | -84K bytes |

## Results ranking (queries.py)

| Cabin filter | Sort order |
|---|---|
| All | `(ab×3 + ap×2 + ag) DESC, date` |
| Business | `ab DESC, date` |
| Plus | `ap + ab DESC, date` |
| Economy | `ag DESC, date` |
