"""Tests for the refactored SAS Awards app (2-page architecture)."""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from app import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ── Primary pages ──

def test_dashboard_loads(client):
    r = client.get("/")
    assert r.status_code == 200
    assert b"Dashboard" in r.data


def test_dashboard_with_region(client):
    r = client.get("/?region=europe&cabin=all")
    assert r.status_code == 200
    assert b"Europe" in r.data


def test_dashboard_with_city_search(client):
    r = client.get("/?city=Barcelona")
    assert r.status_code == 200


def test_dashboard_business_filter(client):
    r = client.get("/?cabin=business&min_seats=2")
    assert r.status_code == 200


def test_reports_default_tab(client):
    r = client.get("/reports")
    assert r.status_code == 200
    assert b"Region" in r.data


def test_reports_city_tab(client):
    r = client.get("/reports?tab=city")
    assert r.status_code == 200
    assert b"Top Destinations" in r.data


def test_reports_business_tab(client):
    r = client.get("/reports?tab=business")
    assert r.status_code == 200


def test_reports_weekend_tab(client):
    r = client.get("/reports?tab=weekend")
    assert r.status_code == 200


def test_reports_new_tab(client):
    r = client.get("/reports?tab=new")
    assert r.status_code == 200


# ── API endpoints ──

def test_api_detail_missing_params(client):
    r = client.get("/api/detail")
    assert r.status_code == 400


def test_api_detail_valid(client):
    results = client.get("/api/flow/results?region=europe&cabin=all&min_seats=2")
    data = json.loads(results.data)
    if not data["rows"]:
        pytest.skip("No data")
    row = data["rows"][0]
    r = client.get(
        f"/api/detail?origin={row['origin']}"
        f"&dest={row['airport_code']}&date={row['date']}"
    )
    assert r.status_code == 200
    d = json.loads(r.data)
    assert "legs" in d
    assert "booking_url" in d


def test_api_flow_regions(client):
    r = client.get("/api/flow/regions")
    assert r.status_code == 200
    data = json.loads(r.data)
    assert isinstance(data, list)
    assert any(d["key"] == "europe" for d in data)


def test_api_flow_results(client):
    r = client.get("/api/flow/results?region=europe&cabin=all")
    assert r.status_code == 200
    data = json.loads(r.data)
    assert "rows" in data
    assert "total" in data


# ── Legacy redirects (301) ──

def test_redirect_search(client):
    r = client.get("/search?q=Barcelona")
    assert r.status_code == 301
    assert "city=Barcelona" in r.headers["Location"]


def test_redirect_flow(client):
    r = client.get("/flow?region=europe")
    assert r.status_code == 301


def test_redirect_all(client):
    r = client.get("/all")
    assert r.status_code == 301
    assert "tab=region" in r.headers["Location"]


def test_redirect_business(client):
    r = client.get("/business")
    assert r.status_code == 301
    assert "tab=business" in r.headers["Location"]


def test_redirect_plus(client):
    r = client.get("/plus")
    assert r.status_code == 301
    assert "tab=region" in r.headers["Location"]


def test_redirect_weekend(client):
    r = client.get("/weekend")
    assert r.status_code == 301
    assert "tab=weekend" in r.headers["Location"]


def test_redirect_new(client):
    r = client.get("/new")
    assert r.status_code == 301
    assert "tab=new" in r.headers["Location"]


def test_redirect_old_reports(client):
    for path in ["/reports/business-by-date", "/reports/new-business",
                 "/reports/plus-europe", "/reports/weekend-trips",
                 "/reports/summary", "/reports/us-calendar"]:
        r = client.get(path)
        assert r.status_code == 301, f"{path} should redirect"


# ── Legacy API aliases ──

def test_legacy_weekend_detail(client):
    r = client.get("/api/weekend-detail")
    assert r.status_code == 400


def test_legacy_weekend_routes(client):
    r = client.get("/api/weekend-routes")
    assert r.status_code == 400


def test_legacy_flow_detail(client):
    r = client.get("/api/flow/detail")
    assert r.status_code == 400


# ── Weekend pair mode ──

def test_dashboard_weekend_mode(client):
    r = client.get("/?mode=weekend")
    assert r.status_code == 200
    assert b"Weekend" in r.data


def test_dashboard_weekend_with_region(client):
    r = client.get("/?mode=weekend&region=europe&cabin=all")
    assert r.status_code == 200


def test_dashboard_weekend_business_only(client):
    r = client.get("/?mode=weekend&cabin=business")
    assert r.status_code == 200


def test_dashboard_weekend_business_plus(client):
    r = client.get("/?mode=weekend&cabin=business_plus")
    assert r.status_code == 200


def test_dashboard_weekend_city_filter(client):
    r = client.get("/?mode=weekend&city=Bergen")
    assert r.status_code == 200


def test_api_weekend_pair_detail_missing(client):
    r = client.get("/api/weekend-pair-detail")
    assert r.status_code == 400


def test_api_weekend_pair_detail_valid(client):
    results = client.get("/?mode=weekend&cabin=all")
    if b"wpr" not in results.data:
        pytest.skip("No weekend pairs in DB")
    r = client.get(
        "/api/weekend-pair-detail?origin=ARN&dest=BGO"
        "&outbound=2026-03-05&inbound=2026-03-08"
    )
    assert r.status_code in (200, 404)
