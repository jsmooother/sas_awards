#!/usr/bin/env python3
"""
Unified test runner for Partner Awards.
Run: python -m partner_awards.tests.run_all

1) Smoke tests (in-memory)
2) DB integrity checks (real DB)
3) HTTP checks (requires Flask running)
"""

from __future__ import annotations

import io
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = os.path.expanduser(
    os.environ.get("PARTNER_AWARDS_DB_PATH")
    or os.path.join(os.environ.get("SAS_DB_PATH", "~/sas_awards"), "partner_awards.sqlite")
)
BASE_URL = os.environ.get("PARTNER_AWARDS_BASE_URL", "http://127.0.0.1:5000")
RESULTS_FILE = PROJECT_ROOT / "partner_awards_test_results.json"

CONNECT_TIMEOUT = 2
READ_TIMEOUT = 10


def run_smoke_tests() -> dict:
    """Run smoke tests, return {status, details}."""
    out = io.StringIO()
    old = sys.stdout
    sys.stdout = out
    try:
        from partner_awards.tests.smoke import main as smoke_main
        smoke_main()
        return {"status": "PASS", "details": out.getvalue().strip()}
    except Exception as e:
        tb = traceback.format_exc()
        sys.stdout = old
        return {"status": "FAIL", "details": tb}
    finally:
        sys.stdout = old


def run_db_checks() -> dict:
    """Run DB integrity checks. Return {status, checks, warnings}."""
    checks = []
    warnings = []
    status = "PASS"
    db_path = os.path.expanduser(DB_PATH)

    if not os.path.exists(db_path):
        checks.append({"name": "DB exists", "status": "SKIP", "msg": f"DB not found: {db_path}"})
        return {"status": "SKIP", "checks": checks, "warnings": ["DB not found"]}

    import sqlite3
    from partner_awards.airfrance.adapter import init_db

    conn = sqlite3.connect(db_path)
    init_db(conn)

    # A) Required tables
    required = [
        "partner_award_watch_routes",
        "partner_award_calendar_fares",
        "partner_award_raw_responses",
        "partner_award_jobs",
        "partner_award_job_tasks",
    ]
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN (" + ",".join("?" * len(required)) + ")",
        required,
    )
    found = {r[0] for r in cur.fetchall()}
    missing = set(required) - found
    if missing:
        checks.append({"name": "Required tables", "status": "FAIL", "msg": f"Missing: {missing}"})
        status = "FAIL"
    else:
        checks.append({"name": "Required tables", "status": "PASS", "msg": "OK"})

    # B) Duplicate calendar fares (source, origin, destination, cabin_class, depart_date, host_used)
    try:
        cur = conn.execute(
            """SELECT source, origin, destination, cabin_class, depart_date, host_used, COUNT(*) as c
               FROM partner_award_calendar_fares
               GROUP BY source, origin, destination, cabin_class, depart_date, host_used
               HAVING c > 1"""
        )
        dups = cur.fetchall()
        cnt = len(dups)
        if cnt > 0:
            checks.append({"name": "Calendar duplicates", "status": "FAIL", "msg": f"{cnt} duplicate keys"})
            status = "FAIL"
        else:
            checks.append({"name": "Calendar duplicates", "status": "PASS", "msg": "0 duplicates"})
    except sqlite3.OperationalError as e:
        checks.append({"name": "Calendar duplicates", "status": "SKIP", "msg": str(e)[:80]})

    # C) Month coverage
    try:
        cur = conn.execute(
            """SELECT DISTINCT substr(depart_date, 1, 7) as ym
               FROM partner_award_calendar_fares WHERE source='AF'
               ORDER BY ym DESC LIMIT 3"""
        )
        months = [r[0] for r in cur.fetchall()]
        for ym in months:
            cur = conn.execute(
                """SELECT origin, destination, cabin_class, COUNT(DISTINCT depart_date) as days
                   FROM partner_award_calendar_fares
                   WHERE source='AF' AND substr(depart_date, 1, 7) = ?
                   GROUP BY origin, destination, cabin_class""",
                (ym,),
            )
            for row in cur.fetchall():
                if row[3] < 20:
                    warnings.append(f"Partial month {ym} {row[0]}→{row[1]} {row[2]}: {row[3]} days")
        if months:
            checks.append({"name": "Month coverage", "status": "PASS", "msg": f"Checked {len(months)} months"})
    except Exception as e:
        checks.append({"name": "Month coverage", "status": "SKIP", "msg": str(e)[:80]})

    # D) Watchlist sanity
    try:
        cur = conn.execute(
            "SELECT id, program, origin, destination FROM partner_award_watch_routes"
        )
        bad_program = []
        bad_code = []
        origin_eq_dest = []
        valid_programs = {"sas", "flyingblue", "virgin"}
        for r in cur.fetchall():
            rid, prog, orig, dest = r
            if prog.lower() not in valid_programs:
                bad_program.append(f"id={rid} program={prog}")
            if not (len(orig) in (2, 3, 4) and orig.isalpha() and orig.isupper()):
                bad_code.append(f"origin {orig}")
            if not (len(dest) in (2, 3, 4) and dest.isalpha() and dest.isupper()):
                bad_code.append(f"destination {dest}")
            if orig == dest:
                origin_eq_dest.append(f"{orig}=={dest}")
        if origin_eq_dest:
            checks.append({"name": "Watchlist sanity", "status": "FAIL", "msg": f"origin==destination: {origin_eq_dest}"})
            status = "FAIL"
        elif bad_program or bad_code:
            checks.append({"name": "Watchlist sanity", "status": "WARN", "msg": f"bad_program={bad_program}, bad_code={bad_code}"})
            warnings.extend(bad_program)
            warnings.extend(bad_code)
        else:
            checks.append({"name": "Watchlist sanity", "status": "PASS", "msg": "OK"})
    except sqlite3.OperationalError as e:
        checks.append({"name": "Watchlist sanity", "status": "SKIP", "msg": str(e)[:80]})

    # E) Jobs/tasks consistency
    try:
        from datetime import timedelta
        cur = conn.execute(
            """SELECT id, started_at FROM partner_award_jobs WHERE status='running'"""
        )
        for jid, started in cur.fetchall():
            if started:
                try:
                    dt = datetime.fromisoformat(started.replace("Z", "+00:00")[:19])
                    if (datetime.now() - dt) > timedelta(hours=2):
                        warnings.append(f"Job {jid} running >2h (started {started})")
                except Exception:
                    pass
        cur = conn.execute(
            """SELECT t.id, t.started_at FROM partner_award_job_tasks t
               WHERE t.status='running' AND t.started_at IS NOT NULL"""
        )
        for tid, started in cur.fetchall():
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00")[:19])
                if (datetime.now() - dt) > timedelta(hours=2):
                    warnings.append(f"Task {tid} running >2h (started {started})")
            except Exception:
                pass
        checks.append({"name": "Jobs/tasks", "status": "PASS", "msg": "OK"})
    except Exception as e:
        checks.append({"name": "Jobs/tasks", "status": "SKIP", "msg": str(e)[:80]})

    conn.close()
    return {"status": status, "checks": checks, "warnings": warnings}


def run_http_checks(sample_route: dict | None) -> dict:
    """Run HTTP checks. sample_route = {origin, destination, month, cabin_class} or None."""
    checks = []
    warnings = []
    status = "PASS"

    use_requests = False
    try:
        import requests as req_mod
        use_requests = True
    except ImportError:
        try:
            import urllib.request
        except ImportError:
            warnings.append("No requests/urllib: HTTP checks skipped")
            return {"status": "SKIP", "checks": [{"name": "HTTP", "status": "SKIP", "msg": "No HTTP library"}], "warnings": warnings}

    def fetch(url: str) -> tuple[int, str | dict, bool]:
        if use_requests:
            try:
                r = req_mod.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                try:
                    return r.status_code, r.json() if "application/json" in (r.headers.get("Content-Type") or "") else r.text, True
                except Exception:
                    return r.status_code, r.text, True
            except Exception as e:
                return -1, str(e), False
        else:
            try:
                import urllib.request
                req = urllib.request.Request(url, headers={"User-Agent": "PartnerAwardsTest/1.0"})
                with urllib.request.urlopen(req, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as r:
                    body = r.read().decode("utf-8", errors="replace")
                    if body.lstrip().startswith("{"):
                        try:
                            return r.getcode(), json.loads(body), True
                        except Exception:
                            pass
                    return r.getcode(), body, True
            except Exception as e:
                return -1, str(e), False

    # Check server reachable
    try:
        if use_requests:
            req_mod.get(f"{BASE_URL}/", timeout=CONNECT_TIMEOUT)
        else:
            import urllib.request
            urllib.request.urlopen(f"{BASE_URL}/", timeout=CONNECT_TIMEOUT + READ_TIMEOUT)
    except Exception:
        warnings.append("Flask not reachable; HTTP checks skipped")
        return {"status": "SKIP", "checks": [{"name": "HTTP", "status": "SKIP", "msg": "Server not reachable"}], "warnings": warnings}

    # Self-test
    code, body, _ = fetch(f"{BASE_URL}/partner-awards/self-test")
    if code == 200:
        content = body if isinstance(body, str) else json.dumps(body)
        has_pass = "PASS" in content or '"status": "PASS"' in content
        checks.append({"name": "GET /partner-awards/self-test", "status": "PASS" if has_pass else "WARN", "msg": f"200, PASS present: {has_pass}"})
    else:
        checks.append({"name": "GET /partner-awards/self-test", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    # Partner awards home
    code, _, _ = fetch(f"{BASE_URL}/partner-awards")
    if code == 200:
        checks.append({"name": "GET /partner-awards", "status": "PASS", "msg": "200"})
    else:
        checks.append({"name": "GET /partner-awards", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    # Flying Blue
    code, _, _ = fetch(f"{BASE_URL}/partner-awards/flyingblue")
    if code == 200:
        checks.append({"name": "GET /partner-awards/flyingblue", "status": "PASS", "msg": "200"})
    else:
        checks.append({"name": "GET /partner-awards/flyingblue", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    # Calendar + telegram + delta (with sample from DB or fallback)
    if sample_route:
        o, d, m, c = sample_route["origin"], sample_route["destination"], sample_route["month"], sample_route.get("cabin_class", "BUSINESS")
    else:
        o, d, m, c = "AMS", "JNB", "2026-03", "BUSINESS"

    cal_url = f"{BASE_URL}/partner-awards/calendar?origin={o}&destination={d}&month={m}&cabin={c}"
    code, _, _ = fetch(cal_url)
    if code == 200:
        checks.append({"name": f"GET /partner-awards/calendar", "status": "PASS", "msg": f"200 ({o}→{d})"})
    else:
        checks.append({"name": "GET /partner-awards/calendar", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    tg_url = f"{BASE_URL}/partner-awards/calendar/telegram?origin={o}&destination={d}&month={m}&cabin={c}"
    code, body, _ = fetch(tg_url)
    if code in (200, 500):
        if code == 500:
            checks.append({"name": "GET /calendar/telegram", "status": "FAIL", "msg": "500"})
            status = "FAIL"
        else:
            is_json = isinstance(body, dict)
            ok_val = body.get("ok", False) if is_json else False
            checks.append({"name": "GET /calendar/telegram", "status": "PASS", "msg": f"200 ok={ok_val}"})
    else:
        checks.append({"name": "GET /calendar/telegram", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    delta_url = f"{BASE_URL}/partner-awards/calendar/delta?origin={o}&destination={d}&month={m}&cabin={c}"
    code, body, _ = fetch(delta_url)
    if code in (200, 500):
        if code == 500:
            checks.append({"name": "GET /calendar/delta", "status": "FAIL", "msg": "500"})
            status = "FAIL"
        else:
            is_json = isinstance(body, dict)
            ok_val = body.get("ok", False) if is_json else False
            checks.append({"name": "GET /calendar/delta", "status": "PASS", "msg": f"200 ok={ok_val}"})
    else:
        checks.append({"name": "GET /calendar/delta", "status": "FAIL", "msg": f"status={code}"})
        status = "FAIL"

    return {"status": status, "checks": checks, "warnings": warnings}


def get_sample_route_from_db() -> dict | None:
    """Get first (origin, destination, month, cabin_class) from calendar_fares for HTTP sample."""
    if not os.path.exists(os.path.expanduser(DB_PATH)):
        return None
    import sqlite3
    conn = sqlite3.connect(os.path.expanduser(DB_PATH))
    cur = conn.execute(
        """SELECT origin, destination, substr(depart_date, 1, 7), cabin_class
           FROM partner_award_calendar_fares WHERE source='AF' LIMIT 1"""
    )
    row = cur.fetchone()
    conn.close()
    if row:
        return {"origin": row[0], "destination": row[1], "month": row[2], "cabin_class": row[3]}
    return None


def main():
    ts = datetime.now().isoformat()
    report = []
    all_pass = True

    # 1) Smoke
    report.append("\n=== Smoke tests ===\n")
    smoke = run_smoke_tests()
    if smoke["status"] == "PASS":
        report.append("PASS\n")
        report.append(smoke["details"])
    else:
        report.append("FAIL")
        report.append(smoke["details"])
        all_pass = False

    # 2) DB
    report.append("\n\n=== DB checks ===\n")
    db = run_db_checks()
    for c in db["checks"]:
        line = f"  [{c['status']}] {c['name']}: {c['msg']}\n"
        report.append(line)
        if c["status"] == "FAIL":
            all_pass = False
    for w in db["warnings"]:
        report.append(f"  WARN: {w}\n")

    # 3) HTTP
    report.append("\n\n=== HTTP checks ===\n")
    sample = get_sample_route_from_db()
    http = run_http_checks(sample)
    for c in http["checks"]:
        line = f"  [{c['status']}] {c['name']}: {c['msg']}\n"
        report.append(line)
        if c["status"] == "FAIL":
            all_pass = False
    for w in http["warnings"]:
        report.append(f"  WARN: {w}\n")

    # Console
    print("".join(report))
    print("\n" + "=" * 40)
    print("PASS" if all_pass else "FAIL")
    print("=" * 40)

    # JSON
    out = {
        "timestamp": ts,
        "smoke": smoke,
        "db": db,
        "http": http,
        "overall": "PASS" if all_pass else "FAIL",
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nResults saved to {RESULTS_FILE}")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
