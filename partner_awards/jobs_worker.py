#!/usr/bin/env python3
"""
Background job worker for Flying Blue batch scans.
Run: python -m partner_awards.jobs_worker

Polls for queued jobs, executes open-dates-month tasks via remote runner, imports results.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Project root (parent of partner_awards/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNNER_DIR = PROJECT_ROOT / "partner_awards_remote_runner"
OUTPUT_AF = RUNNER_DIR / "outputs" / "AF"

PARTNER_DB_DIR = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards"))
DB_PATH = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(PARTNER_DB_DIR, "partner_awards.sqlite")


def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def _log(msg: str):
    print(f"[{datetime.now().isoformat()}] {msg}", flush=True)


def _next_12_months() -> list[str]:
    """Return next 12 months as YYYY-MM from current month."""
    import calendar
    now = datetime.now()
    y, m = now.year, now.month
    out = []
    for _ in range(12):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _route_run_recently(conn: sqlite3.Connection, origin: str, dest: str, month: str, cabin: str, exclude_job_id: int) -> bool:
    """True if this route (origin, dest, month, cabin) was successfully run in the last 24 hours (excluding given job)."""
    cur = conn.execute(
        """SELECT 1 FROM partner_award_job_tasks
           WHERE origin=? AND destination=? AND month=? AND cabin=?
             AND status='done' AND finished_at > datetime('now', '-24 hours')
             AND job_id != ? LIMIT 1""",
        (origin, dest, month, cabin, exclude_job_id),
    )
    return cur.fetchone() is not None


def create_open_dates_tasks(conn: sqlite3.Connection, job_id: int) -> int:
    """Expand job into tasks. Returns total task count. When include_returns=1, also queue return (dest, origin)."""
    cur = conn.execute(
        """SELECT origin, destination, COALESCE(include_returns, 0)
           FROM partner_award_watch_routes WHERE program='flyingblue' AND enabled=1"""
    )
    rows = cur.fetchall()
    months = _next_12_months()
    cabins = ["BUSINESS", "PREMIUM"]
    count = 0
    seen = set()  # (origin, dest, month, cabin) to avoid duplicate tasks
    for (origin, dest, include_returns) in rows:
        legs = [(origin, dest)]
        if include_returns:
            legs.append((dest, origin))
        for (o, d) in legs:
            for cabin in cabins:
                for month in months:
                    key = (o, d, month, cabin)
                    if key in seen:
                        continue
                    seen.add(key)
                    conn.execute(
                        """INSERT INTO partner_award_job_tasks (job_id, origin, destination, month, cabin, status)
                           VALUES (?, ?, ?, ?, ?, 'queued')""",
                        (job_id, o, d, month, cabin),
                    )
                    count += 1
    conn.commit()
    return count


def run_task(origin: str, destination: str, month: str, cabin: str) -> tuple[bool, str, str | None]:
    """
    Run open-dates-month via subprocess. Returns (ok, message, output_folder).
    """
    cmd = [
        sys.executable,
        "runner.py",
        "open-dates-month",
        "--origin", origin,
        "--destination", destination,
        "--month", month,
        "--cabins", cabin,
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(RUNNER_DIR),
            capture_output=True,
            text=True,
            timeout=300,
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or str(result.returncode))[:500]
            return False, err, None
        out_folder = str(OUTPUT_AF / f"{origin}-{destination}" / month)
        return True, "", out_folder
    except subprocess.TimeoutExpired:
        return False, "Timeout (300s)", None
    except Exception as e:
        return False, str(e)[:500], None


def run_import(path: str) -> tuple[bool, str]:
    """Run import_folder. Returns (ok, message)."""
    cmd = [
        sys.executable,
        "-m",
        "partner_awards.airfrance.import_folder",
        "--path", path,
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or str(result.returncode))[:500]
            return False, err
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "Import timeout (120s)"
    except Exception as e:
        return False, str(e)[:500]


def process_job(conn: sqlite3.Connection, job: tuple) -> None:
    job_id, program, job_type = job[0], job[1], job[2]
    params = job[3] if len(job) > 3 else {}
    force_refresh = params.get("force_refresh", False)
    _log(f"Processing job {job_id} type={job_type} force_refresh={force_refresh}")

    conn.execute(
        "UPDATE partner_award_jobs SET status='running', started_at=datetime('now') WHERE id=?",
        (job_id,),
    )
    conn.commit()

    total = create_open_dates_tasks(conn, job_id)
    conn.execute(
        "UPDATE partner_award_jobs SET progress_json=? WHERE id=?",
        (json.dumps({"total_tasks": total, "done_tasks": 0, "skipped_tasks": 0, "current_task": None}), job_id),
    )
    conn.commit()

    cur = conn.execute(
        "SELECT id, origin, destination, month, cabin FROM partner_award_job_tasks WHERE job_id=? AND status='queued' ORDER BY id",
        (job_id,),
    )
    tasks = cur.fetchall()
    done = 0
    failed = 0
    skipped = 0
    last_error = None

    for task_id, origin, dest, month, cabin in tasks:
        if not force_refresh and _route_run_recently(conn, origin, dest, month, cabin, job_id):
            conn.execute(
                "UPDATE partner_award_job_tasks SET status='skipped', finished_at=datetime('now'), last_error='Skipped: run <24h ago' WHERE id=?",
                (task_id,),
            )
            skipped += 1
            done += 1
            conn.execute(
                "UPDATE partner_award_jobs SET progress_json=? WHERE id=?",
                (
                    json.dumps({
                        "total_tasks": total,
                        "done_tasks": done,
                        "skipped_tasks": skipped,
                        "current_task": None,
                    }),
                    job_id,
                ),
            )
            conn.commit()
            _log(f"  Task {origin}→{dest} {month} {cabin} [skipped: <24h]")
            continue

        conn.execute(
            "UPDATE partner_award_job_tasks SET status='running', started_at=datetime('now'), attempts=attempts+1 WHERE id=?",
            (task_id,),
        )
        conn.execute(
            "UPDATE partner_award_jobs SET progress_json=? WHERE id=?",
            (
                json.dumps({
                    "total_tasks": total,
                    "done_tasks": done,
                    "skipped_tasks": skipped,
                    "current_task": f"{origin}→{dest} {month} {cabin}",
                }),
                job_id,
            ),
        )
        conn.commit()

        _log(f"  Task {origin}→{dest} {month} {cabin}")
        ok, err_msg, out_folder = run_task(origin, dest, month, cabin)

        if ok and out_folder and Path(out_folder).exists():
            imp_ok, imp_err = run_import(out_folder)
            if imp_ok:
                conn.execute(
                    "UPDATE partner_award_job_tasks SET status='done', finished_at=datetime('now'), output_folder=? WHERE id=?",
                    (out_folder, task_id),
                )
            else:
                conn.execute(
                    "UPDATE partner_award_job_tasks SET status='failed', finished_at=datetime('now'), last_error=? WHERE id=?",
                    (f"Import: {imp_err}", task_id),
                )
                failed += 1
                last_error = imp_err
        elif ok:
            conn.execute(
                "UPDATE partner_award_job_tasks SET status='done', finished_at=datetime('now'), output_folder=? WHERE id=?",
                (out_folder or "", task_id),
            )
        else:
            conn.execute(
                "UPDATE partner_award_job_tasks SET status='failed', finished_at=datetime('now'), last_error=? WHERE id=?",
                (err_msg, task_id),
            )
            failed += 1
            last_error = err_msg

        done += 1
        conn.execute(
            "UPDATE partner_award_jobs SET progress_json=?, last_error=? WHERE id=?",
            (
                json.dumps({"total_tasks": total, "done_tasks": done, "skipped_tasks": skipped, "current_task": None}),
                last_error,
                job_id,
            ),
        )
        conn.commit()

    job_status = "failed" if failed else "done"
    conn.execute(
        "UPDATE partner_award_jobs SET status=?, finished_at=datetime('now'), progress_json=?, last_error=? WHERE id=?",
        (
            job_status,
            json.dumps({"total_tasks": total, "done_tasks": done, "skipped_tasks": skipped, "current_task": None}),
            last_error if failed else None,
            job_id,
        ),
    )
    conn.commit()
    skip_info = f", {skipped} skipped" if skipped else ""
    _log(f"Job {job_id} finished: {job_status} ({done}/{total} tasks, {failed} failed{skip_info})")


def main():
    sys.path.insert(0, str(PROJECT_ROOT))
    from partner_awards.airfrance.adapter import init_db

    _log("Partner Awards job worker starting")
    if not RUNNER_DIR.exists():
        _log(f"ERROR: Runner dir not found: {RUNNER_DIR}")
        sys.exit(1)

    while True:
        try:
            conn = get_conn()
            init_db(conn)
            cur = conn.execute(
                """SELECT id, program, job_type, params_json FROM partner_award_jobs
                   WHERE program='flyingblue' AND status='queued'
                   ORDER BY id LIMIT 1"""
            )
            row = cur.fetchone()
            conn.close()

            if row:
                job_id, program, job_type, params_json = row[0], row[1], row[2], row[3] or "{}"
                try:
                    params = json.loads(params_json)
                except (json.JSONDecodeError, TypeError):
                    params = {}
                job_tuple = (job_id, program, job_type, params)
                conn = get_conn()
                init_db(conn)
                try:
                    process_job(conn, job_tuple)
                except Exception as e:
                    _log(f"Job error: {e}")
                    try:
                        conn.execute(
                            "UPDATE partner_award_jobs SET status='failed', last_error=?, finished_at=datetime('now') WHERE id=?",
                            (str(e)[:500], job_id),
                        )
                        conn.commit()
                    except Exception:
                        pass
                finally:
                    conn.close()
            else:
                time.sleep(3)
        except Exception as e:
            _log(f"Worker loop error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    main()
