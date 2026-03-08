#!/usr/bin/env python3
"""
Dev script: ingest fixture JSON and verify offers count > 0.
Run from project root: python -m partner_awards.airfrance.dev_ingest_fixture
"""
import os
import sqlite3
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from partner_awards.airfrance.service import ingest_fixture

PARTNER_DB_DIR = os.path.expanduser(os.environ.get("SAS_DB_PATH", "~/sas_awards"))
PARTNER_DB_PATH = os.environ.get("PARTNER_AWARDS_DB_PATH") or os.path.join(
    PARTNER_DB_DIR, "partner_awards.sqlite"
)


def main():
    Path(PARTNER_DB_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(PARTNER_DB_PATH)
    try:
        result = ingest_fixture(
            conn,
            origin="PAR",
            destination="JNB",
            depart_date="2026-02-27",
            cabin_requested="ECONOMY",
        )
        conn.commit()
        ok = result.get("ok")
        count = result.get("inserted_offer_count", 0)
        if ok and count > 0:
            print(f"OK: Ingested {count} offers from fixture.")
            return 0
        if not ok:
            print(f"FAIL: {result.get('error', 'unknown')}")
            return 1
        print(f"FAIL: No offers parsed (inserted_offer_count={count})")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
