#!/bin/bash
cd ~/sas_awards

TODAY=$(date +%Y-%m-%d)
OUT=~/OneDrive/SASReports/business_by_date_${TODAY}.csv

{
  # ── Outbound Business ────────────────────────────────────────────────
  echo "Outbound Business"
  echo "City,Code,Date,Business_Seats"
  sqlite3 sas_awards.sqlite -csv \
    "SELECT
       city_name    AS City,
       airport_code AS Code,
       date         AS Date,
       SUM(ab)      AS Business_Seats
     FROM flights
     WHERE ab > 0
       AND direction = 'outbound'
     GROUP BY city_name, airport_code, date
     ORDER BY city_name COLLATE NOCASE, date;"

  # ── Blank line to separate ───────────────────────────────────────────
  echo

  # ── Inbound Business ─────────────────────────────────────────────────
  echo "Inbound Business"
  echo "City,Code,Date,Business_Seats"
  sqlite3 sas_awards.sqlite -csv \
    "SELECT
       city_name    AS City,
       airport_code AS Code,
       date         AS Date,
       SUM(ab)      AS Business_Seats
     FROM flights
     WHERE ab > 0
       AND direction = 'inbound'
     GROUP BY city_name, airport_code, date
     ORDER BY city_name COLLATE NOCASE, date;"
} > "$OUT"

echo "Written $OUT"
