#!/bin/bash
# Business report: min 2 seats (see report_config.py)
cd ~/sas_awards
: "${SAS_DB_PATH:=sas_awards.sqlite}"
TODAY=$(date +%Y-%m-%d)
OUT=~/OneDrive/SASReports/business_by_date_${TODAY}.csv

{
  echo "Outbound Business (min 2 seats)"
  echo "Origin,City,Code,Date,Business_Seats"
  sqlite3 "$SAS_DB_PATH" -csv \
    "SELECT
       origin       AS Origin,
       city_name    AS City,
       airport_code AS Code,
       date         AS Date,
       SUM(ab)      AS Business_Seats
     FROM flights
     WHERE ab >= 2
       AND direction = 'outbound'
     GROUP BY origin, city_name, airport_code, date
     ORDER BY origin, city_name COLLATE NOCASE, date;"

  echo
  echo "Inbound Business (min 2 seats)"
  echo "Origin,City,Code,Date,Business_Seats"
  sqlite3 "$SAS_DB_PATH" -csv \
    "SELECT
       origin       AS Origin,
       city_name    AS City,
       airport_code AS Code,
       date         AS Date,
       SUM(ab)      AS Business_Seats
     FROM flights
     WHERE ab >= 2
       AND direction = 'inbound'
     GROUP BY origin, city_name, airport_code, date
     ORDER BY origin, city_name COLLATE NOCASE, date;"
} > "$OUT"

echo "Written $OUT"
