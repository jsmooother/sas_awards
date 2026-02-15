#!/bin/bash
set -euo pipefail
cd ~/sas_awards

OUT=~/OneDrive/SASReports/business_new_by_date_$(date +%Y-%m-%d).csv

sqlite3 sas_awards.sqlite <<'SQL' > "$OUT"
.headers on
.mode csv

-- Outbound Business new today (>=2 seats)
SELECT 'Outbound Business'      AS Section, '' AS Origin, '' AS Code, '' AS Date, '' AS Seats
UNION ALL
SELECT city_name, origin, airport_code, date, ab
FROM (
  WITH today AS (
    SELECT origin, airport_code, city_name, date
    FROM flight_history
    WHERE fetch_date = date('now')
      AND ab >= 2
      AND direction = 'outbound'
  ),
  yest AS (
    SELECT origin, airport_code, date
    FROM flight_history
    WHERE fetch_date = date('now','-1 day')
      AND ab >= 2
      AND direction = 'outbound'
  )
  SELECT t.city_name, t.origin, t.airport_code, t.date, fh.ab
  FROM today t
  JOIN flight_history fh
    ON fh.fetch_date = date('now') AND fh.origin = t.origin
   AND fh.airport_code = t.airport_code AND fh.direction = 'outbound' AND fh.date = t.date
  LEFT JOIN yest y
    ON y.origin = t.origin AND y.airport_code = t.airport_code AND y.date = t.date
  WHERE y.airport_code IS NULL
)
ORDER BY origin, city_name COLLATE NOCASE, date;

-- blank row
SELECT '', '', '', '', '';

-- Inbound Business new today (>=2 seats)
SELECT 'Inbound Business'       AS Section, '' AS Origin, '' AS Code, '' AS Date, '' AS Seats
UNION ALL
SELECT city_name, origin, airport_code, date, ab
FROM (
  WITH today AS (
    SELECT origin, airport_code, city_name, date
    FROM flight_history
    WHERE fetch_date = date('now')
      AND ab >= 2
      AND direction = 'inbound'
  ),
  yest AS (
    SELECT origin, airport_code, date
    FROM flight_history
    WHERE fetch_date = date('now','-1 day')
      AND ab >= 2
      AND direction = 'inbound'
  )
  SELECT t.city_name, t.origin, t.airport_code, t.date, fh.ab
  FROM today t
  JOIN flight_history fh
    ON fh.fetch_date = date('now') AND fh.origin = t.origin
   AND fh.airport_code = t.airport_code AND fh.direction = 'inbound' AND fh.date = t.date
  LEFT JOIN yest y
    ON y.origin = t.origin AND y.airport_code = t.airport_code AND y.date = t.date
  WHERE y.airport_code IS NULL
)
ORDER BY origin, city_name COLLATE NOCASE, date;
SQL

echo "Written: $OUT"
