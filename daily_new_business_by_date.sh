#!/bin/bash
set -euo pipefail
cd ~/sas_awards

OUT=~/OneDrive/SASReports/business_new_by_date_$(date +%Y-%m-%d).csv

sqlite3 sas_awards.sqlite <<'SQL' > "$OUT"
.headers on
.mode csv

-- Outbound Business new today
SELECT 'Outbound Business'      AS Section, '' AS Code, '' AS Date, '' AS Seats
UNION ALL
SELECT city_name, airport_code, flight_date, ab
FROM (
  WITH today AS (
    SELECT airport_code, city_name, flight_date
    FROM flight_history
    WHERE fetch_date = date('now')
      AND ab > 0
      AND direction = 'outbound'
  ),
  yest AS (
    SELECT airport_code, flight_date
    FROM flight_history
    WHERE fetch_date = date('now','-1 day')
      AND ab > 0
      AND direction = 'outbound'
  )
  SELECT t.city_name, t.airport_code, t.flight_date, fh.ab
  FROM today t
  JOIN flight_history fh
    ON fh.fetch_date   = date('now')
   AND fh.airport_code = t.airport_code
   AND fh.direction    = 'outbound'
   AND fh.flight_date  = t.flight_date
  LEFT JOIN yest y
    ON y.airport_code = t.airport_code
   AND y.flight_date  = t.flight_date
  WHERE y.airport_code IS NULL
)
ORDER BY city_name COLLATE NOCASE, flight_date;

-- blank row
SELECT '', '', '', '';

-- Inbound Business new today
SELECT 'Inbound Business'       AS Section, '' AS Code, '' AS Date, '' AS Seats
UNION ALL
SELECT city_name, airport_code, flight_date, ab
FROM (
  WITH today AS (
    SELECT airport_code, city_name, flight_date
    FROM flight_history
    WHERE fetch_date = date('now')
      AND ab > 0
      AND direction = 'inbound'
  ),
  yest AS (
    SELECT airport_code, flight_date
    FROM flight_history
    WHERE fetch_date = date('now','-1 day')
      AND ab > 0
      AND direction = 'inbound'
  )
  SELECT t.city_name, t.airport_code, t.flight_date, fh.ab
  FROM today t
  JOIN flight_history fh
    ON fh.fetch_date   = date('now')
   AND fh.airport_code = t.airport_code
   AND fh.direction    = 'inbound'
   AND fh.flight_date  = t.flight_date
  LEFT JOIN yest y
    ON y.airport_code = t.airport_code
   AND y.flight_date  = t.flight_date
  WHERE y.airport_code IS NULL
)
ORDER BY city_name COLLATE NOCASE, flight_date;
SQL

echo "Written: $OUT"
