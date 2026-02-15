#!/bin/bash
# ~/sas_awards/daily_business_by_date.py  (aggregates business by date per origin)

cd ~/sas_awards

OUT=~/OneDrive/SASReports/business_by_date_$(date +%Y-%m-%d).csv

sqlite3 sas_awards.sqlite <<'SQL' > "$OUT"
.headers on
.mode csv
SELECT
  origin,
  date,
  GROUP_CONCAT(
    CASE WHEN direction='outbound' THEN city_name END,
    '; '
  ) AS outbound_cities,
  SUM(
    CASE WHEN direction='outbound' THEN ab ELSE 0 END
  ) AS outbound_business_seats,
  GROUP_CONCAT(
    CASE WHEN direction='inbound' THEN city_name END,
    '; '
  ) AS inbound_cities,
  SUM(
    CASE WHEN direction='inbound'  THEN ab ELSE 0 END
  ) AS inbound_business_seats
FROM flights
WHERE ab >= 2
GROUP BY origin, date
ORDER BY origin, date;
SQL

echo "Written $OUT"
