#!/bin/bash
# ~/sas_awards/daily_plus_europe.sh

cd /Users/jesper/sas_awards

OUT=~/OneDrive/SASReports/daily_plus_europe_new_$(date +%Y-%m-%d).csv

sqlite3 sas_awards.sqlite <<'SQL' > "$OUT"
.headers on
.mode csv
SELECT
  date,
  GROUP_CONCAT(city_name, '; ') AS cities_with_plus,
  SUM(ap)                  AS total_plus_seats
FROM flights
WHERE ap > 0
  AND last_seen >= datetime('now','-1 day')
  AND country_name IN (
    'Austria','Belgium','Denmark','France','Germany',
    'Ireland','Italy','Netherlands','Norway',
    'Portugal','Spain','Sweden','Switzerland','United Kingdom'
  )
GROUP BY date
ORDER BY date;
SQL

echo "Written $OUT"
