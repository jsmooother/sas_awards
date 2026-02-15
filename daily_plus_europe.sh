#!/bin/bash
cd ~/sas_awards

OUT=~/OneDrive/SASReports/plus_europe_by_city_$(date +%Y-%m-%d).csv

sqlite3 sas_awards.sqlite <<'SQL' > "$OUT"
.headers on
.mode csv

SELECT
  city_name            AS City,
  airport_code         AS Code,
  date                 AS Date,
  direction            AS Direction,
  SUM(ap)              AS Plus_Seats
FROM flights
WHERE ap > 0
  AND country_name IN (
    'Österrike','Belgien','Danmark','Frankrike','Tyskland',
    'Irland','Italien','Nederländerna','Norge',
    'Portugal','Spanien','Sverige','Schweiz','Storbritannien'
  )
GROUP BY
  city_name,
  airport_code,
  date,
  direction
ORDER BY
  city_name COLLATE NOCASE,
  date,
  direction;
SQL

echo "Written $OUT"
