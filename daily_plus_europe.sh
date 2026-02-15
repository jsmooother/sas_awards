#!/bin/bash
# Plus Europe report: min 2 seats (see report_config.py)
cd ~/sas_awards
: "${SAS_DB_PATH:=sas_awards.sqlite}"
OUT=~/OneDrive/SASReports/plus_europe_by_city_$(date +%Y-%m-%d).csv

sqlite3 "$SAS_DB_PATH" <<'SQL' > "$OUT"
.headers on
.mode csv

SELECT
  origin               AS Origin,
  city_name            AS City,
  airport_code         AS Code,
  date                 AS Date,
  direction            AS Direction,
  SUM(ap)              AS Plus_Seats
FROM flights
WHERE ap >= 2
  AND country_name IN (
    'Österrike','Belgien','Danmark','Frankrike','Tyskland',
    'Irland','Italien','Nederländerna','Norge',
    'Portugal','Spanien','Sverige','Schweiz','Storbritannien'
  )
GROUP BY
  origin,
  city_name,
  airport_code,
  date,
  direction
ORDER BY
  origin,
  city_name COLLATE NOCASE,
  date,
  direction;
SQL

echo "Written $OUT"
