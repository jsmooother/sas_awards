#!/usr/bin/env bash
set -euo pipefail

cd ~/sas_awards

OUT_DIR=reports/weekend_trips
mkdir -p "$OUT_DIR"

# For each city with an inbound Sat/Sun/Mon award seat in the next year…
sqlite3 sas_awards.sqlite <<'SQL' | while IFS= read -r city; do
.headers off
.mode list
SELECT DISTINCT city_name
FROM flights
WHERE direction='inbound'
  AND (ag>0 OR ap>0)
  AND strftime('%w', date) IN ('6','0','1')
  AND date BETWEEN date('now') AND date('now','+1 year');
SQL

  # find one airport code for the filename
  code=$(sqlite3 sas_awards.sqlite \
    "SELECT airport_code FROM flights WHERE city_name='$city' LIMIT 1;")

  # build the CSV
  file="${OUT_DIR}/${code}_${city// /_}.csv"
  sqlite3 sas_awards.sqlite <<EOF > "$file"
.headers on
.mode csv

SELECT
  inb.date        AS inbound_date,
  outb.date       AS outbound_date,
  inb.ag          AS econ_in,
  inb.ap          AS plus_in,
  outb.ag         AS econ_out,
  outb.ap         AS plus_out
FROM flights AS inb
JOIN flights AS outb
  ON inb.airport_code = outb.airport_code
WHERE
  inb.city_name    = '$city'
  AND inb.direction = 'inbound'
  AND outb.direction= 'outbound'
  AND (inb.ag>0 OR inb.ap>0)
  AND (outb.ag>0 OR outb.ap>0)
  AND strftime('%w', inb.date)  IN ('6','0','1')
  AND strftime('%w', outb.date) IN ('3','4','5')
  AND date(outb.date)
      BETWEEN date(inb.date,'-7 days') 
          AND date(inb.date,'-1 days')
  AND date(inb.date)
      BETWEEN date('now') 
          AND date('now','+1 year')
ORDER BY inb.date, outb.date;
EOF

  echo "Written $file ($(wc -l < "$file") lines)"
done

echo "✅ All done — see files in $OUT_DIR"
