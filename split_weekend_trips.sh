#!/usr/bin/env bash
# Weekend trip reports: min 2 seats, 3–4 day trip length (see report_config.py)
set -euo pipefail

if [[ -n "${SAS_DB_PATH:-}" ]]; then
  cd "$(dirname "$SAS_DB_PATH")"
else
  cd ~/sas_awards
  SAS_DB_PATH=sas_awards.sqlite
fi
OUT_DIR=reports/weekend_trips
mkdir -p "$OUT_DIR"

# For each (origin, city) with 2+ inbound Sat/Sun/Mon seats in the next year
sqlite3 "$SAS_DB_PATH" <<'SQL' | while IFS='|' read -r origin city; do
.headers off
.mode list
SELECT DISTINCT origin, city_name
FROM flights
WHERE direction='inbound'
  AND (ag>=2 OR ap>=2)
  AND strftime('%w', date) IN ('6','0','1')
  AND date BETWEEN date('now') AND date('now','+1 year');
SQL

  [ -z "$origin" ] && continue
  code=$(sqlite3 "$SAS_DB_PATH" \
    "SELECT airport_code FROM flights WHERE origin='$origin' AND city_name='$city' LIMIT 1;")
  safe_city="${city// /_}"
  safe_city="${safe_city//\//-}"
  file="${OUT_DIR}/${origin}_${code}_${safe_city}.csv"
  sqlite3 "$SAS_DB_PATH" <<EOF > "$file"
.headers on
.mode csv

SELECT
  inb.origin      AS origin,
  inb.date        AS inbound_date,
  outb.date       AS outbound_date,
  inb.ag          AS econ_in,
  inb.ap          AS plus_in,
  outb.ag         AS econ_out,
  outb.ap         AS plus_out
FROM flights AS inb
JOIN flights AS outb
  ON inb.airport_code = outb.airport_code
  AND inb.origin = outb.origin
WHERE
  inb.origin      = '$origin'
  AND inb.city_name= '$city'
  AND inb.direction = 'inbound'
  AND outb.direction= 'outbound'
  AND (inb.ag>=2 OR inb.ap>=2)
  AND (outb.ag>=2 OR outb.ap>=2)
  AND strftime('%w', inb.date)  IN ('6','0','1')
  AND strftime('%w', outb.date) IN ('3','4','5')
  AND (julianday(inb.date) - julianday(outb.date)) BETWEEN 3 AND 4
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
