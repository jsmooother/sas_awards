#!/bin/bash
set -euo pipefail
cd ~/sas_awards

# On macOS, use `-v -1d` to get yesterday
TODAY=$(date +%Y-%m-%d)
YEST=$(date -v -1d +%Y-%m-%d)

REPORT_DIR=~/OneDrive/SASReports
TODAY_FILE=$REPORT_DIR/plus_europe_by_city_${TODAY}.csv
YEST_FILE=$REPORT_DIR/plus_europe_by_city_${YEST}.csv
NEW_FILE=$REPORT_DIR/plus_europe_new_${TODAY}.csv

# Ensure snapshot files exist
if [[ ! -f "$TODAY_FILE" ]]; then
  echo "❌ Missing today’s snapshot: $TODAY_FILE" >&2
  exit 1
fi
if [[ ! -f "$YEST_FILE" ]]; then
  echo "⚠️  No yesterday’s snapshot ($YEST_FILE).  Writing all today's entries as “new.”"
  cp "$TODAY_FILE" "$NEW_FILE"
  echo "Written $NEW_FILE"
  exit 0
fi

# Strip headers, sort, and diff
tail -n +2 "$TODAY_FILE" | sort > /tmp/plus_today.txt
tail -n +2 "$YEST_FILE" | sort > /tmp/plus_yest.txt

# Write header to new‐flights file
head -n1 "$TODAY_FILE" > "$NEW_FILE"

# Lines in today but not in yesterday
comm -23 /tmp/plus_today.txt /tmp/plus_yest.txt >> "$NEW_FILE"

echo "✅ Written new‐Plus‐Europe flights to $NEW_FILE"
