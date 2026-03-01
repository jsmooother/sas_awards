#!/bin/bash
# Run once: single route/date (update dates as needed)
set -e
cd "$(dirname "$0")/.."
source venv/bin/activate

# Use AF_DATE env, or today+2d (Linux: date -d, macOS: date -v+2d), or 2026-12-15
if [ -n "$AF_DATE" ]; then
  DATE="$AF_DATE"
else
  DATE=$(date -d '+2 days' '+%Y-%m-%d' 2>/dev/null) || \
  DATE=$(date -v+2d '+%Y-%m-%d' 2>/dev/null) || \
  DATE="2026-12-15"
fi

python runner.py run-once \
  --origin PAR \
  --destination JNB \
  --date "$DATE" \
  --cabin ECONOMY \
  "$@"
