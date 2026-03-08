#!/bin/bash
# Cron job: calendar scan (update dates as needed)
set -e
cd "$(dirname "$0")/.."
source venv/bin/activate

# Start from AF_START env or today
START="${AF_START:-$(date '+%Y-%m-%d')}"

python runner.py calendar-scan \
  --origin AMS \
  --destination JNB \
  --start "$START" \
  --days 14 \
  --cabins ECONOMY,BUSINESS \
  --max-offer-days 5 \
  "$@"
