#!/bin/bash
# Run open-dates-month → import → month report (Telegram text)
# Usage: ./scripts/partner_awards_month_report.sh AMS JNB 2026-03 BUSINESS
set -e

ORIGIN="${1:?Usage: $0 ORIGIN DESTINATION MONTH CABIN}"
DESTINATION="${2:?Usage: $0 ORIGIN DESTINATION MONTH CABIN}"
MONTH="${3:?Usage: $0 ORIGIN DESTINATION MONTH CABIN}"
CABIN="${4:-BUSINESS}"

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RUNNER="$REPO/partner_awards_remote_runner"

# 1) Fetch month from remote runner
cd "$RUNNER"
if [ -f venv/bin/activate ]; then
  source venv/bin/activate
fi
python runner.py open-dates-month --origin "$ORIGIN" --destination "$DESTINATION" --month "$MONTH" --cabins "$CABIN"

# 2) Import into DB
cd "$REPO"
if [ -f venv/bin/activate ]; then
  source venv/bin/activate
fi
python -m partner_awards.airfrance.import_folder --path "$RUNNER/outputs/AF"

# 3) Print Telegram text
python -m partner_awards.airfrance.month_report --origin "$ORIGIN" --destination "$DESTINATION" --month "$MONTH" --cabin "$CABIN"
