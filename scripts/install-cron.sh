#!/bin/bash
# Install cron jobs for SAS Awards data updates.
# Run from project root: ./scripts/install-cron.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="$PROJECT_DIR/venv/bin/python"
LOG="$PROJECT_DIR/run.log"

# Cron entries
UPDATER="# SAS Awards: fetch fresh flight data once per night (~5am)
0 5 * * * cd $PROJECT_DIR && SAS_DB_PATH=$PROJECT_DIR/sas_awards.sqlite $PYTHON update_sas_awards.py >> $LOG 2>&1"

MORNING_REPORT="# SAS Awards: morning report to Telegram (06:20, after updater at 05:00)
# Requires TELEGRAM_CHAT_ID in .env - get it: message bot, then https://api.telegram.org/bot<TOKEN>/getUpdates
20 6 * * * cd $PROJECT_DIR && SAS_DB_PATH=$PROJECT_DIR/sas_awards.sqlite $PYTHON scripts/morning_report.py >> $LOG 2>&1"

echo "=== SAS Awards – cron install ==="
echo "Project: $PROJECT_DIR"
echo ""

# Load .env for TELEGRAM_CHAT_ID check
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  source "$PROJECT_DIR/.env" 2>/dev/null || true
  set +a
fi

# Build new crontab
EXISTING=$(crontab -l 2>/dev/null || true)

# Remove old SAS Awards entries
CLEANED=$(echo "$EXISTING" | grep -v "sas_awards\|sas-awards\|update_sas_awards\|morning_report" || true)

# Add new entries
NEW_CRON="$CLEANED

$UPDATER
"

# Optionally add morning report if TELEGRAM_CHAT_ID is set
if [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
  NEW_CRON="$NEW_CRON
$MORNING_REPORT
"
  echo "✓ Morning report: enabled (TELEGRAM_CHAT_ID found in .env)"
else
  echo "○ Morning report: skipped (add TELEGRAM_CHAT_ID to .env to enable)"
fi

echo "$NEW_CRON" | crontab -
echo ""
echo "✓ Cron installed. Current crontab:"
echo ""
crontab -l | grep -v "^$" | grep -v "^#"
echo ""
echo "Log: $LOG"
