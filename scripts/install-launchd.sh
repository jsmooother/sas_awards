#!/bin/bash
# Install launchd plists so Telegram bot and web dashboard start on login.
# Run from project root: ./scripts/install-launchd.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
LAUNCHD_DIR="$PROJECT_DIR/launchd"

echo "=== SAS Awards – launchd install ==="
echo "Project: $PROJECT_DIR"
echo ""

# Create logs dir
mkdir -p "$PROJECT_DIR/logs"
echo "Created logs dir: $PROJECT_DIR/logs"

# Substitute PROJECT_PATH in plists and install
for plist in com.sasawards.telegram com.sasawards.web; do
  src="$LAUNCHD_DIR/${plist}.plist"
  dst="$LAUNCH_AGENTS/${plist}.plist"
  if [ ! -f "$src" ]; then
    echo "Skip $plist: source not found"
    continue
  fi
  sed "s|PROJECT_PATH|$PROJECT_DIR|g" "$src" > "$dst"
  echo "Installed: $dst"
done

echo ""
echo "Load and start services:"
echo ""
for plist in com.sasawards.telegram com.sasawards.web; do
  dst="$LAUNCH_AGENTS/${plist}.plist"
  if [ -f "$dst" ]; then
    launchctl unload "$dst" 2>/dev/null || true
    launchctl load "$dst"
    echo "  ✓ $plist loaded"
  fi
done

echo ""
echo "Done. Services start on login and restart if they crash."
echo ""
echo "Web dashboard: http://127.0.0.1:5001 (port 5001 to avoid AirPlay on 5000)"
echo ""
echo "Commands:"
echo "  launchctl list | grep sasawards   # check status"
echo "  launchctl stop com.sasawards.telegram"
echo "  launchctl start com.sasawards.telegram"
echo "  tail -f $PROJECT_DIR/logs/telegram-bot.log"
