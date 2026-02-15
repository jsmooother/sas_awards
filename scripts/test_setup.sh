#!/bin/bash
# SAS Awards – Quick setup and API test
# Run from project root: ./scripts/test_setup.sh

set -e
cd "$(dirname "$0")/.."

echo "=== SAS Awards – Setup Test ==="
echo ""

# 1. Check Python and venv
echo "1. Python & venv"
if [ ! -d venv ]; then
  echo "   Creating venv..."
  python3 -m venv venv
fi
source venv/bin/activate
echo "   Python: $(which python3)"
pip install -q -r requirements.txt
echo "   ✓ Dependencies OK"
echo ""

# 2. Data directory
echo "2. Data directory"
mkdir -p ~/sas_awards
# Use project dir for DB if we're not in ~/sas_awards
PROJECT_DIR="$(pwd)"
if [ "$PROJECT_DIR" = "$HOME/sas_awards" ]; then
  DB_DIR="$HOME/sas_awards"
else
  DB_DIR="$HOME/sas_awards"
  echo "   Project at $PROJECT_DIR"
  echo "   DB will be at $DB_DIR/sas_awards.sqlite"
fi
echo "   ✓ $DB_DIR exists"
echo ""

# 3. Quick API test (one destination, one origin)
echo "3. API test (CPH → BCN)"
RESULT=$(python3 -c "
import requests
import os
os.environ.setdefault('SAS_DB_PATH', '$DB_DIR/sas_awards.sqlite')
r = requests.get('https://www.sas.se/bff/award-finder/destinations/v1',
  params={'market':'se-sv','origin':'CPH','destinations':'BCN','availability':'true','passengers':1},
  timeout=30)
data = r.json()
if data:
  d = data[0]
  out = len(d.get('availability',{}).get('outbound',[]))
  inc = len(d.get('availability',{}).get('inbound',[]))
  print(f'{out},{inc}')
else:
  print('0,0')
" 2>/dev/null)
OUT=$(echo $RESULT | cut -d, -f1)
INC=$(echo $RESULT | cut -d, -f2)
echo "   CPH→BCN: $OUT outbound dates, $INC inbound dates"
if [ "$OUT" -gt 0 ] || [ "$INC" -gt 0 ]; then
  echo "   ✓ API OK"
else
  echo "   ⚠ API returned no data (check network / SAS site)"
fi
echo ""

# 4. Full fetch (optional – takes a few minutes)
read -p "4. Run full data fetch? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  export SAS_DB_PATH="$DB_DIR/sas_awards.sqlite"
  python3 update_sas_awards.py
  echo ""
  sqlite3 "$DB_DIR/sas_awards.sqlite" "SELECT origin, COUNT(*) FROM flights GROUP BY origin;" 2>/dev/null || true
else
  echo "   Skipped. Run: python update_sas_awards.py"
fi
echo ""
echo "=== Done ==="
