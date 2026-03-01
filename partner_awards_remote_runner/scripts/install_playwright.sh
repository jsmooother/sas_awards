#!/bin/bash
# Install Playwright and Chromium for Remote Fetch Runner
set -e
cd "$(dirname "$0")/.."
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
echo "Done. Activate with: source venv/bin/activate"
