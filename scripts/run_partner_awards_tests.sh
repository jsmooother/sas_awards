#!/usr/bin/env bash
# Run Partner Awards test suite
# Usage: ./scripts/run_partner_awards_tests.sh

cd "$(dirname "$0")/.."

echo "Partner Awards test runner"
echo ""
echo "Reminders:"
echo "  - Start Flask:  python app.py"
echo "  - Optional:    python -m partner_awards.jobs_worker"
echo ""

exec python -m partner_awards.tests.run_all
