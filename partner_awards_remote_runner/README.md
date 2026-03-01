# Partner Awards Remote Fetch Runner

Fetches Air France GraphQL responses on a VPS where the AF edge is reachable. Outputs JSON files ingestible by the main app's `import_folder` script.

## Why

Playwright live fetch is blocked from local networks (home/office/hotspot) due to AF edge behavior. Deploy this runner on a clean VPS (Hetzner, OVH, DigitalOcean) to fetch data and copy it back.

## VPS Setup

### Recommended

- **OS**: Ubuntu 22.04 LTS
- **Size**: 1 vCPU, 1 GB RAM (e.g. Hetzner CX11, DO Basic $6, OVH Starter)
- **Disk**: 10 GB

### Install

```bash
# Clone or copy the runner folder
cd ~
git clone <repo> sas_awards  # or scp/rsync partner_awards_remote_runner/
cd sas_awards/partner_awards_remote_runner

# Install Playwright + Chromium
./scripts/install_playwright.sh
```

### Optional: config.json

Copy and edit:

```bash
cp config.example.json config.json
```

Override `output_dir`, `user_agent`, `pacing_delay_sec`, etc. Env vars: `AF_COUNTRY`, `AF_LANGUAGE`, `AF_OUTPUT_DIR`, `AF_DATE`, `AF_START`.

## Hosts (multi-host failover)

Config prefers **KLM-SE** first, **AF-US** second. Same API, different hostnames. If one blocks or times out, the runner tries the next.

## Run Modes

### Run once (single route/date)

```bash
source venv/bin/activate

# Current date + 2 days (or set AF_DATE)
python runner.py run-once --origin PAR --destination JNB --date 2026-02-27 --cabin ECONOMY

# December 2026
python runner.py run-once --origin PAR --destination JNB --date 2026-12-15 --cabin ECONOMY

# Dry-run (validate config and paths only)
python runner.py run-once --origin PAR --destination JNB --date 2026-12-15 --cabin ECONOMY --dry-run
```

Output: `outputs/AF/PAR-JNB/2026-02-27/available_offers_ECONOMY_20260227_123456.json`

### Calendar scan (14 days, candidate dates)

```bash
# Calendar-only (no drilldown to full offers) — best miles per day
python runner.py calendar-scan \
  --origin AMS --destination JNB \
  --start 2026-03-01 --days 14 \
  --cabins ECONOMY,BUSINESS \
  --max-offer-days 0

# Calendar + drilldown (fetch available offers for top dates)
python runner.py calendar-scan \
  --origin AMS --destination JNB \
  --start 2026-03-01 --days 14 \
  --cabins ECONOMY,BUSINESS \
  --max-offer-days 5
```

`--max-offer-days 0` = calendar-only mode, outputs `lowest_fares_*.json`.

### Open dates month (single month, MONTH mode)

Fetches a full month for the KLM-style "open dates" calendar:

```bash
python runner.py open-dates-month --origin AMS --destination JNB --month 2026-03 --cabins BUSINESS
```

Output: `outputs/AF/AMS-JNB/2026-03/lowest_fares_MONTH_BUSINESS_<ts>.json`

### Verify output

```bash
python runner.py verify-output --path outputs/AF
```

Checks `.meta.json` exists and JSON files have `meta_ref` + `body`.

## Copy outputs to local machine

### scp

```bash
# From local machine
scp -r user@vps-ip:~/sas_awards/partner_awards_remote_runner/outputs/AF ./outputs_backup/
```

### rsync

```bash
rsync -avz user@vps-ip:~/sas_awards/partner_awards_remote_runner/outputs/ ./partner_outputs/
```

## Ingest locally

From the main sas_awards repo:

```bash
source venv/bin/activate

# Ingest a single date folder (from scp/rsync)
python -m partner_awards.airfrance.import_folder --path ./outputs_backup/AF/PAR-JNB/2026-02-27

# Ingest entire outputs tree
python -m partner_awards.airfrance.import_folder --path ./partner_outputs
```

Then open Partner Awards in the Flask app and "Load offers".

## Cron (nightly at 02:00)

```bash
crontab -e
```

Add:

```
0 2 * * * cd /home/user/sas_awards/partner_awards_remote_runner && ./scripts/run_cron.sh >> outputs/cron.log 2>&1
```

Adjust path and user.

## Output structure

```
outputs/
├── AF/
│   ├── AMS-JNB/
│   │   └── 2026-03/
│   │       ├── .meta.json
│   │       └── lowest_fares_MONTH_BUSINESS_<ts>.json
│   └── PAR-JNB/
│       ├── 2026-02-27/
│       │   ├── available_offers_ECONOMY_20260227_120000.json
│       │   └── available_offers_BUSINESS_20260227_120030.json
│       └── 2026-12-15/
│           └── available_offers_ECONOMY_20261215_020000.json
└── run_20260225.log
```

## Troubleshooting

- **Warmup fails**: VPS egress may also be blocked. Try a different provider or region.
- **403/429/503**: Retries with 3s backoff are automatic. Increase `pacing_delay_sec` in config.
- **Timeout**: Default 60s. Increase `gql_timeout_ms` in config.json.
- **Empty data for some routes (e.g. AMS→CPT)** when the KLM website shows availability: The API often returns empty for unauthenticated requests. Use your **Flying Blue session cookies**:
  1. Log in to https://www.klm.se and open the award calendar (Use your Miles).
  2. In DevTools → Network, trigger a search that returns data.
  3. Find the `SharedSearchLowestFareOffersForSearchQuery` request → Copy as cURL.
  4. Extract the `-b '...'` cookie string and either:
     - Add to `config.json`: `"cookie_string": "name1=val1; name2=val2; ..."` (the full `-b` value), or
     - Set env: `export AF_COOKIE_STRING="name1=val1; name2=val2; ..."` before running.
  Cookies expire; refresh when scans start returning empty again.
