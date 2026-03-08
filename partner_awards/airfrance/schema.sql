-- Partner Awards schema (SQLite)
-- Tables prefixed with partner_award_ to keep separate from SAS

-- Scan runs (optional but recommended)
CREATE TABLE IF NOT EXISTS partner_award_scan_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  ingest_type TEXT DEFAULT 'fixture',
  origin TEXT,
  destination TEXT,
  cabin_requested TEXT,
  depart_date TEXT,
  started_at TEXT NOT NULL DEFAULT (datetime('now')),
  ended_at TEXT,
  status TEXT NOT NULL DEFAULT 'ok',
  error TEXT,
  host_used TEXT,
  host_attempts TEXT
);

-- Raw responses for forensic debugging / re-parsing later
CREATE TABLE IF NOT EXISTS partner_award_raw_responses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scan_run_id INTEGER REFERENCES partner_award_scan_runs(id),
  source TEXT NOT NULL,
  operation_name TEXT NOT NULL,
  origin TEXT,
  destination TEXT,
  depart_date TEXT,
  cabin_requested TEXT,
  retrieved_at TEXT NOT NULL DEFAULT (datetime('now')),
  body TEXT NOT NULL
);

-- Normalized itinerary offers (one row per itinerary)
CREATE TABLE IF NOT EXISTS partner_award_offers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scan_run_id INTEGER REFERENCES partner_award_scan_runs(id),
  source TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  depart_date TEXT NOT NULL,
  cabin_requested TEXT,
  itinerary_key TEXT NOT NULL,
  stops INTEGER NOT NULL,
  duration_minutes INTEGER,
  carriers TEXT,
  segments TEXT NOT NULL,
  raw_offer_id TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (source, origin, destination, depart_date, itinerary_key)
);

-- Cabin pricing/availability per itinerary
CREATE TABLE IF NOT EXISTS partner_award_offer_cabins (
  offer_id INTEGER NOT NULL REFERENCES partner_award_offers(id) ON DELETE CASCADE,
  cabin_class TEXT NOT NULL,
  miles INTEGER,
  miles_currency TEXT,
  tax REAL,
  tax_currency TEXT,
  seats_available INTEGER,
  fare_family TEXT,
  flight_details_path TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (offer_id, cabin_class)
);

CREATE INDEX IF NOT EXISTS idx_partner_award_offers_route_date
  ON partner_award_offers (source, origin, destination, depart_date);

CREATE INDEX IF NOT EXISTS idx_partner_award_offer_cabins_miles
  ON partner_award_offer_cabins (cabin_class, miles);

-- Best offer per date/cabin (compact, fast UX)
CREATE TABLE IF NOT EXISTS partner_award_best_offers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  depart_date TEXT NOT NULL,
  cabin_class TEXT NOT NULL,
  best_miles INTEGER,
  best_tax REAL,
  is_direct INTEGER NOT NULL DEFAULT 0,
  duration_minutes INTEGER,
  carrier TEXT,
  offer_id INTEGER REFERENCES partner_award_offers(id),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (source, origin, destination, depart_date, cabin_class)
);

CREATE INDEX IF NOT EXISTS idx_partner_award_best_offers_route
  ON partner_award_best_offers (source, origin, destination, depart_date);

-- Calendar fares from LowestFareOffers (date-level miles, no full itineraries)
-- UNIQUE includes host_used to allow multiple hosts; best-overall = MIN(miles) per route/date/cabin
CREATE TABLE IF NOT EXISTS partner_award_calendar_fares (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scan_run_id INTEGER REFERENCES partner_award_scan_runs(id),
  host_used TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT 'AF',
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  cabin_class TEXT NOT NULL,
  depart_date TEXT NOT NULL,
  miles INTEGER,
  tax REAL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (source, origin, destination, cabin_class, depart_date, host_used)
);

CREATE INDEX IF NOT EXISTS idx_partner_award_calendar_fares_route
  ON partner_award_calendar_fares (source, origin, destination, depart_date);

-- Watchlist: routes to track per program (sas | flyingblue | virgin)
CREATE TABLE IF NOT EXISTS partner_award_watch_routes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  program TEXT NOT NULL,
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (program, origin, destination)
);

CREATE INDEX IF NOT EXISTS idx_partner_award_watch_routes_program
  ON partner_award_watch_routes (program);

-- Batch jobs (worker-driven scans)
CREATE TABLE IF NOT EXISTS partner_award_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  program TEXT NOT NULL,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  started_at TEXT,
  finished_at TEXT,
  params_json TEXT NOT NULL DEFAULT '{}',
  progress_json TEXT NOT NULL DEFAULT '{}',
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_partner_award_jobs_program_status
  ON partner_award_jobs (program, status);

CREATE TABLE IF NOT EXISTS partner_award_job_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL REFERENCES partner_award_jobs(id),
  origin TEXT NOT NULL,
  destination TEXT NOT NULL,
  month TEXT NOT NULL,
  cabin TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  started_at TEXT,
  finished_at TEXT,
  attempts INTEGER DEFAULT 0,
  last_error TEXT,
  output_folder TEXT
);

CREATE INDEX IF NOT EXISTS idx_partner_award_job_tasks_job_id
  ON partner_award_job_tasks (job_id);

-- Best overall: min miles per route/date/cabin (computed in query)
