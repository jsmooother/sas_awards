-- Partner Awards schema (SQLite)
-- Tables prefixed with partner_award_ to keep separate from SAS

-- Scan runs (optional but recommended)
CREATE TABLE IF NOT EXISTS partner_award_scan_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  origin TEXT,
  destination TEXT,
  cabin_requested TEXT,
  depart_date TEXT,
  started_at TEXT NOT NULL DEFAULT (datetime('now')),
  ended_at TEXT,
  status TEXT NOT NULL DEFAULT 'ok',
  error TEXT
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
