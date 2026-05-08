PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('fuel_user', 'station_owner', 'admin')),
    home_latitude REAL,
    home_longitude REAL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stations (
    station_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    area TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fuel_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    fuel_type TEXT NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel')),
    available_liters REAL NOT NULL CHECK (available_liters >= 0),
    price REAL NOT NULL CHECK (price >= 0),
    last_updated TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE,
    UNIQUE (station_id, fuel_type)
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    fuel_type TEXT NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel')),
    liters_sold REAL NOT NULL,
    txn_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS owner_station_access (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    station_id INTEGER NOT NULL,
    assigned_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE,
    UNIQUE (user_id, station_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    source_path TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_ingested INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS rejected_records (
    rejected_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    source_path TEXT,
    record_payload TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS daily_demand_summary (
    summary_date TEXT NOT NULL,
    station_id INTEGER NOT NULL,
    fuel_type TEXT NOT NULL,
    liters_sold REAL NOT NULL DEFAULT 0,
    transaction_count INTEGER NOT NULL DEFAULT 0,
    refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_date, station_id, fuel_type),
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS inventory_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT NOT NULL,
    station_id INTEGER NOT NULL,
    fuel_type TEXT NOT NULL,
    available_liters REAL NOT NULL,
    price REAL NOT NULL,
    captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS revenue_rollups (
    rollup_date TEXT NOT NULL,
    station_id INTEGER NOT NULL,
    fuel_type TEXT NOT NULL,
    estimated_revenue REAL NOT NULL DEFAULT 0,
    liters_sold REAL NOT NULL DEFAULT 0,
    refreshed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rollup_date, station_id, fuel_type),
    FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data_quality_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    anomaly_count INTEGER NOT NULL DEFAULT 0,
    details TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_stations_area ON stations(area);
CREATE INDEX IF NOT EXISTS idx_inventory_station_fuel ON fuel_inventory(station_id, fuel_type);
CREATE INDEX IF NOT EXISTS idx_transactions_station_fuel_time ON transactions(station_id, fuel_type, txn_time);
CREATE INDEX IF NOT EXISTS idx_users_username_role ON users(username, role);
CREATE INDEX IF NOT EXISTS idx_owner_access_user_station ON owner_station_access(user_id, station_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_rejected_records_run ON rejected_records(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_reports_run ON data_quality_reports(run_id);
