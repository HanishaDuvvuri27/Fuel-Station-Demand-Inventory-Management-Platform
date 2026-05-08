CREATE TABLE IF NOT EXISTS users (
    user_id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('fuel_user', 'station_owner', 'admin')),
    home_latitude DOUBLE PRECISION,
    home_longitude DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stations (
    station_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    area TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS fuel_inventory (
    id BIGSERIAL PRIMARY KEY,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    fuel_type TEXT NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel')),
    available_liters DOUBLE PRECISION NOT NULL CHECK (available_liters >= 0),
    price DOUBLE PRECISION NOT NULL CHECK (price >= 0),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (station_id, fuel_type)
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id BIGSERIAL PRIMARY KEY,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    fuel_type TEXT NOT NULL CHECK (fuel_type IN ('Petrol', 'Diesel')),
    liters_sold DOUBLE PRECISION NOT NULL,
    txn_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS owner_station_access (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, station_id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    source_path TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_ingested BIGINT NOT NULL DEFAULT 0,
    rows_rejected BIGINT NOT NULL DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS rejected_records (
    rejected_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    source_path TEXT,
    record_payload JSONB NOT NULL,
    rejection_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_demand_summary (
    summary_date DATE NOT NULL,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    fuel_type TEXT NOT NULL,
    liters_sold DOUBLE PRECISION NOT NULL DEFAULT 0,
    transaction_count BIGINT NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (summary_date, station_id, fuel_type)
);

CREATE TABLE IF NOT EXISTS inventory_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    fuel_type TEXT NOT NULL,
    available_liters DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS revenue_rollups (
    rollup_date DATE NOT NULL,
    station_id BIGINT NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    fuel_type TEXT NOT NULL,
    estimated_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    liters_sold DOUBLE PRECISION NOT NULL DEFAULT 0,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (rollup_date, station_id, fuel_type)
);

CREATE TABLE IF NOT EXISTS data_quality_reports (
    report_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES pipeline_runs(run_id) ON DELETE SET NULL,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    anomaly_count BIGINT NOT NULL DEFAULT 0,
    details TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stations_area ON stations(area);
CREATE INDEX IF NOT EXISTS idx_inventory_station_fuel ON fuel_inventory(station_id, fuel_type);
CREATE INDEX IF NOT EXISTS idx_transactions_station_fuel_time ON transactions(station_id, fuel_type, txn_time);
CREATE INDEX IF NOT EXISTS idx_users_username_role ON users(username, role);
CREATE INDEX IF NOT EXISTS idx_owner_access_user_station ON owner_station_access(user_id, station_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started ON pipeline_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_rejected_records_run ON rejected_records(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_reports_run ON data_quality_reports(run_id);
