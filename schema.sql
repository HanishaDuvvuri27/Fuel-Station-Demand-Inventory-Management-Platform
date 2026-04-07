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

CREATE INDEX IF NOT EXISTS idx_stations_area ON stations(area);
CREATE INDEX IF NOT EXISTS idx_inventory_station_fuel ON fuel_inventory(station_id, fuel_type);
CREATE INDEX IF NOT EXISTS idx_transactions_station_fuel_time ON transactions(station_id, fuel_type, txn_time);
CREATE INDEX IF NOT EXISTS idx_users_username_role ON users(username, role);
CREATE INDEX IF NOT EXISTS idx_owner_access_user_station ON owner_station_access(user_id, station_id);
