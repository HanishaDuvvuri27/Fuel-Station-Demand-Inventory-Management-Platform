import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, Optional

import numpy as np
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "fuel_management.db")
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


@contextmanager
def get_connection(db_path: str = DB_PATH) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize_database(reset: bool = False, db_path: str = DB_PATH) -> None:
    with get_connection(db_path) as conn:
        if reset:
            conn.executescript(
                """
                DROP TABLE IF EXISTS data_quality_reports;
                DROP TABLE IF EXISTS revenue_rollups;
                DROP TABLE IF EXISTS inventory_snapshots;
                DROP TABLE IF EXISTS daily_demand_summary;
                DROP TABLE IF EXISTS rejected_records;
                DROP TABLE IF EXISTS pipeline_runs;
                DROP TABLE IF EXISTS owner_station_access;
                DROP TABLE IF EXISTS transactions;
                DROP TABLE IF EXISTS fuel_inventory;
                DROP TABLE IF EXISTS stations;
                DROP TABLE IF EXISTS users;
                """
            )
        with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
            conn.executescript(schema_file.read())


def get_station_count() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM stations").fetchone()
        return int(row["cnt"])


def bulk_insert_stations(df_stations: pd.DataFrame) -> None:
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO stations (name, area, latitude, longitude)
            VALUES (?, ?, ?, ?)
            """,
            df_stations[["name", "area", "latitude", "longitude"]].itertuples(index=False, name=None),
        )


def bulk_insert_inventory(df_inventory: pd.DataFrame) -> None:
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO fuel_inventory (station_id, fuel_type, available_liters, price, last_updated)
            VALUES (?, ?, ?, ?, ?)
            """,
            df_inventory[
                ["station_id", "fuel_type", "available_liters", "price", "last_updated"]
            ].itertuples(index=False, name=None),
        )


def bulk_insert_transactions(df_txn: pd.DataFrame) -> None:
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO transactions (station_id, fuel_type, liters_sold, txn_time)
            VALUES (?, ?, ?, ?)
            """,
            df_txn[["station_id", "fuel_type", "liters_sold", "txn_time"]].itertuples(index=False, name=None),
        )


def bulk_insert_users(df_users: pd.DataFrame) -> None:
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO users (username, password, role, home_latitude, home_longitude)
            VALUES (?, ?, ?, ?, ?)
            """,
            df_users[["username", "password", "role", "home_latitude", "home_longitude"]].itertuples(
                index=False, name=None
            ),
        )


def bulk_insert_owner_station_access(df_access: pd.DataFrame) -> None:
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO owner_station_access (user_id, station_id)
            VALUES (?, ?)
            """,
            df_access[["user_id", "station_id"]].itertuples(index=False, name=None),
        )


def authenticate_user(username: str, password: str) -> Optional[dict]:
    query = """
    SELECT user_id, username, role, home_latitude, home_longitude
    FROM users
    WHERE username = ? AND password = ?
    """
    with get_connection() as conn:
        row = conn.execute(query, (username, password)).fetchone()
    return dict(row) if row else None


def username_exists(username: str) -> bool:
    with get_connection() as conn:
        row = conn.execute("SELECT 1 AS ok FROM users WHERE username = ?", (username,)).fetchone()
    return bool(row)


def create_user(
    username: str,
    password: str,
    role: str,
    home_latitude: Optional[float] = None,
    home_longitude: Optional[float] = None,
) -> int:
    if role not in {"fuel_user", "station_owner", "admin"}:
        raise ValueError("Invalid role.")
    if username_exists(username):
        raise ValueError("Username already exists.")
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (username, password, role, home_latitude, home_longitude)
            VALUES (?, ?, ?, ?, ?)
            """,
            (username.strip(), password, role, home_latitude, home_longitude),
        )
        return int(cur.lastrowid)


def assign_owner_to_stations(user_id: int, station_ids: list[int]) -> None:
    if not station_ids:
        return
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO owner_station_access (user_id, station_id)
            VALUES (?, ?)
            """,
            [(user_id, int(station_id)) for station_id in station_ids],
        )


def get_all_users() -> pd.DataFrame:
    query = """
    SELECT user_id, username, role, home_latitude, home_longitude, created_at
    FROM users
    ORDER BY role, username
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


def get_areas() -> list[str]:
    query = "SELECT DISTINCT area FROM stations ORDER BY area"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
    return [row["area"] for row in rows]


def get_stations_by_area(area: str) -> pd.DataFrame:
    query = """
    SELECT station_id, name, area, latitude, longitude
    FROM stations
    WHERE area = ?
    ORDER BY name
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(area,))


def get_top_stations_highest_availability(
    area: Optional[str] = None, fuel_type: Optional[str] = None, limit: int = 5
) -> pd.DataFrame:
    filters = []
    params: list = []
    if area:
        filters.append("s.area = ?")
        params.append(area)
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT s.station_id, s.name, s.area, fi.fuel_type, fi.available_liters, fi.price, fi.last_updated
    FROM stations s
    JOIN fuel_inventory fi ON s.station_id = fi.station_id
    {where_clause}
    ORDER BY fi.available_liters DESC, fi.price ASC
    LIMIT ?
    """
    params.append(limit)
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_top_stations_lowest_price(
    area: Optional[str] = None, fuel_type: Optional[str] = None, limit: int = 5
) -> pd.DataFrame:
    filters = []
    params: list = []
    if area:
        filters.append("s.area = ?")
        params.append(area)
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT s.station_id, s.name, s.area, fi.fuel_type, fi.available_liters, fi.price, fi.last_updated
    FROM stations s
    JOIN fuel_inventory fi ON s.station_id = fi.station_id
    {where_clause}
    ORDER BY fi.price ASC, fi.available_liters DESC
    LIMIT ?
    """
    params.append(limit)
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_low_stock_stations(
    threshold: float = 200.0,
    area: Optional[str] = None,
    fuel_type: Optional[str] = None,
    station_ids: Optional[list[int]] = None,
) -> pd.DataFrame:
    filters = ["fi.available_liters < ?"]
    params: list = [threshold]
    if area:
        filters.append("s.area = ?")
        params.append(area)
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    SELECT s.station_id, s.name, s.area, fi.fuel_type, fi.available_liters, fi.price, fi.last_updated
    FROM stations s
    JOIN fuel_inventory fi ON s.station_id = fi.station_id
    WHERE {' AND '.join(filters)}
    ORDER BY fi.available_liters ASC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_total_demand_per_station(
    area: Optional[str] = None,
    fuel_type: Optional[str] = None,
    station_ids: Optional[list[int]] = None,
    days: Optional[int] = None,
) -> pd.DataFrame:
    filters = ["t.liters_sold > 0"]
    params: list = []
    if area:
        filters.append("s.area = ?")
        params.append(area)
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)
    if days is not None:
        filters.append("DATE(t.txn_time) >= DATE('now', ?)")
        params.append(f"-{int(days)} day")

    query = f"""
    SELECT
        s.station_id,
        s.name,
        s.area,
        ROUND(SUM(t.liters_sold), 2) AS total_liters_sold
    FROM transactions t
    JOIN stations s ON s.station_id = t.station_id
    WHERE {' AND '.join(filters)}
    GROUP BY s.station_id, s.name, s.area
    ORDER BY total_liters_sold DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_average_price_per_area(
    fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None
) -> pd.DataFrame:
    filters = []
    params: list = []
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT s.area, fi.fuel_type, ROUND(AVG(fi.price), 2) AS avg_price
    FROM fuel_inventory fi
    JOIN stations s ON s.station_id = fi.station_id
    {where_clause}
    GROUP BY s.area, fi.fuel_type
    ORDER BY s.area, fi.fuel_type
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_stations_inventory_view(
    area: Optional[str] = None,
    fuel_type: Optional[str] = None,
    station_ids: Optional[list[int]] = None,
) -> pd.DataFrame:
    filters = []
    params: list = []
    if area:
        filters.append("s.area = ?")
        params.append(area)
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT
        s.station_id,
        s.name,
        s.area,
        s.latitude,
        s.longitude,
        fi.fuel_type,
        fi.available_liters,
        fi.price,
        fi.last_updated
    FROM stations s
    JOIN fuel_inventory fi ON fi.station_id = s.station_id
    {where_clause}
    ORDER BY s.area, s.name, fi.fuel_type
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_nearby_stations(user_lat: float, user_lon: float, fuel_type: str, radius_km: float = 8.0, limit: int = 15) -> pd.DataFrame:
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / 102.0

    query = """
    SELECT
        s.station_id,
        s.name,
        s.area,
        s.latitude,
        s.longitude,
        fi.fuel_type,
        fi.available_liters,
        fi.price,
        fi.last_updated,
        (
            ((s.latitude - ?) * (s.latitude - ?)) * 12321.0 +
            ((s.longitude - ?) * (s.longitude - ?)) * 10404.0
        ) AS distance_sq_km
    FROM stations s
    JOIN fuel_inventory fi ON fi.station_id = s.station_id
    WHERE fi.fuel_type = ?
      AND s.latitude BETWEEN ? AND ?
      AND s.longitude BETWEEN ? AND ?
    ORDER BY distance_sq_km ASC, fi.price ASC
    LIMIT ?
    """
    params = (
        user_lat,
        user_lat,
        user_lon,
        user_lon,
        fuel_type,
        user_lat - lat_delta,
        user_lat + lat_delta,
        user_lon - lon_delta,
        user_lon + lon_delta,
        limit,
    )
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    if df.empty:
        return df
    df["distance_km"] = np.sqrt(df["distance_sq_km"])
    return df[df["distance_km"] <= radius_km].sort_values(["distance_km", "price"]).reset_index(drop=True)


def get_station_inventory_row(station_id: int, fuel_type: str) -> Optional[sqlite3.Row]:
    query = """
    SELECT *
    FROM fuel_inventory
    WHERE station_id = ? AND fuel_type = ?
    """
    with get_connection() as conn:
        row = conn.execute(query, (station_id, fuel_type)).fetchone()
    return row


def get_station_fuel_types(station_id: int) -> pd.DataFrame:
    query = """
    SELECT fuel_type, available_liters, price, last_updated
    FROM fuel_inventory
    WHERE station_id = ?
    ORDER BY fuel_type
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(station_id,))


def get_station_lookup(station_ids: Optional[list[int]] = None) -> pd.DataFrame:
    query = "SELECT station_id, name, area FROM stations"
    params: list = []
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        query += f" WHERE station_id IN ({placeholders})"
        params.extend(station_ids)
    query += " ORDER BY area, name"
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def create_station_with_inventory(
    name: str,
    area: str,
    latitude: float,
    longitude: float,
    petrol_liters: float,
    petrol_price: float,
    diesel_liters: float,
    diesel_price: float,
) -> int:
    if not name.strip():
        raise ValueError("Station name is required.")
    if petrol_liters < 0 or diesel_liters < 0:
        raise ValueError("Initial liters cannot be negative.")
    if petrol_price <= 0 or diesel_price <= 0:
        raise ValueError("Prices must be greater than 0.")

    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO stations (name, area, latitude, longitude)
            VALUES (?, ?, ?, ?)
            """,
            (name.strip(), area.strip(), latitude, longitude),
        )
        station_id = int(cur.lastrowid)
        conn.executemany(
            """
            INSERT INTO fuel_inventory (station_id, fuel_type, available_liters, price, last_updated)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (station_id, "Petrol", petrol_liters, petrol_price),
                (station_id, "Diesel", diesel_liters, diesel_price),
            ],
        )
    return station_id


def get_owner_station_ids(user_id: int) -> list[int]:
    query = "SELECT station_id FROM owner_station_access WHERE user_id = ? ORDER BY station_id"
    with get_connection() as conn:
        rows = conn.execute(query, (user_id,)).fetchall()
    return [int(r["station_id"]) for r in rows]


def get_owner_summary(user_id: int) -> pd.DataFrame:
    query = """
    WITH owner_stations AS (
        SELECT station_id
        FROM owner_station_access
        WHERE user_id = ?
    ),
    demand AS (
        SELECT station_id, SUM(CASE WHEN liters_sold > 0 THEN liters_sold ELSE 0 END) AS sold_liters
        FROM transactions
        GROUP BY station_id
    )
    SELECT
        s.station_id,
        s.name,
        s.area,
        ROUND(COALESCE(d.sold_liters, 0), 2) AS total_sold_liters,
        ROUND(AVG(fi.price), 2) AS avg_price,
        SUM(CASE WHEN fi.available_liters < 200 THEN 1 ELSE 0 END) AS low_stock_fuels
    FROM owner_stations os
    JOIN stations s ON s.station_id = os.station_id
    LEFT JOIN demand d ON d.station_id = s.station_id
    LEFT JOIN fuel_inventory fi ON fi.station_id = s.station_id
    GROUP BY s.station_id, s.name, s.area, d.sold_liters
    ORDER BY total_sold_liters DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(user_id,))


def get_admin_overview() -> dict:
    query = """
    SELECT
        (SELECT COUNT(*) FROM users) AS users_count,
        (SELECT COUNT(*) FROM stations) AS stations_count,
        (SELECT COUNT(*) FROM fuel_inventory WHERE available_liters < 200) AS low_stock_count,
        (SELECT ROUND(SUM(CASE WHEN liters_sold > 0 THEN liters_sold ELSE 0 END), 2) FROM transactions) AS total_sold_liters,
        (SELECT ROUND(AVG(price), 2) FROM fuel_inventory) AS overall_avg_price
    """
    with get_connection() as conn:
        row = conn.execute(query).fetchone()
    return dict(row) if row else {}


def get_city_kpis(days: int = 30) -> dict:
    query = """
    WITH revenue AS (
        SELECT SUM(CASE WHEN t.liters_sold > 0 THEN t.liters_sold * fi.price ELSE 0 END) AS total_revenue
        FROM transactions t
        JOIN fuel_inventory fi ON fi.station_id = t.station_id AND fi.fuel_type = t.fuel_type
        WHERE DATE(t.txn_time) >= DATE('now', ?)
    ),
    turnover AS (
        SELECT
            AVG(
                CASE
                    WHEN fi.available_liters <= 0 THEN NULL
                    ELSE COALESCE(sold.sold_liters, 0) / fi.available_liters
                END
            ) AS avg_turnover
        FROM fuel_inventory fi
        LEFT JOIN (
            SELECT station_id, fuel_type, SUM(liters_sold) AS sold_liters
            FROM transactions
            WHERE liters_sold > 0 AND DATE(txn_time) >= DATE('now', ?)
            GROUP BY station_id, fuel_type
        ) sold ON sold.station_id = fi.station_id AND sold.fuel_type = fi.fuel_type
    )
    SELECT
        ROUND(COALESCE((SELECT total_revenue FROM revenue), 0), 2) AS total_revenue,
        (SELECT COUNT(*) FROM stations) AS active_stations,
        (SELECT COUNT(*) FROM fuel_inventory WHERE available_liters < 200) AS stockout_alerts,
        ROUND(COALESCE((SELECT avg_turnover FROM turnover), 0), 3) AS avg_inventory_turnover
    """
    with get_connection() as conn:
        row = conn.execute(query, (f"-{int(days)} day", f"-{int(days)} day")).fetchone()
    return dict(row) if row else {}


def get_pipeline_run_log(limit: int = 25) -> pd.DataFrame:
    query = """
    SELECT
        run_id,
        pipeline_name,
        source_path,
        started_at,
        completed_at,
        status,
        rows_ingested,
        rows_rejected,
        message
    FROM pipeline_runs
    ORDER BY started_at DESC
    LIMIT ?
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(int(limit),))


def get_latest_quality_report(limit: int = 50) -> pd.DataFrame:
    query = """
    SELECT
        dqr.report_id,
        dqr.run_id,
        dqr.check_name,
        dqr.severity,
        dqr.anomaly_count,
        dqr.details,
        dqr.created_at,
        pr.status AS run_status
    FROM data_quality_reports dqr
    LEFT JOIN pipeline_runs pr ON pr.run_id = dqr.run_id
    ORDER BY dqr.created_at DESC, dqr.report_id DESC
    LIMIT ?
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(int(limit),))


def get_rejected_records(limit: int = 50) -> pd.DataFrame:
    query = """
    SELECT rejected_id, run_id, source_path, rejection_reason, created_at, record_payload
    FROM rejected_records
    ORDER BY created_at DESC, rejected_id DESC
    LIMIT ?
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=(int(limit),))


def get_daily_demand_trend(days: int = 30, fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None) -> pd.DataFrame:
    filters = ["t.liters_sold > 0", "DATE(t.txn_time) >= DATE('now', ?)"]
    params: list = [f"-{days} day"]
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"t.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    SELECT DATE(t.txn_time) AS day, ROUND(SUM(t.liters_sold), 2) AS liters_sold
    FROM transactions t
    WHERE {' AND '.join(filters)}
    GROUP BY DATE(t.txn_time)
    ORDER BY day
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_hourly_demand_pattern(days: int = 30, fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None) -> pd.DataFrame:
    filters = ["t.liters_sold > 0", "DATE(t.txn_time) >= DATE('now', ?)"]
    params: list = [f"-{days} day"]
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"t.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    SELECT strftime('%H', t.txn_time) AS hour_of_day, ROUND(SUM(t.liters_sold), 2) AS liters_sold
    FROM transactions t
    WHERE {' AND '.join(filters)}
    GROUP BY strftime('%H', t.txn_time)
    ORDER BY hour_of_day
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_revenue_estimate_per_station(
    days: int = 30, fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None
) -> pd.DataFrame:
    filters = ["t.liters_sold > 0", "DATE(t.txn_time) >= DATE('now', ?)"]
    params: list = [f"-{days} day"]
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"t.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    SELECT
        s.station_id,
        s.name,
        s.area,
        ROUND(SUM(t.liters_sold), 2) AS liters_sold,
        ROUND(SUM(t.liters_sold * fi.price), 2) AS estimated_revenue
    FROM transactions t
    JOIN fuel_inventory fi ON fi.station_id = t.station_id AND fi.fuel_type = t.fuel_type
    JOIN stations s ON s.station_id = t.station_id
    WHERE {' AND '.join(filters)}
    GROUP BY s.station_id, s.name, s.area
    ORDER BY estimated_revenue DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_demand_growth_by_station(
    fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None, window_days: int = 7
) -> pd.DataFrame:
    filters = ["t.liters_sold > 0"]
    params: list = []
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"t.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    WITH slices AS (
        SELECT
            t.station_id,
            SUM(CASE WHEN DATE(t.txn_time) >= DATE('now', '-{window_days} day') THEN t.liters_sold ELSE 0 END) AS recent_liters,
            SUM(CASE
                WHEN DATE(t.txn_time) >= DATE('now', '-{2 * window_days} day')
                 AND DATE(t.txn_time) < DATE('now', '-{window_days} day') THEN t.liters_sold
                ELSE 0
            END) AS prev_liters
        FROM transactions t
        WHERE {' AND '.join(filters)}
        GROUP BY t.station_id
    )
    SELECT
        s.station_id,
        s.name,
        s.area,
        ROUND(sl.recent_liters, 2) AS recent_liters,
        ROUND(sl.prev_liters, 2) AS previous_liters,
        ROUND(
            CASE
                WHEN sl.prev_liters <= 0 THEN 100.0
                ELSE ((sl.recent_liters - sl.prev_liters) / sl.prev_liters) * 100.0
            END, 2
        ) AS growth_pct
    FROM slices sl
    JOIN stations s ON s.station_id = sl.station_id
    ORDER BY growth_pct DESC, recent_liters DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_price_spread_by_area(fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None) -> pd.DataFrame:
    filters = []
    params: list = []
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT
        s.area,
        fi.fuel_type,
        ROUND(AVG(fi.price), 2) AS avg_price,
        ROUND(MIN(fi.price), 2) AS min_price,
        ROUND(MAX(fi.price), 2) AS max_price,
        ROUND(MAX(fi.price) - MIN(fi.price), 2) AS price_spread
    FROM fuel_inventory fi
    JOIN stations s ON s.station_id = fi.station_id
    {where_clause}
    GROUP BY s.area, fi.fuel_type
    ORDER BY price_spread DESC, s.area
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_inventory_turnover_report(
    days: int = 30, fuel_type: Optional[str] = None, station_ids: Optional[list[int]] = None
) -> pd.DataFrame:
    filters = ["t.liters_sold > 0", "DATE(t.txn_time) >= DATE('now', ?)"]
    params: list = [f"-{days} day"]
    if fuel_type:
        filters.append("t.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"t.station_id IN ({placeholders})")
        params.extend(station_ids)

    query = f"""
    WITH sold AS (
        SELECT t.station_id, t.fuel_type, SUM(t.liters_sold) AS sold_liters
        FROM transactions t
        WHERE {' AND '.join(filters)}
        GROUP BY t.station_id, t.fuel_type
    )
    SELECT
        s.station_id,
        s.name,
        s.area,
        fi.fuel_type,
        ROUND(COALESCE(sold.sold_liters, 0), 2) AS sold_liters_{days}d,
        ROUND(fi.available_liters, 2) AS current_stock_liters,
        ROUND(
            CASE
                WHEN fi.available_liters <= 0 THEN 9999
                ELSE COALESCE(sold.sold_liters, 0) / fi.available_liters
            END, 3
        ) AS turnover_ratio
    FROM fuel_inventory fi
    JOIN stations s ON s.station_id = fi.station_id
    LEFT JOIN sold ON sold.station_id = fi.station_id AND sold.fuel_type = fi.fuel_type
    ORDER BY turnover_ratio DESC, sold_liters_{days}d DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_area_fuel_mix() -> pd.DataFrame:
    query = """
    SELECT
        s.area,
        t.fuel_type,
        ROUND(SUM(CASE WHEN t.liters_sold > 0 THEN t.liters_sold ELSE 0 END), 2) AS sold_liters
    FROM transactions t
    JOIN stations s ON s.station_id = t.station_id
    GROUP BY s.area, t.fuel_type
    ORDER BY s.area, sold_liters DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


def get_stockout_risk_report() -> pd.DataFrame:
    query = """
    WITH recent_sales AS (
        SELECT
            station_id,
            fuel_type,
            SUM(CASE WHEN liters_sold > 0 AND DATE(txn_time) >= DATE('now', '-7 day') THEN liters_sold ELSE 0 END) / 7.0 AS avg_daily_sales_7d
        FROM transactions
        GROUP BY station_id, fuel_type
    )
    SELECT
        s.station_id,
        s.name,
        s.area,
        fi.fuel_type,
        fi.available_liters,
        ROUND(COALESCE(rs.avg_daily_sales_7d, 0), 2) AS avg_daily_sales_7d,
        ROUND(
            CASE
                WHEN COALESCE(rs.avg_daily_sales_7d, 0) <= 0 THEN 9999
                ELSE fi.available_liters / rs.avg_daily_sales_7d
            END, 2
        ) AS estimated_days_to_stockout
    FROM fuel_inventory fi
    JOIN stations s ON s.station_id = fi.station_id
    LEFT JOIN recent_sales rs ON rs.station_id = fi.station_id AND rs.fuel_type = fi.fuel_type
    ORDER BY estimated_days_to_stockout ASC, fi.available_liters ASC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


def get_stockout_risk_report_dynamic(
    lookback_days: int = 7,
    fuel_type: Optional[str] = None,
    station_ids: Optional[list[int]] = None,
) -> pd.DataFrame:
    filters = []
    params: list = []
    if fuel_type:
        filters.append("fi.fuel_type = ?")
        params.append(fuel_type)
    if station_ids:
        placeholders = ",".join("?" for _ in station_ids)
        filters.append(f"s.station_id IN ({placeholders})")
        params.extend(station_ids)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    WITH recent_sales AS (
        SELECT
            station_id,
            fuel_type,
            SUM(
                CASE
                    WHEN liters_sold > 0 AND DATE(txn_time) >= DATE('now', '-{int(lookback_days)} day')
                    THEN liters_sold ELSE 0
                END
            ) / {float(lookback_days)} AS avg_daily_sales
        FROM transactions
        GROUP BY station_id, fuel_type
    )
    SELECT
        s.station_id,
        s.name,
        s.area,
        fi.fuel_type,
        fi.available_liters,
        ROUND(COALESCE(rs.avg_daily_sales, 0), 2) AS avg_daily_sales,
        ROUND(
            CASE
                WHEN COALESCE(rs.avg_daily_sales, 0) <= 0 THEN 9999
                ELSE fi.available_liters / rs.avg_daily_sales
            END, 2
        ) AS estimated_days_to_stockout
    FROM fuel_inventory fi
    JOIN stations s ON s.station_id = fi.station_id
    LEFT JOIN recent_sales rs ON rs.station_id = fi.station_id AND rs.fuel_type = fi.fuel_type
    {where_clause}
    ORDER BY estimated_days_to_stockout ASC, fi.available_liters ASC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def _log_transaction(
    conn: sqlite3.Connection, station_id: int, fuel_type: str, liters_sold: float, txn_time: Optional[str] = None
) -> None:
    resolved_time = txn_time or datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        INSERT INTO transactions (station_id, fuel_type, liters_sold, txn_time)
        VALUES (?, ?, ?, ?)
        """,
        (station_id, fuel_type, liters_sold, resolved_time),
    )


def add_fuel(station_id: int, fuel_type: str, liters: float) -> None:
    if liters <= 0:
        raise ValueError("Refill liters must be greater than 0.")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE fuel_inventory
            SET available_liters = available_liters + ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE station_id = ? AND fuel_type = ?
            """,
            (liters, station_id, fuel_type),
        )
        if cur.rowcount == 0:
            raise ValueError(f"{fuel_type} is not enabled for this station.")
        _log_transaction(conn, station_id, fuel_type, liters_sold=-liters)


def subtract_fuel(station_id: int, fuel_type: str, liters: float) -> None:
    if liters <= 0:
        raise ValueError("Sale liters must be greater than 0.")
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT available_liters
            FROM fuel_inventory
            WHERE station_id = ? AND fuel_type = ?
            """,
            (station_id, fuel_type),
        ).fetchone()
        if row is None:
            raise ValueError("Inventory row not found for station/fuel type.")
        if row["available_liters"] < liters:
            raise ValueError("Insufficient stock for this sale.")
        conn.execute(
            """
            UPDATE fuel_inventory
            SET available_liters = available_liters - ?,
                last_updated = CURRENT_TIMESTAMP
            WHERE station_id = ? AND fuel_type = ?
            """,
            (liters, station_id, fuel_type),
        )
        _log_transaction(conn, station_id, fuel_type, liters_sold=liters)


def update_price(station_id: int, fuel_type: str, new_price: float) -> None:
    if new_price <= 0:
        raise ValueError("Price must be greater than 0.")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE fuel_inventory
            SET price = ?, last_updated = CURRENT_TIMESTAMP
            WHERE station_id = ? AND fuel_type = ?
            """,
            (new_price, station_id, fuel_type),
        )
        if cur.rowcount == 0:
            raise ValueError(f"{fuel_type} is not enabled for this station.")
        _log_transaction(conn, station_id, fuel_type, liters_sold=0.0)


def enable_fuel_type(station_id: int, fuel_type: str, initial_liters: float, price: float) -> None:
    if fuel_type not in {"Petrol", "Diesel"}:
        raise ValueError("Only Petrol/Diesel fuel types are supported.")
    if initial_liters < 0:
        raise ValueError("Initial liters cannot be negative.")
    if price <= 0:
        raise ValueError("Price must be greater than 0.")

    with get_connection() as conn:
        exists = conn.execute(
            """
            SELECT 1
            FROM fuel_inventory
            WHERE station_id = ? AND fuel_type = ?
            """,
            (station_id, fuel_type),
        ).fetchone()
        if exists:
            raise ValueError(f"{fuel_type} already enabled for this station.")

        conn.execute(
            """
            INSERT INTO fuel_inventory (station_id, fuel_type, available_liters, price, last_updated)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (station_id, fuel_type, initial_liters, price),
        )
        _log_transaction(conn, station_id, fuel_type, liters_sold=0.0)


def disable_fuel_type(station_id: int, fuel_type: str) -> None:
    if fuel_type not in {"Petrol", "Diesel"}:
        raise ValueError("Only Petrol/Diesel fuel types are supported.")

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT available_liters
            FROM fuel_inventory
            WHERE station_id = ? AND fuel_type = ?
            """,
            (station_id, fuel_type),
        ).fetchone()
        if row is None:
            raise ValueError(f"{fuel_type} is already not enabled.")
        if float(row["available_liters"]) > 0:
            raise ValueError(f"Cannot remove {fuel_type} while stock is above 0 liters.")

        conn.execute(
            """
            DELETE FROM fuel_inventory
            WHERE station_id = ? AND fuel_type = ?
            """,
            (station_id, fuel_type),
        )
