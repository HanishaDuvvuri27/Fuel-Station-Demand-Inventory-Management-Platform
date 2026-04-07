import argparse
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

import pandas as pd

from database import (
    bulk_insert_inventory,
    bulk_insert_owner_station_access,
    bulk_insert_stations,
    bulk_insert_transactions,
    bulk_insert_users,
    get_connection,
    get_station_count,
    initialize_database,
)

random.seed(42)


CITY_CENTER = (17.3850, 78.4867)  # Hyderabad
AREAS = {
    "Banjara Hills": (17.4126, 78.4347),
    "Gachibowli": (17.4401, 78.3489),
    "Madhapur": (17.4483, 78.3915),
    "Kukatpally": (17.4948, 78.3996),
    "Ameerpet": (17.4375, 78.4482),
    "Begumpet": (17.4441, 78.4660),
    "Jubilee Hills": (17.4320, 78.4071),
    "Hitech City": (17.4435, 78.3772),
}

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
BRANDS = [
    "IndianOil",
    "Bharat Petroleum",
    "HP Petrol Pump",
    "Reliance",
    "Shell",
    "Nayara",
]
NAME_SUFFIX = [
    "Service Station",
    "Fuel Centre",
    "Energy Point",
    "Petrol Bunk",
    "Fuel Hub",
]


def _random_geo_around(lat: float, lon: float, spread: float = 0.01) -> tuple[float, float]:
    return (
        round(lat + random.uniform(-spread, spread), 6),
        round(lon + random.uniform(-spread, spread), 6),
    )


def _assign_area_by_nearest_center(lat: float, lon: float) -> str:
    nearest_area = None
    nearest_distance = float("inf")
    for area, (a_lat, a_lon) in AREAS.items():
        distance = ((lat - a_lat) ** 2 + (lon - a_lon) ** 2) ** 0.5
        if distance < nearest_distance:
            nearest_distance = distance
            nearest_area = area
    return nearest_area or "Unknown"


def fetch_osm_fuel_stations(
    city_name: str = "Hyderabad",
    country_name: str = "India",
    max_stations: int = 300,
    timeout_sec: int = 60,
) -> pd.DataFrame:
    # Overpass query: fetch fuel amenities from city administrative boundary.
    query = f"""
    [out:json][timeout:60];
    area["name"="{city_name}"]["boundary"="administrative"]->.city;
    (
      node["amenity"="fuel"](area.city);
      way["amenity"="fuel"](area.city);
      relation["amenity"="fuel"](area.city);
    );
    out center tags;
    """

    response = requests.post(
        OVERPASS_URL,
        data={"data": query},
        timeout=timeout_sec,
        headers={"User-Agent": "fuel-demand-inventory-system/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    elements = payload.get("elements", [])

    rows: list[dict] = []
    seen = set()
    unnamed_counter = 1

    for elem in elements:
        tags = elem.get("tags", {})
        lat = elem.get("lat")
        lon = elem.get("lon")

        if lat is None or lon is None:
            center = elem.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        if lat is None or lon is None:
            continue

        key = (round(float(lat), 6), round(float(lon), 6))
        if key in seen:
            continue
        seen.add(key)

        name = tags.get("name")
        if not name:
            name = f"OSM Fuel Station {unnamed_counter:03d}"
            unnamed_counter += 1

        area = _assign_area_by_nearest_center(float(lat), float(lon))
        rows.append(
            {
                "name": str(name)[:150],
                "area": area,
                "latitude": round(float(lat), 6),
                "longitude": round(float(lon), 6),
            }
        )

        if len(rows) >= max_stations:
            break

    return pd.DataFrame(rows)


def generate_stations(num_stations: int = 40) -> pd.DataFrame:
    rows: list[dict] = []
    area_names = list(AREAS.keys())

    for i in range(1, num_stations + 1):
        area = random.choice(area_names)
        base_lat, base_lon = AREAS[area]
        lat, lon = _random_geo_around(base_lat, base_lon)
        brand = BRANDS[(i - 1) % len(BRANDS)]
        suffix = NAME_SUFFIX[(i - 1) % len(NAME_SUFFIX)]
        rows.append(
            {
                "name": f"{brand} {area} {suffix}",
                "area": area,
                "latitude": lat,
                "longitude": lon,
            }
        )
    return pd.DataFrame(rows)


def upgrade_existing_station_names_if_synthetic() -> None:
    # One-time in-place cleanup for old synthetic names (Fuel Station XX).
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT station_id, area
            FROM stations
            WHERE name LIKE 'Fuel Station %'
            ORDER BY station_id
            """
        ).fetchall()
        for idx, row in enumerate(rows, start=1):
            area = row["area"] if row["area"] else "City"
            brand = BRANDS[(idx - 1) % len(BRANDS)]
            suffix = NAME_SUFFIX[(idx - 1) % len(NAME_SUFFIX)]
            conn.execute(
                "UPDATE stations SET name = ? WHERE station_id = ?",
                (f"{brand} {area} {suffix}", int(row["station_id"])),
            )


def generate_inventory(num_stations: int, now: datetime) -> pd.DataFrame:
    rows: list[dict] = []
    for station_id in range(1, num_stations + 1):
        for fuel_type in ("Petrol", "Diesel"):
            price = round(
                random.uniform(99.0, 111.0) if fuel_type == "Petrol" else random.uniform(88.0, 100.0),
                2,
            )
            if random.random() < 0.15:
                available = round(random.uniform(50.0, 180.0), 2)
            else:
                available = round(random.uniform(220.0, 5000.0), 2)
            updated_at = now - timedelta(minutes=random.randint(5, 48 * 60))

            rows.append(
                {
                    "station_id": station_id,
                    "fuel_type": fuel_type,
                    "available_liters": available,
                    "price": price,
                    "last_updated": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
    return pd.DataFrame(rows)


def generate_transactions(num_stations: int, now: datetime, rows_count: int = 4000) -> pd.DataFrame:
    rows: list[dict] = []
    for _ in range(rows_count):
        station_id = random.randint(1, num_stations)
        fuel_type = random.choice(["Petrol", "Diesel"])
        liters_sold = round(random.uniform(2.0, 70.0), 2)
        txn_time = now - timedelta(minutes=random.randint(0, 30 * 24 * 60))
        rows.append(
            {
                "station_id": station_id,
                "fuel_type": fuel_type,
                "liters_sold": liters_sold,
                "txn_time": txn_time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return pd.DataFrame(rows)


def generate_users() -> pd.DataFrame:
    rows = [
        {
            "username": "admin",
            "password": "admin123",
            "role": "admin",
            "home_latitude": CITY_CENTER[0],
            "home_longitude": CITY_CENTER[1],
        }
    ]

    for i in range(1, 6):
        rows.append(
            {
                "username": f"owner{i}",
                "password": f"owner{i}123",
                "role": "station_owner",
                "home_latitude": CITY_CENTER[0] + random.uniform(-0.02, 0.02),
                "home_longitude": CITY_CENTER[1] + random.uniform(-0.02, 0.02),
            }
        )

    for i in range(1, 11):
        rows.append(
            {
                "username": f"user{i}",
                "password": f"user{i}123",
                "role": "fuel_user",
                "home_latitude": CITY_CENTER[0] + random.uniform(-0.03, 0.03),
                "home_longitude": CITY_CENTER[1] + random.uniform(-0.03, 0.03),
            }
        )
    return pd.DataFrame(rows)


def generate_owner_station_access(station_count: int) -> pd.DataFrame:
    # Users inserted in generate_users order:
    # 1=admin, 2..6=owner1..owner5
    owners = [2, 3, 4, 5, 6]
    rows: list[dict] = []
    for station_id in range(1, station_count + 1):
        owner_id = owners[(station_id - 1) % len(owners)]
        rows.append({"user_id": owner_id, "station_id": station_id})
    return pd.DataFrame(rows)


def seed_database(
    force_reset: bool = False,
    use_real_data: bool = False,
    max_real_stations: int = 250,
    transaction_rows: int = 5000,
) -> None:
    if (not force_reset) and get_station_count() > 0:
        upgrade_existing_station_names_if_synthetic()
        return

    initialize_database(reset=force_reset)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    df_stations: Optional[pd.DataFrame] = None
    if use_real_data:
        try:
            df_stations = fetch_osm_fuel_stations(
                city_name="Hyderabad",
                country_name="India",
                max_stations=max_real_stations,
            )
            # If scraping succeeds but returns too few rows, we still top up synthetically.
            if df_stations.empty:
                df_stations = None
        except Exception as exc:
            print(f"[WARN] Real data scrape failed. Falling back to synthetic data. Details: {exc}")
            df_stations = None

    if df_stations is None:
        df_stations = generate_stations(num_stations=40)

    # Ensure enough rows for robust analytics if real scrape returns smaller set.
    if len(df_stations) < 40:
        top_up = generate_stations(num_stations=(40 - len(df_stations)))
        df_stations = pd.concat([df_stations, top_up], ignore_index=True)

    bulk_insert_stations(df_stations)

    station_count = len(df_stations)
    df_inventory = generate_inventory(num_stations=station_count, now=now)
    bulk_insert_inventory(df_inventory)

    df_txn = generate_transactions(num_stations=station_count, now=now, rows_count=transaction_rows)
    bulk_insert_transactions(df_txn)

    df_users = generate_users()
    bulk_insert_users(df_users)

    df_access = generate_owner_station_access(station_count=station_count)
    bulk_insert_owner_station_access(df_access)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed fuel management database.")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate tables before seeding.")
    parser.add_argument(
        "--real",
        action="store_true",
        help="Use OpenStreetMap scrape (Overpass API) for real station locations.",
    )
    parser.add_argument(
        "--max-real-stations",
        type=int,
        default=250,
        help="Maximum stations to ingest from OSM when --real is used.",
    )
    parser.add_argument(
        "--txn-rows",
        type=int,
        default=5000,
        help="Number of transactions to generate.",
    )
    args = parser.parse_args()

    seed_database(
        force_reset=args.reset,
        use_real_data=args.real,
        max_real_stations=args.max_real_stations,
        transaction_rows=args.txn_rows,
    )
    print("Database seeded successfully.")
