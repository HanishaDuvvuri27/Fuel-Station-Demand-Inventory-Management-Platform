from datetime import datetime, timezone

import pandas as pd

from database import get_stations_inventory_view


def recommend_stations(area: str, fuel_type: str = "Petrol", top_n: int = 3) -> pd.DataFrame:
    df = get_stations_inventory_view(area=area, fuel_type=fuel_type)
    if df.empty:
        return df

    df = df.copy()
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df["minutes_since_update"] = (now - df["last_updated"]).dt.total_seconds() / 60.0
    df["minutes_since_update"] = df["minutes_since_update"].fillna(df["minutes_since_update"].median())

    availability_score = (df["available_liters"] - df["available_liters"].min()) / (
        (df["available_liters"].max() - df["available_liters"].min()) or 1
    )
    price_score = 1 - (df["price"] - df["price"].min()) / (((df["price"].max() - df["price"].min()) or 1))
    recency_score = 1 - (df["minutes_since_update"] - df["minutes_since_update"].min()) / (
        (df["minutes_since_update"].max() - df["minutes_since_update"].min()) or 1
    )

    # Weighted score: availability and price dominate; recency adds operational trust.
    df["score"] = (0.45 * availability_score) + (0.4 * price_score) + (0.15 * recency_score)

    cols = ["station_id", "name", "area", "fuel_type", "available_liters", "price", "last_updated", "score"]
    return df.sort_values("score", ascending=False).head(top_n)[cols]
