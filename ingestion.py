from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from database import get_connection, initialize_database
from pipeline_audit import finish_pipeline_run, log_rejected_record, start_pipeline_run

LOGGER = logging.getLogger("fuel_pipeline.ingestion")
REQUIRED_COLUMNS = {"station_id", "fuel_type", "liters_sold", "txn_time"}
FUEL_TYPES = {"Petrol", "Diesel"}


def _read_raw_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".json", ".jsonl"}:
        return pd.read_json(path, lines=path.suffix.lower() == ".jsonl")
    raise ValueError(f"Unsupported source format: {path.suffix}. Use CSV, JSON, or JSONL.")


def _validate_and_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    work = df.copy()
    work = work[list(REQUIRED_COLUMNS)].copy()
    work["station_id"] = pd.to_numeric(work["station_id"], errors="coerce")
    work["liters_sold"] = pd.to_numeric(work["liters_sold"], errors="coerce")
    work["txn_time"] = pd.to_datetime(work["txn_time"], errors="coerce")
    work["fuel_type"] = work["fuel_type"].astype("string").str.strip()

    invalid_mask = (
        work["station_id"].isna()
        | work["fuel_type"].isna()
        | ~work["fuel_type"].isin(FUEL_TYPES)
        | work["liters_sold"].isna()
        | work["txn_time"].isna()
    )
    rejected = work[invalid_mask].copy()
    cleaned = work[~invalid_mask].copy()
    duplicate_mask = cleaned.duplicated(subset=["station_id", "fuel_type", "liters_sold", "txn_time"], keep="first")
    duplicates = cleaned[duplicate_mask].copy()
    cleaned = cleaned[~duplicate_mask].copy()

    rejected = pd.concat([rejected, duplicates], ignore_index=True)
    cleaned["station_id"] = cleaned["station_id"].astype(int)
    cleaned["txn_time"] = cleaned["txn_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return cleaned, rejected


def ingest_transactions(source_path: str, db_path: str | None = None) -> dict[str, int | str]:
    initialize_database(db_path=db_path) if db_path else initialize_database()
    path = Path(source_path)
    run_id = start_pipeline_run("transaction_ingestion", str(path))
    rows_ingested = 0
    rows_rejected = 0
    try:
        raw = _read_raw_file(path)
        cleaned, rejected = _validate_and_clean(raw)
        rows_rejected = int(len(rejected))

        connection_ctx = get_connection(db_path) if db_path else get_connection()
        with connection_ctx as conn:
            station_ids = {
                int(row[0])
                for row in conn.execute("SELECT station_id FROM stations").fetchall()
            }
            missing_station = cleaned[~cleaned["station_id"].isin(station_ids)].copy()
            cleaned = cleaned[cleaned["station_id"].isin(station_ids)].copy()
            rows_rejected += int(len(missing_station))

            if not cleaned.empty:
                conn.executemany(
                    """
                    INSERT INTO transactions (station_id, fuel_type, liters_sold, txn_time)
                    VALUES (?, ?, ?, ?)
                    """,
                    cleaned[["station_id", "fuel_type", "liters_sold", "txn_time"]].itertuples(index=False, name=None),
                )
                rows_ingested = int(len(cleaned))

        for _, row in rejected.iterrows():
            log_rejected_record(run_id, str(path), row.to_dict(), "schema/null/duplicate validation failed")
        for _, row in missing_station.iterrows():
            log_rejected_record(run_id, str(path), row.to_dict(), "station_id not found")

        finish_pipeline_run(run_id, "success", rows_ingested, rows_rejected, "ingestion completed")
        LOGGER.info("Ingested %s rows and rejected %s rows from %s", rows_ingested, rows_rejected, path)
        return {"run_id": run_id, "rows_ingested": rows_ingested, "rows_rejected": rows_rejected, "status": "success"}
    except Exception as exc:
        finish_pipeline_run(run_id, "failed", rows_ingested, rows_rejected, str(exc))
        LOGGER.exception("Ingestion failed for %s", path)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw fuel transactions from CSV/JSON into the warehouse.")
    parser.add_argument("source_path", help="Path to a CSV, JSON, or JSONL file.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest_transactions(args.source_path))


if __name__ == "__main__":
    main()
