from __future__ import annotations

import argparse
import logging

import pandas as pd

from database import get_connection, initialize_database
from pipeline_audit import finish_pipeline_run, log_quality_result, start_pipeline_run

LOGGER = logging.getLogger("fuel_pipeline.quality")


def _count_query(query: str, params: tuple = ()) -> int:
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
    return int(row[0] or 0)


def run_quality_checks(price_floor: float = 50.0, price_ceiling: float = 200.0) -> dict[str, int | str]:
    initialize_database()
    run_id = start_pipeline_run("data_quality_checks")
    try:
        checks = [
            (
                "outlier_prices",
                "warning",
                _count_query(
                    "SELECT COUNT(*) FROM fuel_inventory WHERE price < ? OR price > ?",
                    (float(price_floor), float(price_ceiling)),
                ),
                f"Fuel prices outside {price_floor}-{price_ceiling}.",
            ),
            (
                "negative_inventory",
                "critical",
                _count_query("SELECT COUNT(*) FROM fuel_inventory WHERE available_liters < 0"),
                "Inventory rows with negative available_liters.",
            ),
            (
                "missing_station_ids",
                "critical",
                _count_query(
                    """
                    SELECT COUNT(*)
                    FROM transactions t
                    LEFT JOIN stations s ON s.station_id = t.station_id
                    WHERE t.station_id IS NULL OR s.station_id IS NULL
                    """
                ),
                "Transactions without a valid station dimension row.",
            ),
            (
                "negative_sales",
                "warning",
                _count_query("SELECT COUNT(*) FROM transactions WHERE liters_sold < 0"),
                "Negative transactions usually represent restocks; review for classification.",
            ),
        ]
        anomaly_total = 0
        for check_name, severity, count, details in checks:
            anomaly_total += int(count)
            log_quality_result(check_name, severity, int(count), details, run_id=run_id)

        status = "success"
        message = f"quality checks completed with {anomaly_total} anomalies"
        finish_pipeline_run(run_id, status, len(checks), anomaly_total, message)
        LOGGER.info(message)
        return {"run_id": run_id, "checks_run": len(checks), "anomalies": anomaly_total, "status": status}
    except Exception as exc:
        finish_pipeline_run(run_id, "failed", 0, 0, str(exc))
        LOGGER.exception("Quality checks failed")
        raise


def export_quality_report(path: str = "quality_report.csv") -> str:
    query = """
    SELECT check_name, severity, anomaly_count, details, created_at
    FROM data_quality_reports
    ORDER BY created_at DESC, report_id DESC
    """
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn)
    df.to_csv(path, index=False)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run warehouse data quality checks.")
    parser.add_argument("--price-floor", type=float, default=50.0)
    parser.add_argument("--price-ceiling", type=float, default=200.0)
    parser.add_argument("--export", default="")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run_quality_checks(args.price_floor, args.price_ceiling))
    if args.export:
        print(export_quality_report(args.export))


if __name__ == "__main__":
    main()
