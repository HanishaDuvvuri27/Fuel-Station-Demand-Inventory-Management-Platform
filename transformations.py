from __future__ import annotations

import argparse
import logging

from database import get_connection, initialize_database
from pipeline_audit import finish_pipeline_run, start_pipeline_run

LOGGER = logging.getLogger("fuel_pipeline.batch")


def run_batch_refresh(days: int = 30) -> dict[str, int | str]:
    initialize_database()
    run_id = start_pipeline_run("batch_aggregation_refresh", f"last_{int(days)}_days")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_demand_summary
                    (summary_date, station_id, fuel_type, liters_sold, transaction_count, refreshed_at)
                SELECT
                    DATE(txn_time) AS summary_date,
                    station_id,
                    fuel_type,
                    ROUND(SUM(CASE WHEN liters_sold > 0 THEN liters_sold ELSE 0 END), 2) AS liters_sold,
                    SUM(CASE WHEN liters_sold > 0 THEN 1 ELSE 0 END) AS transaction_count,
                    CURRENT_TIMESTAMP
                FROM transactions
                WHERE DATE(txn_time) >= DATE('now', ?)
                GROUP BY DATE(txn_time), station_id, fuel_type
                """,
                (f"-{int(days)} day",),
            )
            demand_rows = int(conn.execute("SELECT changes()").fetchone()[0])

            conn.execute(
                """
                INSERT INTO inventory_snapshots
                    (snapshot_date, station_id, fuel_type, available_liters, price, captured_at)
                SELECT DATE('now'), station_id, fuel_type, available_liters, price, CURRENT_TIMESTAMP
                FROM fuel_inventory
                """
            )
            snapshot_rows = int(conn.execute("SELECT changes()").fetchone()[0])

            conn.execute(
                """
                INSERT OR REPLACE INTO revenue_rollups
                    (rollup_date, station_id, fuel_type, estimated_revenue, liters_sold, refreshed_at)
                SELECT
                    DATE(t.txn_time) AS rollup_date,
                    t.station_id,
                    t.fuel_type,
                    ROUND(SUM(CASE WHEN t.liters_sold > 0 THEN t.liters_sold * fi.price ELSE 0 END), 2) AS estimated_revenue,
                    ROUND(SUM(CASE WHEN t.liters_sold > 0 THEN t.liters_sold ELSE 0 END), 2) AS liters_sold,
                    CURRENT_TIMESTAMP
                FROM transactions t
                JOIN fuel_inventory fi ON fi.station_id = t.station_id AND fi.fuel_type = t.fuel_type
                WHERE DATE(t.txn_time) >= DATE('now', ?)
                GROUP BY DATE(t.txn_time), t.station_id, t.fuel_type
                """,
                (f"-{int(days)} day",),
            )
            revenue_rows = int(conn.execute("SELECT changes()").fetchone()[0])

        total_rows = demand_rows + snapshot_rows + revenue_rows
        finish_pipeline_run(run_id, "success", total_rows, 0, "batch refresh completed")
        LOGGER.info("Batch refresh complete: %s rows refreshed", total_rows)
        return {"run_id": run_id, "rows_refreshed": total_rows, "status": "success"}
    except Exception as exc:
        finish_pipeline_run(run_id, "failed", 0, 0, str(exc))
        LOGGER.exception("Batch refresh failed")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh daily demand, inventory, and revenue aggregate tables.")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run_batch_refresh(days=args.days))


if __name__ == "__main__":
    main()
