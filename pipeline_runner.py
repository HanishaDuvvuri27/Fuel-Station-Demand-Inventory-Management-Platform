from __future__ import annotations

import argparse
import logging
import time

from ingestion import ingest_transactions
from quality_checks import run_quality_checks
from s3_export import upload_report_to_s3
from transformations import run_batch_refresh

LOGGER = logging.getLogger("fuel_pipeline.runner")


def run_pipeline_once(source_path: str | None = None, days: int = 30, s3_bucket: str | None = None) -> None:
    if source_path:
        LOGGER.info("Starting ingestion for %s", source_path)
        ingest_transactions(source_path)
    LOGGER.info("Refreshing aggregate tables")
    run_batch_refresh(days=days)
    LOGGER.info("Running data quality checks")
    run_quality_checks()
    if s3_bucket:
        for report in ["daily_demand_summary", "inventory_snapshots", "revenue_rollups", "data_quality_reports"]:
            upload_report_to_s3(report, s3_bucket)


def main() -> None:
    parser = argparse.ArgumentParser(description="Orchestrate ingestion, batch refresh, quality checks, and report export.")
    parser.add_argument("--source-path", default="", help="Optional raw CSV/JSON transaction file to ingest first.")
    parser.add_argument("--days", type=int, default=30, help="Aggregation lookback window.")
    parser.add_argument("--s3-bucket", default="", help="Optional S3 bucket for processed report exports.")
    parser.add_argument("--every-minutes", type=float, default=0, help="Run repeatedly to simulate a scheduled batch pipeline.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    while True:
        run_pipeline_once(args.source_path or None, args.days, args.s3_bucket or None)
        if args.every_minutes <= 0:
            break
        LOGGER.info("Sleeping %.2f minutes before next scheduled refresh", args.every_minutes)
        time.sleep(args.every_minutes * 60)


if __name__ == "__main__":
    main()
