from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from database import get_connection
from pipeline_audit import finish_pipeline_run, start_pipeline_run

LOGGER = logging.getLogger("fuel_pipeline.s3_export")


REPORT_QUERIES = {
    "daily_demand_summary": "SELECT * FROM daily_demand_summary ORDER BY summary_date DESC, station_id, fuel_type",
    "inventory_snapshots": "SELECT * FROM inventory_snapshots ORDER BY captured_at DESC, station_id, fuel_type",
    "revenue_rollups": "SELECT * FROM revenue_rollups ORDER BY rollup_date DESC, station_id, fuel_type",
    "data_quality_reports": "SELECT * FROM data_quality_reports ORDER BY created_at DESC",
    "pipeline_runs": "SELECT * FROM pipeline_runs ORDER BY started_at DESC",
}


def export_report_to_csv(report_name: str, output_dir: str = "exports") -> Path:
    if report_name not in REPORT_QUERIES:
        raise ValueError(f"Unknown report '{report_name}'. Choose one of: {', '.join(REPORT_QUERIES)}")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    csv_path = output_path / f"{report_name}.csv"
    with get_connection() as conn:
        df = pd.read_sql_query(REPORT_QUERIES[report_name], conn)
    df.to_csv(csv_path, index=False)
    return csv_path


def upload_report_to_s3(report_name: str, bucket: str, key_prefix: str = "fuel-reports", output_dir: str = "exports") -> dict[str, str | int]:
    run_id = start_pipeline_run("s3_report_export", f"s3://{bucket}/{key_prefix}/{report_name}.csv")
    try:
        import boto3

        csv_path = export_report_to_csv(report_name, output_dir)
        key = f"{key_prefix.rstrip('/')}/{csv_path.name}"
        boto3.client("s3").upload_file(str(csv_path), bucket, key)
        finish_pipeline_run(run_id, "success", 1, 0, f"uploaded {csv_path} to s3://{bucket}/{key}")
        LOGGER.info("Uploaded %s to s3://%s/%s", csv_path, bucket, key)
        return {"run_id": run_id, "bucket": bucket, "key": key, "status": "success"}
    except Exception as exc:
        finish_pipeline_run(run_id, "failed", 0, 0, str(exc))
        LOGGER.exception("S3 export failed")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Export processed reports to CSV and optionally upload to S3.")
    parser.add_argument("report_name", choices=sorted(REPORT_QUERIES))
    parser.add_argument("--bucket", default="")
    parser.add_argument("--key-prefix", default="fuel-reports")
    parser.add_argument("--output-dir", default="exports")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.bucket:
        print(upload_report_to_s3(args.report_name, args.bucket, args.key_prefix, args.output_dir))
    else:
        print(export_report_to_csv(args.report_name, args.output_dir))


if __name__ == "__main__":
    main()
