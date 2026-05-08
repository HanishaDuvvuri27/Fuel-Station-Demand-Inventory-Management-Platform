from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from database import get_connection


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def start_pipeline_run(pipeline_name: str, source_path: str | None = None) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO pipeline_runs (pipeline_name, source_path, started_at, status)
            VALUES (?, ?, ?, 'running')
            """,
            (pipeline_name, source_path, utc_now()),
        )
        return int(cur.lastrowid)


def finish_pipeline_run(
    run_id: int,
    status: str,
    rows_ingested: int = 0,
    rows_rejected: int = 0,
    message: str | None = None,
) -> None:
    if status not in {"success", "failed"}:
        raise ValueError("Pipeline status must be success or failed.")
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE pipeline_runs
            SET completed_at = ?, status = ?, rows_ingested = ?, rows_rejected = ?, message = ?
            WHERE run_id = ?
            """,
            (utc_now(), status, int(rows_ingested), int(rows_rejected), message, int(run_id)),
        )


def log_rejected_record(run_id: int, source_path: str | None, payload: dict[str, Any], reason: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO rejected_records (run_id, source_path, record_payload, rejection_reason)
            VALUES (?, ?, ?, ?)
            """,
            (int(run_id), source_path, json.dumps(payload, default=str), reason),
        )


def log_quality_result(
    check_name: str,
    severity: str,
    anomaly_count: int,
    details: str,
    run_id: int | None = None,
) -> None:
    if severity not in {"info", "warning", "critical"}:
        raise ValueError("Quality severity must be info, warning, or critical.")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO data_quality_reports (run_id, check_name, severity, anomaly_count, details)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, check_name, severity, int(anomaly_count), details),
        )
