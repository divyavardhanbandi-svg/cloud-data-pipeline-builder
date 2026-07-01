"""
monitor.py
-----------
Monitoring and logging utilities for pipeline runs. Persists
structured run logs (per-stage status, duration, row counts,
errors) to disk, simulating the observability layer of a cloud
orchestration platform (e.g., Airflow task logs / CloudWatch).
"""

import json
import os
from datetime import datetime
from typing import List

from pipeline_core import StageResult


def save_run_log(pipeline_name: str, run_log: List[StageResult], log_dir: str) -> str:
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(log_dir, f"{pipeline_name}_run_{timestamp}.json")

    payload = {
        "pipeline": pipeline_name,
        "run_timestamp": timestamp,
        "stages": [
            {
                "stage_name": r.stage_name,
                "status": r.status,
                "started_at": r.started_at,
                "finished_at": r.finished_at,
                "duration_seconds": r.duration_seconds,
                "rows_processed": r.rows_processed,
                "error": r.error,
            }
            for r in run_log
        ],
    }

    with open(path, "w") as f:
        json.dump(payload, f, indent=2)

    return path


def print_run_report(pipeline_name: str, run_log: List[StageResult]):
    print(f"\n--- Run Report: {pipeline_name} ---")
    for r in run_log:
        status_marker = {"success": "OK", "failed": "FAIL", "skipped": "SKIP"}.get(r.status, "?")
        rows = f", rows={r.rows_processed}" if r.rows_processed is not None else ""
        print(f"  [{status_marker:4s}] {r.stage_name:35s} "
              f"{r.duration_seconds:6.3f}s{rows}")
        if r.error:
            print(f"           error: {r.error}")
