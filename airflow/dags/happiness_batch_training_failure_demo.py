"""Airflow DAG — Controlled Failure Demo for Workshop 3.

Demonstrates Airflow's failure handling, error isolation, and task dependency safety:

    harmonize -> validate_ge (CONTROLLED FAILURE) -x-> train (UPSTREAM_FAILED)
                                                  -x-> export_stream (UPSTREAM_FAILED)

In this DAG, a deliberate data-quality defect is introduced during the validation
step. Great Expectations detects the anomaly and raises an exception. Airflow halts
the pipeline immediately, ensuring that downstream model training and streaming
exports never execute with corrupted data.

Run with:
    airflow dags test happiness_batch_failure_demo
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Resolve repo root inside or outside Docker/Podman
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from airflow.decorators import dag, task  # noqa: E402

from src import config  # noqa: E402


@dag(
    dag_id="happiness_batch_failure_demo",
    description="Batch ETL with Controlled Failure Demo: Quality check failure halts pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "demo", "failure-test", "workshop3"],
)
def happiness_batch_failure_demo():
    @task
    def harmonize() -> str:
        """Step 1 (Success): Build the unified dataset."""
        from src.harmonize import build_unified

        df = build_unified(write=False)
        print(f"[DEMO] Harmonize finished: {df.shape[0]} rows ready for validation.")
        return str(config.UNIFIED_PARQUET)

    @task
    def validate_ge(path: str) -> str:
        """Step 2 (Controlled Failure): Injects a synthetic quality defect.

        Simulates incoming batch data violating business constraints (e.g., negative
        GDP or corrupted happiness score scale), raising a Contract/Expectation failure.
        """
        import pandas as pd

        print("[DEMO] Reading dataset for validation...")
        df = pd.read_parquet(path)

        # Deliberately inject anomaly to trigger controlled failure
        print("[DEMO ALERT] Injecting controlled data quality defect for demonstration:")
        print("  -> Corrupting happiness_score: inserting out-of-range value (score = 99.0)")
        df.loc[0, "happiness_score"] = 99.0

        # Run validation
        from src.validate import run_validation

        summary = run_validation(df)
        if summary.get("n_failed", 0) > 0:
            failed_count = summary["n_failed"]
            raise ValueError(
                f"[CONTROLLED FAILURE] Great Expectations validation failed with "
                f"{failed_count} failing expectations! Halting pipeline to prevent "
                f"training a model on corrupt data."
            )
        return path

    @task
    def train(path: str) -> dict:
        """Step 3 (Skipped/Upstream Failed): Train model (never reached)."""
        print("[CRITICAL] This task should NEVER run if validate_ge failed!")
        from src.train import train_step

        return train_step()

    @task
    def export_stream(metadata: dict) -> None:
        """Step 4 (Skipped/Upstream Failed): Export streaming test events (never reached)."""
        print("[CRITICAL] This task should NEVER run if upstream failed!")
        from src.train import export_stream_step

        export_stream_step()

    # Define dependency chain
    export_stream(train(validate_ge(harmonize())))


happiness_batch_failure_demo()
