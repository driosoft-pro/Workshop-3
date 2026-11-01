"""Airflow DAG — Part A (batch) for Workshop 3.

Orchestrates ONLY the offline pipeline; the streaming consumer is a
long-running service and deliberately never appears in a DAG:

    harmonize -> validate_ge (failure stops the DAG) -> train -> export_stream

The heavy logic lives in ``src/`` (harmonize, validate, train) — exactly the
code the notebooks use — so there is a single implementation everywhere.
Run with:  airflow dags test happiness_batch_training
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# In the compose container the repo is mounted at /workspace; locally it is
# the parent of airflow/ — both resolve to the same source tree.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from airflow.decorators import dag, task  # noqa: E402

from src import config  # noqa: E402


@dag(
    dag_id="happiness_batch_training",
    description="Batch ETL: harmonize -> validate (Great Expectations) -> train -> export stream",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "ml", "workshop3"],
)
def happiness_batch_training():
    @task
    def harmonize() -> str:
        """Build the unified dataset (data/processed/unified_happiness.parquet)."""
        from src.harmonize import build_unified

        df = build_unified(write=True)
        print(f"harmonized: {df.shape[0]} rows, nulls={int(df.isna().sum().sum())}")
        return str(config.UNIFIED_PARQUET)

    @task
    def validate_ge(path: str) -> str:
        """Run the Great Expectations suite; raises (stops the DAG) on failure."""
        import pandas as pd

        from src.validate import run_validation

        summary = run_validation(pd.read_parquet(path))
        print(f"GE: {summary['n_expectations']} expectations, "
              f"{summary['n_failed']} failed")
        return path

    @task
    def train(path: str) -> dict:
        """Train the pipeline and persist model.pkl + model_metadata.json."""
        from src.train import train_step

        metadata = train_step()
        print(f"model {metadata['model_version']}: "
              f"MAE={metadata['mae']} RMSE={metadata['rmse']} R2={metadata['r2']}")
        return metadata

    @task
    def export_stream(metadata: dict) -> None:
        """Write data/streaming/stream_events.csv + invalid_events.csv."""
        from src.train import export_stream_step

        export_stream_step()

    # Dataflow: paths/metadata between tasks (never DataFrames through XCom)
    export_stream(train(validate_ge(harmonize())))


happiness_batch_training()
