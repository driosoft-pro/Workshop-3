"""Great Expectations suite for the unified batch dataset.

Event-level validation is deliberately cheap and lives in
:mod:`src.contract`; this module validates the *batch* dataset that feeds
training (Fase 2 / Part A Step 1-2) and is invoked by the Airflow DAG — if
it fails, the DAG stops before training.

Usage::

    python -m src.validate
"""

from __future__ import annotations

import sys

import pandas as pd

import great_expectations as gx
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeOfType,
    ExpectColumnValuesToNotBeNull,
    ExpectCompoundColumnsToBeUnique,
    ExpectTableColumnsToMatchOrderedList,
)

from src import config
from src.contract import FEATURES, RANGES, TARGET_FIELD
from src.harmonize import UNIFIED_COLUMNS, build_unified

SUITE_NAME = "unified_happiness_suite"
CHECKPOINT_NAME = "unified_happiness_checkpoint"

# In the event/contract the target is `actual_happiness_score`; in the
# unified dataset the same measure is `happiness_score`.
SCORE_COLUMN = "happiness_score"
COLUMN_OF_FIELD = {
    field: (SCORE_COLUMN if field == TARGET_FIELD else field) for field in RANGES
}

NOT_NULL_COLUMNS = [
    "country",
    "year",
    SCORE_COLUMN,
    *FEATURES,
]
DTYPES = {
    "country": "str",
    "region": "str",
    "year": "int64",
    **{col: "float64" for col in (*FEATURES, SCORE_COLUMN)},
}


def build_suite() -> gx.ExpectationSuite:
    """Define the data-quality expectations of the unified schema."""
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    suite.add_expectation(
        ExpectTableColumnsToMatchOrderedList(column_list=UNIFIED_COLUMNS)
    )
    for column in NOT_NULL_COLUMNS:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=column))
    suite.add_expectation(
        ExpectColumnValuesToBeInSet(column="year", value_set=[2015, 2016, 2017, 2018, 2019])
    )
    for field, (low, high) in RANGES.items():
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(
                column=COLUMN_OF_FIELD[field], min_value=low, max_value=high
            )
        )
    suite.add_expectation(
        ExpectCompoundColumnsToBeUnique(column_list=["country", "year"])
    )
    for column, dtype in DTYPES.items():
        suite.add_expectation(
            ExpectColumnValuesToBeOfType(column=column, type_=dtype)
        )
    return suite


def run_validation(df: pd.DataFrame | None = None, raise_on_failure: bool = True) -> dict:
    """Run the suite against the unified dataset and return a summary.

    Raises ``RuntimeError`` when any expectation fails (the Airflow DAG
    relies on the exception to halt before training).
    """
    if df is None:
        df = build_unified(write=False)

    context = gx.get_context(mode="ephemeral")
    try:  # keep logs readable (no tqdm progress bars)
        context.progress_bars = False
    except Exception:  # pragma: no cover - option may not exist in all versions
        pass
    suite = context.suites.add(build_suite())

    datasource = context.data_sources.add_pandas("unified_pandas")
    asset = datasource.add_dataframe_asset("unified_happiness")
    batch_definition = asset.add_batch_definition_whole_dataframe("whole_dataframe")

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="unified_happiness_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=CHECKPOINT_NAME,
            validation_definitions=[validation_definition],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})

    evaluations = []
    for run_result in result.run_results.values():
        for r in run_result.results:
            exp = r.expectation
            evaluations.append(
                {
                    "expectation": type(exp).__name__,
                    "kwargs": {
                        k: getattr(exp, k)
                        for k in ("column", "column_list", "columns", "value_set")
                        if getattr(exp, k, None) is not None
                    },
                    "success": bool(r.success),
                }
            )

    failed = [e for e in evaluations if not e["success"]]
    summary = {
        "successful": bool(result.success),
        "n_expectations": len(evaluations),
        "n_failed": len(failed),
        "failed": failed,
        "rows_validated": int(len(df)),
    }

    if failed and raise_on_failure:
        details = "; ".join(f"{f['expectation']}{f['kwargs']}" for f in failed[:10])
        raise RuntimeError(
            f"Great Expectations: {len(failed)}/{len(evaluations)} expectations failed: {details}"
        )
    return summary


def main() -> int:
    df = build_unified(write=False)
    summary = run_validation(df, raise_on_failure=False)

    print(f"rows validated: {summary['rows_validated']}")
    print(f"expectations:   {summary['n_expectations']} (failed: {summary['n_failed']})")
    for failure in summary["failed"][:10]:
        print(f"  FAILED {failure['expectation']} {failure['kwargs']}")
    print(f"result: {'PASSED' if summary['successful'] else 'FAILED'}")
    print(f"artifact: {config.UNIFIED_PARQUET.relative_to(config.ROOT)}")
    return 0 if summary["successful"] else 1


if __name__ == "__main__":
    sys.exit(main())
