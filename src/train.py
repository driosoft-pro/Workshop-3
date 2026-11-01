"""Train the happiness-score regression pipeline (Part A, Step 3-4).

What it does:

1. Loads the unified dataset produced by :mod:`src.harmonize`.
2. Splits 70/30 with ``GroupShuffleSplit`` so no country leaks between
   train and test (each country appears up to 5 times, once per year).
3. Trains a scikit-learn ``Pipeline`` (median imputer -> standard scaler ->
   regressor). Everything learned at training time travels inside the
   serialized model, so serving needs no extra preprocessing.
4. Reports MAE / RMSE / R2 and saves ``models/model.pkl`` +
   ``models/model_metadata.json``.
5. Exports the streaming inputs: ``data/streaming/stream_events.csv`` (test
   set only, so error KPIs are not optimistic) and
   ``data/streaming/invalid_events.csv`` (deliberately broken events used to
   demonstrate the INVALID_* states).

Usage::

    python -m src.train
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import joblib
import pandas as pd
import sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer

from src import config
from src.contract import FEATURES, TARGET_FIELD, REQUIRED
from src.harmonize import UNIFIED_COLUMNS

TARGET = "happiness_score"  # column name in the unified dataset


def build_pipeline() -> Pipeline:
    """Imputer + scaler + regressor, selected by column *name*."""
    return Pipeline(
        [
            (
                "prep",
                ColumnTransformer(
                    [
                        (
                            "num",
                            Pipeline(
                                [
                                    ("imp", SimpleImputer(strategy="median")),
                                    ("sc", StandardScaler()),
                                ]
                            ),
                            FEATURES,
                        )
                    ],
                    remainder="drop",
                ),
            ),
            ("model", LinearRegression()),
        ]
    )


def metrics(y_true, y_pred) -> dict[str, float]:
    return {
        "mae": round(float(mean_absolute_error(y_true, y_pred)), 4),
        "rmse": round(float(mean_squared_error(y_true, y_pred) ** 0.5), 4),
        "r2": round(float(r2_score(y_true, y_pred)), 4),
    }


def split_indices(df: pd.DataFrame) -> tuple[list[int], list[int]]:
    """Deterministic 70/30 hold-out that keeps every country on ONE side.

    Each country appears up to 5 times (once per year); a naive random split
    would put the same country in train and test and bias the error estimate.
    """
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.3, random_state=42)
    train_idx, test_idx = next(splitter.split(df, groups=df["country"]))
    return list(train_idx), list(test_idx)


def evaluate_splits(
    df: pd.DataFrame,
) -> tuple[dict[str, dict[str, float]], Pipeline, tuple[list[int], list[int]]]:
    """Report several split/model combinations; the chosen one is documented."""
    X, y = df[FEATURES], df[TARGET]
    results: dict[str, dict[str, float]] = {}

    train_idx, test_idx = split_indices(df)
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    pipe = build_pipeline()
    pipe.fit(X_train, y_train)
    results["linear_regression_group_split"] = metrics(
        y_test, pipe.predict(X_test)
    )

    # Reference: naive random split (same country can appear in train+test)
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    rand_pipe = build_pipeline()
    rand_pipe.fit(X_tr, y_tr)
    results["linear_regression_random_split"] = metrics(y_te, rand_pipe.predict(X_te))

    rf = Pipeline(
        [
            (
                "prep",
                ColumnTransformer(
                    [
                        (
                            "num",
                            Pipeline(
                                [
                                    ("imp", SimpleImputer(strategy="median")),
                                    ("sc", StandardScaler()),
                                ]
                            ),
                            FEATURES,
                        )
                    ],
                    remainder="drop",
                ),
            ),
            (
                "model",
                RandomForestRegressor(
                    n_estimators=200, random_state=42, n_jobs=-1
                ),
            ),
        ]
    )
    rf.fit(X_train, y_train)
    results["random_forest_group_split"] = metrics(y_test, rf.predict(X_test))

    return results, pipe, (train_idx, test_idx)


def export_stream_files(df: pd.DataFrame, test_idx) -> None:
    """Write the events the producer will stream (test set = unseen rows)."""
    config.STREAMING_DIR.mkdir(parents=True, exist_ok=True)
    test = df.iloc[test_idx][
        ["country", "year", *FEATURES, TARGET]
    ].reset_index(drop=True)
    test.to_csv(config.STREAM_EVENTS_CSV, index=False)

    now = datetime.now().isoformat(timespec="seconds")
    base = {
        "event_id": "",
        "country": "Colombia",
        "year": 2019,
        "gdp": 1.2,
        "family": 0.8,
        "health": 0.9,
        "freedom": 0.6,
        "generosity": 0.3,
        "corruption": 0.1,
        TARGET_FIELD: 6.2,
        "event_time": now,
    }
    # _defect/_expected_status document what each broken event must trigger.
    # _drop_field removes a field, _null_field sends JSON null, _raw replaces
    # the whole message (malformed JSON).
    rows = [
        dict(base, event_id="E9001", _defect="missing_field", _expected_status="INVALID_SCHEMA", _drop_field="event_id"),
        dict(base, event_id="E9002", _defect="country_null", _expected_status="INVALID_SCHEMA", _null_field="country"),
        dict(base, event_id="E9003", year="twenty nineteen", _defect="year_not_integer", _expected_status="INVALID_SCHEMA"),
        dict(base, event_id="E9004", gdp="not-a-number", _defect="feature_not_numeric", _expected_status="INVALID_SCHEMA"),
        dict(base, event_id="E9005", gdp=-0.5, _defect="negative_gdp", _expected_status="INVALID_VALUES"),
        dict(base, event_id="E9006", **{TARGET_FIELD: 99.0}, _defect="score_out_of_range", _expected_status="INVALID_VALUES"),
        dict(base, event_id="E9007", event_time="30/12/2026 15:30:00", _defect="bad_event_time", _expected_status="INVALID_VALUES"),
        dict(base, event_id="E9008", corruption=None, _defect="nan_feature", _expected_status="INVALID_VALUES"),
        dict(base, event_id="E9009", freedom=None, _defect="null_feature", _expected_status="INVALID_SCHEMA", _null_field="freedom"),
        dict(base, event_id="E9010", _defect="malformed_json", _expected_status="INVALID_SCHEMA",
             _raw='{"event_id": "E9010", "country": "Colombia", "year": 2019, "gdp": 1.2'),
    ]
    invalid = pd.DataFrame(
        rows,
        columns=[
            "_defect", "_expected_status", "_drop_field", "_null_field", "_raw",
            *REQUIRED,
        ],
    )
    invalid.to_csv(config.INVALID_EVENTS_CSV, index=False)

    print(f"wrote {config.STREAM_EVENTS_CSV.relative_to(config.ROOT)} ({len(test)} events)")
    print(f"wrote {config.INVALID_EVENTS_CSV.relative_to(config.ROOT)} ({len(invalid)} broken events)")


def train_step() -> dict:
    """Fit, evaluate, persist ``model.pkl`` + ``model_metadata.json``.

    Used by the CLI (``python -m src.train``) and by the Airflow DAG task
    ``train`` — one single implementation.
    """
    df = pd.read_parquet(config.UNIFIED_PARQUET)
    missing = [c for c in UNIFIED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"unified dataset is missing columns: {missing}")

    results, pipe, (train_idx, test_idx) = evaluate_splits(df)
    chosen = results["linear_regression_group_split"]

    X, y = df[FEATURES], df[TARGET]
    pipe.fit(X.iloc[train_idx], y.iloc[train_idx])  # final model on the train split

    config.MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, config.MODEL_PATH)

    metadata = {
        "model_version": f"happiness-linreg-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}",
        "algorithm": "LinearRegression",
        "pipeline": ["SimpleImputer(strategy=median)", "StandardScaler", "LinearRegression"],
        "features": FEATURES,
        "target": TARGET,
        "split": "GroupShuffleSplit(test_size=0.3, random_state=42, groups=country)",
        **chosen,
        "comparison": results,
        "n_rows": int(len(df)),
        "n_train": int(len(train_idx)),
        "n_test": int(len(test_idx)),
        "train_countries": int(df.iloc[train_idx]["country"].nunique()),
        "test_countries": int(df.iloc[test_idx]["country"].nunique()),
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sklearn_version": sklearn.__version__,
        "dataset": str(config.UNIFIED_PARQUET.relative_to(config.ROOT)),
    }
    config.MODEL_METADATA_PATH.write_text(json.dumps(metadata, indent=2))
    return metadata


def export_stream_step() -> None:
    """Write the producer inputs (test set split is recomputed deterministically)."""
    df = pd.read_parquet(config.UNIFIED_PARQUET)
    _, test_idx = split_indices(df)
    export_stream_files(df, test_idx)


def main() -> int:
    metadata = train_step()

    print(f"model -> {config.MODEL_PATH.relative_to(config.ROOT)}")
    print("metrics (hold-out test set, by-country split):")
    print(f"  MAE  = {metadata['mae']}")
    print(f"  RMSE = {metadata['rmse']}")
    print(f"  R2   = {metadata['r2']}")
    print("comparison:")
    for name, m in metadata["comparison"].items():
        print(f"  {name:38s} MAE={m['mae']:.3f} RMSE={m['rmse']:.3f} R2={m['r2']:.3f}")

    export_stream_step()
    return 0


if __name__ == "__main__":
    sys.exit(main())
