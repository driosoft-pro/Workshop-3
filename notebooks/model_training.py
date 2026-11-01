# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.6
# ---

# %% [markdown]
# # Workshop 3 — Feature Engineering & Model Training
# **Part A · Steps 3-4 — predict `happiness_score`**
#
# Focus of the workshop: an *integrated, reproducible pipeline* — not model
# optimization. Therefore: **6 features, a simple regressor, everything
# (imputation + scaling) serialized inside one scikit-learn `Pipeline`.**
#
# The reusable implementation lives in `src/train.py`; this notebook drives
# it (same code path as the Airflow DAG).

# %%
import sys
from pathlib import Path

REPO = Path.cwd() if (Path.cwd() / "src").exists() else Path.cwd().parent
sys.path.insert(0, str(REPO))

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src import config
from src.contract import FEATURES, TARGET_FIELD
from src.train import (
    TARGET,
    build_pipeline,
    evaluate_splits,
    export_stream_step,
    split_indices,
    train_step,
)

pd.set_option("display.width", 140)

df = pd.read_parquet(config.UNIFIED_PARQUET)
print("dataset:", df.shape)
df.head()

# %% [markdown]
# ## 1. Feature selection (and what is deliberately excluded)
#
# | feature | correlation with target | why |
# |---|---|---|
# | `gdp` | 0.79 | strongest statistical relationship |
# | `health` | 0.74 | life expectancy |
# | `family` | 0.65 | social support |
# | `freedom` | 0.55 | decision autonomy |
# | `corruption` | 0.40 | trust in institutions |
# | `generosity` | 0.14 | weak, but **required by the event contract** |
#
# **Excluded — target leakage:** `Dystopia Residual`, `Happiness Rank`,
# `Standard Error`, `Whisker.*`, confidence intervals. The published score is
# the *sum* of the six components + Dystopia Residual: using any of them makes
# R² ≈ 1 for the wrong reason.
#
# **Excluded — unavailable in streaming / high cardinality:**
# `region` (not in the event contract), `country` (≈164 values → memorizes),
# `year` (plain trend, no signal for this exercise).
#
# `actual_happiness_score` travels **inside the event** but is used only to
# *score* predictions — never as a model input.

# %%
corr = df[FEATURES + [TARGET]].corr()[TARGET].drop(TARGET).sort_values(ascending=False)
print("feature correlation with the target:")
print(corr.round(2).to_string())

# %% [markdown]
# ## 2. Train/test split — 70/30 **by country**
#
# Each country appears up to 5 times (one per year). A random split puts the
# same country in train *and* test → the error estimate is biased.
# `GroupShuffleSplit(test_size=0.3, random_state=42, groups=country)` keeps
# every country on exactly one side.

# %%
train_idx, test_idx = split_indices(df)
train_countries = set(df.iloc[train_idx]["country"])
test_countries = set(df.iloc[test_idx]["country"])

print(f"train: {len(train_idx)} rows / {len(train_countries)} countries")
print(f"test : {len(test_idx)} rows / {len(test_countries)} countries")
print(f"countries in BOTH splits: {len(train_countries & test_countries)} (must be 0)")

# %% [markdown]
# ## 3. Pipeline: imputer → scaler → regressor
#
# All learned preprocessing travels **inside** `model.pkl` (no train/serving
# skew). `ColumnTransformer` selects columns *by name*, but the consumer
# still builds the DataFrame in the exact `FEATURES` order.

# %%
pipe = build_pipeline()
pipe

# %%
results, fitted_pipe, (train_idx, test_idx) = evaluate_splits(df)

comparison = pd.DataFrame(results).T
comparison.index.name = "model / split"
print("hold-out evaluation (test = 30%):")
print(comparison.to_string())

fig, ax = plt.subplots(figsize=(8, 4))
comparison[["mae", "rmse"]].plot.bar(ax=ax)
ax.set_title("MAE / RMSE by model & split")
ax.set_ylabel("error")
plt.xticks(rotation=20, ha="right")
plt.tight_layout()
plt.show()

# %% [markdown]
# **Reading the table**
#
# * `linear_regression_group_split` is the **selected** configuration:
#   simplest model + methodologically correct split.
# * R² ≈ 0.78 → no leakage (≈0.99 would mean leakage) and no column
#   misalignment (< 0.5 would mean it).
# * RandomForest does not beat linear regression meaningfully → keep the
#   simpler model (workshop rule: integration over optimization).

# %% [markdown]
# ## 4. Train the final model & persist the artifacts

# %%
metadata = train_step()
keys = ["model_version", "algorithm", "mae", "rmse", "r2",
        "n_rows", "n_train", "n_test", "trained_at", "sklearn_version"]
print(pd.Series({k: metadata[k] for k in keys}, dtype=object).to_string())

print("\nmetadata file:")
print(config.MODEL_METADATA_PATH.read_text()[:600])

# %% [markdown]
# ## 5. Evaluation plots (hold-out test set)

# %%
X_test, y_test = df.iloc[test_idx][FEATURES], df.iloc[test_idx][TARGET]
preds = fitted_pipe.predict(X_test)
errors = preds - y_test.to_numpy()

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

axes[0].scatter(y_test, preds, alpha=0.75, color="steelblue")
lims = [min(y_test.min(), preds.min()) - 0.2, max(y_test.max(), preds.max()) + 0.2]
axes[0].plot(lims, lims, "r--", lw=1, label="y = x")
axes[0].set_xlabel("actual happiness score")
axes[0].set_ylabel("predicted score")
axes[0].set_title("Predicted vs actual (test set)")
axes[0].legend()

axes[1].hist(errors, bins=20, color="seagreen", edgecolor="white")
axes[1].axvline(0, color="red", lw=1)
axes[1].set_title(f"Residuals  (bias = {errors.mean():+.3f})")
axes[1].set_xlabel("predicted - actual")

plt.tight_layout()
plt.show()

print(f"MAE  = {abs(errors).mean():.4f}")
print(f"RMSE = {(errors ** 2).mean() ** 0.5:.4f}")
print(f"R2   = {1 - (errors ** 2).sum() / ((y_test - y_test.mean()) ** 2).sum():.4f}")

# %% [markdown]
# ## 6. Streaming inputs (generated from the **test set**)
#
# Streaming the whole dataset would predict on rows the model already saw →
# an optimistic error KPI. Only held-out rows are streamed.

# %%
export_stream_step()

stream = pd.read_csv(config.STREAM_EVENTS_CSV)
invalid = pd.read_csv(config.INVALID_EVENTS_CSV)
print(f"stream_events.csv   : {stream.shape[0]} events, columns={list(stream.columns)}")
print(f"invalid_events.csv  : {invalid.shape[0]} deliberately broken events")
print(invalid[["_defect", "_expected_status"]].to_string(index=False))

# %% [markdown]
# ## 7. Clean-session sanity check — load `model.pkl` and predict

# %%
fresh = joblib.load(config.MODEL_PATH)
sample = pd.DataFrame(
    [[1.2, 0.8, 0.9, 0.6, 0.3, 0.1]], columns=FEATURES
)
print("sample event  :", sample.to_dict("records")[0])
print("prediction    :", round(float(fresh.predict(sample)[0]), 3),
      f"(actual in the event: 6.2 -> {TARGET_FIELD} is evaluation-only)")
