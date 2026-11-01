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
# # Workshop 3 — EDA, Cleaning & Schema Harmonization
# **Part A · Steps 1-2 — World Happiness 2015-2019**
#
# This notebook profiles the five yearly CSVs, documents the data-quality
# findings, proposes a unified analytical schema and validates it with
# **Great Expectations**.
#
# The reusable implementation lives in `src/harmonize.py` (`build_unified()`);
# this notebook only *drives* it, so the Airflow DAG runs exactly the same
# code path (single implementation).

# %%
import sys
from pathlib import Path

REPO = Path.cwd() if (Path.cwd() / "src").exists() else Path.cwd().parent
sys.path.insert(0, str(REPO))

import pandas as pd

from src import config
from src.harmonize import RENAME, UNIFIED_COLUMNS, build_unified, load_aliases

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 160)

RAW_FILES = sorted(config.RAW_DIR.glob("20*.csv"))
{p.stem: p for p in RAW_FILES}

# %% [markdown]
# ## 1. Raw profiling — one table per year (data-quality observations)
#
# Schema, dtypes, nulls and duplicates are inspected **without modifying**
# the source files (they stay untouched in `data/raw/`).

# %%
raw = {int(p.stem): pd.read_csv(p) for p in RAW_FILES}

rows = []
for year, df in sorted(raw.items()):
    rows.append(
        {
            "year": year,
            "rows": df.shape[0],
            "cols": df.shape[1],
            "null_cells": int(df.isna().sum().sum()),
            "dup_rows": int(df.duplicated().sum()),
            "numeric_cols": int(df.select_dtypes("number").shape[1]),
        }
    )
quality = pd.DataFrame(rows)
quality.loc[quality.index[-1], ""] = None
print(quality.to_string(index=False))
print("TOTAL rows:", int(quality["rows"].sum()))

# %%
nulls_by_year = pd.DataFrame(
    {year: df.isna().sum() for year, df in sorted(raw.items())}
).T
print("nulls per column (only non-zero shown):")
print(nulls_by_year.loc[:, nulls_by_year.sum() > 0].to_string())

# %% [markdown]
# **Finding:** exactly **one** missing cell in the whole dataset —
# `corruption` for *United Arab Emirates* in 2018. Everything else is
# complete, with 0 duplicated rows per file.

# %%
COUNTRY_COL = {2015: "Country", 2016: "Country", 2017: "Country",
               2018: "Country or region", 2019: "Country or region"}
dups = {year: int(df.duplicated(subset=[COUNTRY_COL[year]]).sum())
        for year, df in sorted(raw.items())}
print("duplicate country rows per year:", dups)

# %% [markdown]
# ## 2. Schema differences between years
#
# The same concept changes its column name every year — this is the core
# integration problem of the workshop.

# %%
schema = pd.DataFrame(
    {
        year: {col: "✓" for col in df.columns}
        for year, df in sorted(raw.items())
    }
).fillna("")
schema["unified_name"] = [
    next(
        (RENAME[y].get(col) for y in sorted(RENAME) if RENAME[y].get(col)),
        "",
    )
    if any(RENAME[y].get(col) for y in RENAME)
    else "— dropped —"
    for col in schema.index
]
print(schema.to_string())

# %% [markdown]
# ### Unified schema proposal
#
# | unified column | source |
# |---|---|
# | `country` | `Country` / `Country or region` |
# | `year` | **file name** (no year column exists in the files) |
# | `region` | `Region` (2015-2016 only) → propagated |
# | `gdp` | `Economy (GDP per Capita)` / `GDP per capita` / ... |
# | `family` | `Family` / `Social support` |
# | `health` | `Health (Life Expectancy)` / `Healthy life expectancy` |
# | `freedom` | `Freedom` / `Freedom to make life choices` |
# | `generosity` | `Generosity` |
# | `corruption` | `Trust (Government Corruption)` / `Perceptions of corruption` |
# | `happiness_score` | `Happiness Score` / `Happiness.Score` / `Score` |
#
# **Dropped on purpose (target leakage / derived):** `Happiness Rank`,
# `Standard Error`, confidence intervals, `Whisker.*`, `Dystopia Residual`.

# %% [markdown]
# ## 3. Inconsistent country names
#
# ~170 raw names but fewer than 141 countries appear in all five years: part
# of the difference is *the same country written differently*. A versioned
# alias dictionary is applied **before** the merge.

# %%
raw_names = sorted(
    {
        str(v).strip()
        for year, df in sorted(raw.items())
        for v in df["Country" if year in (2015, 2016, 2017) else "Country or region"]
    }
)
print(f"distinct raw country names: {len(raw_names)}")

aliases = pd.read_csv(config.REFERENCE_DIR / "country_aliases.csv")
print("\ncountry_aliases.csv (variant -> canonical):")
print(aliases.to_string(index=False))

# %%
# Any variant still present after harmonization would break dim_country
# and the "predictions by country" KPI -> verify explicitly.
df = build_unified(write=False)
per_year = df.groupby("country")["year"].nunique()
incomplete = per_year[per_year < 5]
print(f"\ncanonical countries: {df['country'].nunique()}")
print(f"present in all 5 years: {(per_year == 5).sum()}")
print(f"in <5 years: {len(incomplete)} (gaps in the SOURCE, not name variants):")
print(incomplete.to_string())

# %% [markdown]
# ## 4. Harmonization: types, missing values, duplicates
#
# Cleaning decisions (justified in the README):
#
# | problem | decision | justification |
# |---|---|---|
# | 1 null (`corruption`, UAE 2018) | impute with UAE's 2017+2019 mean | dropping the row loses a whole country-year; 5 other indicators are valid |
# | exact zeros (Sierra Leone, ...) | **keep** | published values at the bottom of the scale, not disguised nulls |
# | `generosity` ≈ 0.838 (2017) | **keep** (flagged by IQR) | real value of a real country; goal is the pipeline, not model tuning |
# | schema drift | `RENAME` map per year | one unified schema |
# | region only 2015-16 | propagate, fallback `Unknown` | region is not a feature (not in the event contract) |

# %%
unified = build_unified(write=True)

print("shape:", unified.shape)
print("dtypes:")
print(unified.dtypes.to_string())
print("nulls:", int(unified.isna().sum().sum()))
print("duplicate (country, year):", int(unified.duplicated(["country", "year"]).sum()))
print("\nrows per year:")
print(unified["year"].value_counts().sort_index().to_string())
print("\nhead:")
print(unified.head(10).to_string(index=False))

# %%
# The single imputed value (surface it from the harmonizer for the report)
imputations = getattr(build_unified, "last_imputations", [])
print("imputed values:")
for note in imputations:
    print(" -", note)

# %% [markdown]
# ## 5. Outliers — reported, never deleted
#
# IQR rule per feature: values outside `[Q1 - 1.5·IQR, Q3 + 1.5·IQR]` are
# counted (and plotted), but **kept**: they are genuine published data.

# %%
features = ["gdp", "family", "health", "freedom", "generosity", "corruption", "happiness_score"]
iqr_rows = []
for col in features:
    q1, q3 = unified[col].quantile([0.25, 0.75])
    low, high = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    out = unified[(unified[col] < low) | (unified[col] > high)]
    iqr_rows.append({"column": col, "low": round(low, 3), "high": round(high, 3),
                     "n_outliers": len(out),
                     "examples": ", ".join(out.nlargest(2, col)["country"])})
print(pd.DataFrame(iqr_rows).to_string(index=False))

# %%
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

fig, axes = plt.subplots(2, 4, figsize=(16, 6))
for ax, col in zip(axes.flat, features):
    sns.boxplot(y=unified[col], ax=ax, color="steelblue")
    ax.set_title(col)
axes.flat[-1].axis("off")
fig.suptitle("Boxplots — outliers are reported, never removed")
fig.tight_layout()
plt.show()

# %% [markdown]
# ## 6. Distributions & relationships (feature engineering input)

# %%
fig, axes = plt.subplots(2, 4, figsize=(16, 6))
for ax, col in zip(axes.flat, features):
    sns.histplot(unified[col], ax=ax, kde=True, color="seagreen")
    ax.set_title(col)
axes.flat[-1].axis("off")
fig.suptitle("Feature distributions (unified dataset)")
fig.tight_layout()
plt.show()

# %%
corr = unified[features].corr()["happiness_score"].sort_values(ascending=False)
print("correlation with happiness_score:")
print(corr.round(2).to_string())

fig, ax = plt.subplots(figsize=(8, 4))
corr.drop("happiness_score").plot.bar(ax=ax, color="steelblue")
ax.set_ylabel("Pearson r")
ax.set_title("Correlation of each feature with the target")
plt.tight_layout()
plt.show()

# %%
fig, ax = plt.subplots(figsize=(9, 7))
sns.heatmap(unified[features].corr(), annot=True, fmt=".2f", cmap="vlag",
            center=0, ax=ax)
ax.set_title("Feature correlation matrix")
plt.tight_layout()
plt.show()

# %%
fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for ax, col in zip(axes, ["gdp", "health", "family"]):
    sns.scatterplot(data=unified, x=col, y="happiness_score", hue="year",
                    palette="viridis", ax=ax, legend=False)
    ax.set_title(f"happiness_score vs {col}")
plt.tight_layout()
plt.show()

# %%
print("region coverage (propagated from 2015-2016):")
print(unified["region"].value_counts().head(15).to_string())

# %% [markdown]
# ## 7. Great Expectations — data-quality gate on the unified dataset
#
# 29 expectations: column order, no nulls, year domain, per-feature ranges,
# `(country, year)` uniqueness and dtypes. The Airflow DAG runs this gate
# **before** training and stops if it fails.

# %%
from src.validate import run_validation

summary = run_validation(unified, raise_on_failure=False)
print(f"rows validated : {summary['rows_validated']}")
print(f"expectations   : {summary['n_expectations']}")
print(f"failed         : {summary['n_failed']}")
print("result         :", "PASSED ✓" if summary["successful"] else "FAILED ✗")
if summary["failed"]:
    for failure in summary["failed"]:
        print("  FAILED", failure)

# %% [markdown]
# ## 8. Deliverables produced by this notebook
#
# * `data/processed/unified_happiness.parquet` (+ `.csv`) — 782 rows,
#   0 nulls, unique `(country, year)`, validated.
# * Data-quality observations (sections 1-6) and the unified schema proposal.
#
# Next: `notebooks/model_training.ipynb` (features + model).

# %%
print(f"wrote {config.UNIFIED_PARQUET.relative_to(config.ROOT)}")
print(f"wrote {config.UNIFIED_CSV.relative_to(config.ROOT)}")
unified.head(10)
