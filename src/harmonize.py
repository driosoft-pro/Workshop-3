"""Schema harmonization for the World Happiness 2015-2019 CSV files.

The five yearly files do not share a schema: column names, extra columns and
country spellings all differ. This module maps every year onto one unified
long-format analytical schema (one row per country-year) and is the **single
implementation** used by the EDA notebook, the training step and the Airflow
DAG.

Usage::

    python -m src.harmonize          # writes data/processed/unified_happiness.*
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

from src import config

# --- Column mapping: raw name (per year) -> unified snake_case name ---------
# Columns not listed here (Rank, Standard Error, confidence intervals,
# Whiskers, Dystopia Residual) are dropped: they are derived from the target
# or from its uncertainty -> target leakage / unavailable in streaming.
RENAME: dict[int, dict[str, str]] = {
    2015: {
        "Country": "country",
        "Region": "region",
        "Happiness Score": "happiness_score",
        "Economy (GDP per Capita)": "gdp",
        "Family": "family",
        "Health (Life Expectancy)": "health",
        "Freedom": "freedom",
        "Trust (Government Corruption)": "corruption",
        "Generosity": "generosity",
    },
    2016: {
        "Country": "country",
        "Region": "region",
        "Happiness Score": "happiness_score",
        "Economy (GDP per Capita)": "gdp",
        "Family": "family",
        "Health (Life Expectancy)": "health",
        "Freedom": "freedom",
        "Trust (Government Corruption)": "corruption",
        "Generosity": "generosity",
    },
    2017: {
        "Country": "country",
        "Happiness.Score": "happiness_score",
        "Economy..GDP.per.Capita.": "gdp",
        "Family": "family",
        "Health..Life.Expectancy.": "health",
        "Freedom": "freedom",
        "Trust..Government.Corruption.": "corruption",
        "Generosity": "generosity",
    },
    2018: {
        "Country or region": "country",
        "Score": "happiness_score",
        "GDP per capita": "gdp",
        "Social support": "family",
        "Healthy life expectancy": "health",
        "Freedom to make life choices": "freedom",
        "Perceptions of corruption": "corruption",
        "Generosity": "generosity",
    },
    2019: {
        "Country or region": "country",
        "Score": "happiness_score",
        "GDP per capita": "gdp",
        "Social support": "family",
        "Healthy life expectancy": "health",
        "Freedom to make life choices": "freedom",
        "Perceptions of corruption": "corruption",
        "Generosity": "generosity",
    },
}

# Unified schema (column order is asserted by the Great Expectations suite).
UNIFIED_COLUMNS: list[str] = [
    "country",
    "year",
    "region",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "happiness_score",
]
NUMERIC_COLUMNS = [
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "happiness_score",
]
REGION_YEARS = (2015, 2016)  # only years that carry a Region column


def load_aliases() -> dict[str, str]:
    """Load the versioned country-alias dictionary (variant -> canonical)."""
    path = config.REFERENCE_DIR / "country_aliases.csv"
    df = pd.read_csv(path)
    return {
        str(v).strip(): str(c).strip()
        for v, c in zip(df["variant"], df["canonical"])
    }


def _canonical_country(series: pd.Series, aliases: dict[str, str]) -> pd.Series:
    """Strip, collapse inner whitespace and apply COUNTRY_ALIASES."""
    cleaned = (
        series.astype("string")
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )
    return cleaned.replace(aliases)


def read_year(path: Path, aliases: dict[str, str]) -> pd.DataFrame:
    """Read one raw CSV and map it onto the unified schema."""
    year = int(path.stem)
    if year not in RENAME:
        raise ValueError(f"unexpected input file: {path.name}")
    raw = pd.read_csv(path)

    missing = [c for c in RENAME[year] if c not in raw.columns]
    if missing:
        raise ValueError(f"{path.name}: expected columns not found: {missing}")

    df = raw.rename(columns=RENAME[year])
    df["year"] = year  # the year lives in the file name, not in the file
    df["country"] = _canonical_country(df["country"], aliases)

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            # errors="raise": a non-numeric cell is a bug, not a silent NaN
            df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")

    keep = [c for c in UNIFIED_COLUMNS if c in df.columns]
    df = df[keep]  # drops rank/std-error/whiskers/dystopia, keeps one row/country-year
    return df


def _impute_missing(df: pd.DataFrame) -> list[str]:
    """Impute isolated missing indicators with the country's own history.

    Rationale: dropping the row would lose a whole country-year; a single
    missing cell does not justify discarding five other valid indicators.
    Fallback when the country has no other year: column median. The model
    pipeline keeps a ``SimpleImputer(median)`` as a safety net for new events.
    """
    notes: list[str] = []
    for col in NUMERIC_COLUMNS:
        for idx in df.index[df[col].isna()]:
            country = df.at[idx, "country"]
            year = df.at[idx, "year"]
            peers = df.loc[
                (df["country"] == country) & (df["year"] != year), col
            ].dropna()
            value = float(peers.mean()) if len(peers) else float(df[col].median())
            source = (
                f"mean of {country} other years"
                if len(peers)
                else "column median"
            )
            df.at[idx, col] = value
            notes.append(
                f"{col} [{country} {year}] was null -> {value:.4f} ({source})"
            )
    return notes


def _attach_regions(region_source: pd.DataFrame, countries: pd.Series) -> pd.Series:
    """Propagate region (only present in 2015-2016) to every country-year."""
    known = (
        region_source.dropna(subset=["region"])
        .drop_duplicates(subset=["country"])
        .set_index("country")["region"]
    )
    return countries.map(known).fillna("Unknown").astype("string")


def build_unified(write: bool = True) -> pd.DataFrame:
    """Harmonize all yearly CSVs into one validated DataFrame.

    Raises on schema errors, unexpected nulls or duplicate (country, year)
    pairs, so both the notebook and the Airflow DAG fail loudly.
    """
    aliases = load_aliases()
    paths = sorted(config.RAW_DIR.glob("20*.csv"))
    if not paths:
        raise FileNotFoundError(f"no raw CSVs found in {config.RAW_DIR}")

    frames = [read_year(p, aliases) for p in paths]
    region_source = pd.concat(
        [f[["country", "region"]] for f in frames if "region" in f.columns],
        ignore_index=True,
    )
    df = pd.concat(frames, ignore_index=True)  # long format: one row per country-year

    df = df.drop(columns=["region"], errors="ignore")
    df["region"] = _attach_regions(region_source, df["country"])

    imputed = _impute_missing(df)

    # --- Enforce types -------------------------------------------------------
    df["country"] = df["country"].astype("string")
    df["year"] = df["year"].astype("int64")
    for col in NUMERIC_COLUMNS:
        df[col] = df[col].astype("float64")

    df = df[UNIFIED_COLUMNS]

    # --- Invariants ----------------------------------------------------------
    if df[UNIFIED_COLUMNS].isna().any().any():
        nulls = df.columns[df.isna().any()].tolist()
        raise ValueError(f"unexpected nulls after cleaning: {nulls}")
    dup = df.duplicated(subset=["country", "year"])
    if dup.any():
        raise ValueError(
            f"duplicate (country, year) pairs: {df.loc[dup, ['country', 'year']].to_dict('records')}"
        )
    bad_years = sorted(set(df["year"]) - set(range(2015, 2020)))
    if bad_years:
        raise ValueError(f"years outside 2015-2019: {bad_years}")

    if write:
        config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        df.to_parquet(config.UNIFIED_PARQUET, index=False)
        df.to_csv(config.UNIFIED_CSV, index=False)

    build_unified.last_imputations = imputed  # surfaced by CLI/notebook
    return df


def _alias_report(df: pd.DataFrame) -> pd.DataFrame:
    """Countries present in fewer than 5 years (leftover variant detector)."""
    per_year = df.groupby("country")["year"].nunique()
    incomplete = per_year[per_year < 5].sort_values()
    return incomplete.rename("years_present").reset_index()


def main() -> int:
    df = build_unified(write=True)

    print(f"unified dataset: {df.shape[0]} rows x {df.shape[1]} cols")
    print("rows per year:")
    print(df["year"].value_counts().sort_index().to_string())
    print(f"unique countries: {df['country'].nunique()}")
    print(f"(country, year) unique: {not df.duplicated(['country', 'year']).any()}")
    print(f"nulls: {int(df.isna().sum().sum())}")

    imputed = getattr(build_unified, "last_imputations", [])
    if imputed:
        print("imputed values:")
        for note in imputed:
            print(f"  - {note}")

    incomplete = _alias_report(df)
    print(
        f"countries in <5 years: {len(incomplete)} "
        "(review for leftover name variants)"
    )
    if len(incomplete) <= 20:
        print(incomplete.to_string(index=False))

    print(f"wrote {config.UNIFIED_PARQUET.relative_to(config.ROOT)}")
    print(f"wrote {config.UNIFIED_CSV.relative_to(config.ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
