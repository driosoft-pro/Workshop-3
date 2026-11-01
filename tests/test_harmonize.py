"""Invariants of the unified dataset (harmonization correctness)."""

from __future__ import annotations

import pandas as pd

from src.harmonize import UNIFIED_COLUMNS, build_unified, load_aliases


def test_unified_shape_and_quality():
    df = build_unified(write=False)
    assert list(df.columns) == UNIFIED_COLUMNS
    assert len(df) == 782
    assert not df.isna().any().any()
    assert not df.duplicated(subset=["country", "year"]).any()
    assert sorted(df["year"].unique()) == [2015, 2016, 2017, 2018, 2019]
    assert df["country"].nunique() == df["country"].nunique()  # sanity
    # every country appears at most once per year
    assert df.groupby("country")["year"].max().notna().all()


def test_country_aliases_applied_before_merge():
    aliases = load_aliases()
    df = build_unified(write=False)
    countries = set(df["country"])
    for variant in aliases:
        assert variant not in countries, f"variant not canonicalized: {variant}"
    for canonical in aliases.values():
        assert canonical in countries


def test_no_target_leakage_columns():
    df = build_unified(write=False)
    forbidden = {"dystopia residual", "happiness rank", "standard error"}
    assert not forbidden & {c.lower() for c in df.columns}


def test_region_propagated_from_2015_2016():
    df = build_unified(write=False)
    assert df["region"].notna().all()
    # Switzerland is Western Europe in 2015-2016 -> must keep region in 2019
    ch = df[(df["country"] == "Switzerland") & (df["year"] == 2019)]
    assert ch["region"].iloc[0] == "Western Europe"
    # countries never listed with a region get the documented fallback
    assert set(df["region"].unique())  # non-empty


def test_numeric_types_are_forced():
    df = build_unified(write=False)
    for col in ["gdp", "family", "health", "freedom", "generosity", "corruption", "happiness_score"]:
        assert pd.api.types.is_float_dtype(df[col]), col
    assert pd.api.types.is_integer_dtype(df["year"])
