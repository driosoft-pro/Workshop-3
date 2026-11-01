"""Unit tests for the event contract (src/contract.py).

One test per contract rule: presence, types, domain ranges and timestamps.
These map 1:1 to the rows of the contract table in the README.
"""

from __future__ import annotations

import math

import pytest

from src.contract import (
    FEATURES,
    REQUIRED,
    ContractError,
    validate,
)


def valid_payload() -> dict:
    """A payload that must always pass validation."""
    return {
        "event_id": "E0001",
        "country": "Colombia",
        "year": 2019,
        "gdp": 1.2,
        "family": 0.8,
        "health": 0.9,
        "freedom": 0.6,
        "generosity": 0.3,
        "corruption": 0.1,
        "actual_happiness_score": 6.2,
        "event_time": "2026-10-30T15:30:00",
    }


def test_valid_payload_passes():
    payload = valid_payload()
    assert validate(payload) is payload


def test_features_order_matches_training():
    # consumer builds the input DataFrame from FEATURES imported here
    assert FEATURES == [
        "gdp", "family", "health", "freedom", "generosity", "corruption",
    ]
    assert all(f in REQUIRED for f in FEATURES)


@pytest.mark.parametrize("missing", ["event_id", "country", "year", "gdp"])
def test_missing_field_is_invalid_schema(missing):
    payload = valid_payload()
    del payload[missing]
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


@pytest.mark.parametrize("field", ["event_id", "country"])
def test_empty_string_is_invalid_schema(field):
    payload = valid_payload()
    payload[field] = "   "
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


@pytest.mark.parametrize("field", ["event_id", "country", "event_time"])
def test_null_field_is_invalid_schema(field):
    payload = valid_payload()
    payload[field] = None
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


def test_payload_must_be_a_json_object():
    for bad in ([1, 2], "text", None, 42):
        with pytest.raises(ContractError) as exc:
            validate(bad)
        assert exc.value.status == "INVALID_SCHEMA"


def test_year_must_be_an_integer():
    for bad in ("2019", 2019.0, True):
        payload = valid_payload()
        payload["year"] = bad
        with pytest.raises(ContractError) as exc:
            validate(payload)
        assert exc.value.status == "INVALID_SCHEMA"


@pytest.mark.parametrize("bad_year", [2014, 2020])
def test_year_outside_source_range_is_invalid_values(bad_year):
    payload = valid_payload()
    payload["year"] = bad_year
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_VALUES"


@pytest.mark.parametrize("field", [*FEATURES, "actual_happiness_score"])
def test_non_numeric_feature_is_invalid_schema(field):
    payload = valid_payload()
    payload[field] = "not-a-number"
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


@pytest.mark.parametrize("field", [*FEATURES, "actual_happiness_score"])
def test_boolean_feature_is_invalid_schema(field):
    payload = valid_payload()
    payload[field] = True  # bool is an int subclass: must be rejected
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


@pytest.mark.parametrize("field", [*FEATURES, "actual_happiness_score"])
def test_nan_and_infinite_are_invalid_values(field):
    for bad in (math.nan, math.inf, -math.inf):
        payload = valid_payload()
        payload[field] = bad
        with pytest.raises(ContractError) as exc:
            validate(payload)
        assert exc.value.status == "INVALID_VALUES"


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("gdp", -0.5),
        ("gdp", 3.0),
        ("family", 2.5),
        ("health", 2.0),
        ("freedom", -0.1),
        ("generosity", 1.5),
        ("corruption", 1.2),
        ("actual_happiness_score", -1.0),
        ("actual_happiness_score", 99.0),
    ],
)
def test_value_outside_range_is_invalid_values(field, bad):
    payload = valid_payload()
    payload[field] = bad
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_VALUES"


@pytest.mark.parametrize(
    "bad_time",
    ["30/12/2026 15:30:00", "not-a-date", "", "2026-13-45T99:99:99"],
)
def test_bad_event_time_is_invalid_values(bad_time):
    payload = valid_payload()
    payload["event_time"] = bad_time
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_VALUES"


def test_event_time_must_be_a_string():
    payload = valid_payload()
    payload["event_time"] = 1730311800
    with pytest.raises(ContractError) as exc:
        validate(payload)
    assert exc.value.status == "INVALID_SCHEMA"


def test_actual_score_is_not_a_feature():
    # target leakage guard: the evaluation field never joins FEATURES
    assert "actual_happiness_score" not in FEATURES
    assert "actual_happiness_score" in REQUIRED
