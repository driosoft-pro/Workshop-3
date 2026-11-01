"""Event contract for the ``happiness-events`` Kafka topic.

Single source of truth shared by the producer, the consumer, the model
training code and the tests. Nothing else may redefine ``FEATURES`` or the
valid value ranges.

Validation rule of thumb:

* shape / type / presence  -> ``INVALID_SCHEMA``
* value outside its domain -> ``INVALID_VALUES``
* exception during ``predict`` -> ``PREDICTION_ERROR``
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

# Fixed feature order: the consumer must build the input DataFrame in this
# exact order (it matches the training pipeline).
FEATURES: list[str] = [
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
]

# Evaluation only. Travels inside the event so downstream code can score the
# prediction, but it must NEVER be used as a model input (target leakage).
TARGET_FIELD = "actual_happiness_score"

# Inclusive [min, max] ranges, calibrated from the unified dataset EDA.
RANGES: dict[str, tuple[float, float]] = {
    "gdp": (0.0, 2.5),
    "family": (0.0, 2.0),
    "health": (0.0, 1.5),
    "freedom": (0.0, 1.0),
    "generosity": (0.0, 1.0),
    "corruption": (0.0, 1.0),
    TARGET_FIELD: (0.0, 10.0),
}

YEAR_RANGE: tuple[int, int] = (2015, 2019)

# Every field the event must carry, in producer emission order.
REQUIRED: list[str] = [
    "event_id",
    "country",
    "year",
    *FEATURES,
    TARGET_FIELD,
    "event_time",
]

# Raw states a message can end in (stored in raw_happiness_events).
STATUS_RECEIVED = "RECEIVED"
STATUS_VALID = "VALID"
STATUS_INVALID_SCHEMA = "INVALID_SCHEMA"
STATUS_INVALID_VALUES = "INVALID_VALUES"
STATUS_PREDICTION_ERROR = "PREDICTION_ERROR"
PROCESSING_STATUSES = (
    STATUS_RECEIVED,
    STATUS_VALID,
    STATUS_INVALID_SCHEMA,
    STATUS_INVALID_VALUES,
    STATUS_PREDICTION_ERROR,
)


class ContractError(Exception):
    """Raised when a payload breaks the contract.

    ``status`` is the processing status the consumer must persist in
    ``raw_happiness_events`` before skipping the event.
    """

    def __init__(self, status: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def _is_number(value: Any) -> bool:
    """True for int/float but not for bool (bool is a subclass of int)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate(payload: Any) -> dict:
    """Validate an event payload against the contract.

    Returns the payload unchanged when valid, otherwise raises
    :class:`ContractError` with the status the consumer must store.
    """
    if not isinstance(payload, dict):
        raise ContractError("INVALID_SCHEMA", "payload is not a JSON object")

    missing = [field for field in REQUIRED if payload.get(field) is None]
    if missing:
        raise ContractError("INVALID_SCHEMA", f"missing/null fields: {missing}")

    for field in ("event_id", "country"):
        value = payload[field]
        if not isinstance(value, str) or not value.strip():
            raise ContractError(
                "INVALID_SCHEMA", f"{field} must be a non-empty string"
            )

    year = payload["year"]
    if isinstance(year, bool) or not isinstance(year, int):
        raise ContractError("INVALID_SCHEMA", "year must be an integer")
    if not YEAR_RANGE[0] <= year <= YEAR_RANGE[1]:
        raise ContractError(
            "INVALID_VALUES", f"year out of range: {year} (allowed {YEAR_RANGE})"
        )

    for field, (low, high) in RANGES.items():
        value = payload[field]
        if not _is_number(value):
            raise ContractError("INVALID_SCHEMA", f"{field} must be numeric")
        if not math.isfinite(value) or not low <= value <= high:
            raise ContractError(
                "INVALID_VALUES", f"{field}={value} outside [{low}, {high}]"
            )

    event_time = payload["event_time"]
    if not isinstance(event_time, str):
        raise ContractError("INVALID_SCHEMA", "event_time must be a string")
    try:
        datetime.fromisoformat(event_time)
    except ValueError:
        raise ContractError(
            "INVALID_VALUES", f"event_time is not ISO-8601: {event_time!r}"
        )

    return payload
