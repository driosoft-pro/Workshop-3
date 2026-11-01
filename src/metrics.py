"""Prometheus metrics for the streaming consumer (operational health).

Business KPIs live in PostgreSQL/Superset; these metrics answer a different
question: *is the pipeline healthy right now?* — scraped by Prometheus and
visualized in Grafana.

Exposed on ``http://localhost:8000/metrics`` (``METRICS_PORT``).
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram, start_http_server

# How many messages ended in each processing status (VALID, INVALID_*, ...)
EVENTS_CONSUMED = Counter(
    "events_consumed_total",
    "Messages consumed, labelled by final processing status",
    ["status"],
)

# Model inference latency (consumer step "predict")
PREDICTION_LATENCY = Histogram(
    "prediction_latency_seconds",
    "Time spent inside model.predict()",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# Absolute prediction error of successful predictions (streaming MAE proxy)
PREDICTION_ABS_ERROR = Histogram(
    "prediction_abs_error",
    "abs(predicted - actual) for successful predictions",
    buckets=(0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0),
)


def start_metrics_server(port: int) -> bool:
    """Start the /metrics HTTP endpoint; returns False when already in use."""
    try:
        start_http_server(port)
        return True
    except OSError:
        return False


def observe(status: str, latency: float | None = None, abs_error: float | None = None) -> None:
    """Record one processed message."""
    EVENTS_CONSUMED.labels(status=status).inc()
    if latency is not None:
        PREDICTION_LATENCY.observe(latency)
    if abs_error is not None:
        PREDICTION_ABS_ERROR.observe(abs_error)
