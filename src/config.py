"""Runtime configuration: paths and settings read from the environment.

Every external service (Kafka, PostgreSQL, Prometheus) is configured through
environment variables so the same code runs on Nix/Podman or on Docker.
Defaults match ``docker-compose.yml`` and ``.env.example``.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Optional .env (never committed; see .env.example)
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:  # pragma: no cover - dotenv is optional at import time
    pass

# --- Paths -----------------------------------------------------------------
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
REFERENCE_DIR = DATA_DIR / "reference"
PROCESSED_DIR = DATA_DIR / "processed"
STREAMING_DIR = DATA_DIR / "streaming"
MODELS_DIR = ROOT / "models"
UNIFIED_PARQUET = PROCESSED_DIR / "unified_happiness.parquet"
UNIFIED_CSV = PROCESSED_DIR / "unified_happiness.csv"
MODEL_PATH = MODELS_DIR / "model.pkl"
MODEL_METADATA_PATH = MODELS_DIR / "model_metadata.json"
STREAM_EVENTS_CSV = STREAMING_DIR / "stream_events.csv"
INVALID_EVENTS_CSV = STREAMING_DIR / "invalid_events.csv"

# --- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "happiness-events")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "happiness-ml-team")

# --- PostgreSQL ------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://happiness:happiness@localhost:5432/happiness",
)

# --- Observability ---------------------------------------------------------
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
