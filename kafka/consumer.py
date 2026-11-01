#!/usr/bin/env python
"""Kafka consumer with real-time inference.

Reliability contract, per message::

    poll() -> INSERT raw_happiness_events (RECEIVED)   [own transaction]
           -> json.loads        failure -> INVALID_SCHEMA
           -> validate(payload) failure -> INVALID_SCHEMA / INVALID_VALUES
           -> DataFrame(...)[FEATURES] -> model.predict
                            exception -> PREDICTION_ERROR
           -> ONE transaction: upsert dim_country/dim_date,
                                INSERT fact_predictions,
                                raw row -> VALID
           -> commit(offset)      <- only after everything is persisted

Invalid events are *business outcomes*: they are stored with their status,
skipped from prediction and never crash the consumer. Database outages are
*infrastructure failures*: the offset is NOT committed, the message is
retried with backoff and the process exits so the supervisor restarts it
from the last committed offset (at-least-once + idempotent loads).

Usage::

    python kafka/consumer.py
"""

from __future__ import annotations

import argparse
import json
import math
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, text

from src import config
from src.contract import (
    FEATURES,
    TARGET_FIELD,
    STATUS_INVALID_SCHEMA,
    STATUS_INVALID_VALUES,
    STATUS_PREDICTION_ERROR,
    STATUS_VALID,
    ContractError,
    validate,
)
from src.metrics import observe, start_metrics_server

# --- SQL --------------------------------------------------------------------
SQL_RAW_INSERT = """
INSERT INTO raw_happiness_events
    (kafka_topic, kafka_partition, kafka_offset, kafka_key, kafka_timestamp, raw_payload)
VALUES (:topic, :partition, :offset, :key, :kts, :raw)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
RETURNING raw_event_id
"""

SQL_FINISH_RAW = """
UPDATE raw_happiness_events
SET processing_status = :status,
    event_id = :event_id,
    payload_json = :payload_json,
    error_detail = :detail
WHERE raw_event_id = :raw_id
"""

SQL_COUNTRY_UPSERT = """
INSERT INTO dim_country (country_name, region)
VALUES (:name, :region)
ON CONFLICT (country_name) DO UPDATE
   SET region = CASE WHEN EXCLUDED.region <> 'Unknown'
                     THEN EXCLUDED.region
                     ELSE dim_country.region END
RETURNING country_id
"""

SQL_DATE_UPSERT = """
INSERT INTO dim_date (date_id, full_date, year, quarter, month, day)
VALUES (:date_id, :full_date, :year, :quarter, :month, :day)
ON CONFLICT (date_id) DO UPDATE SET full_date = EXCLUDED.full_date
RETURNING date_id
"""

SQL_FACT_INSERT = """
INSERT INTO fact_predictions
    (raw_event_id, country_id, date_id, model_id, observation_year,
     actual_score, predicted_score, prediction_error, abs_prediction_error,
     event_time)
VALUES (:raw_id, :country_id, :date_id, :model_id, :observation_year,
        :actual, :predicted, :error, :abs_error, :event_time)
ON CONFLICT (raw_event_id) DO NOTHING
RETURNING prediction_id
"""

SQL_MODEL_UPSERT = """
INSERT INTO dim_model (model_version, algorithm, trained_at, mae, rmse, r2)
VALUES (:version, :algorithm, :trained_at, :mae, :rmse, :r2)
ON CONFLICT (model_version) DO UPDATE
   SET algorithm = EXCLUDED.algorithm, trained_at = EXCLUDED.trained_at,
       mae = EXCLUDED.mae, rmse = EXCLUDED.rmse, r2 = EXCLUDED.r2
RETURNING model_id
"""


def sanitize_for_jsonb(value):
    """Replace NaN/Infinity with null (PostgreSQL jsonb rejects them).

    The verbatim message is always kept in ``raw_payload`` (TEXT), so no
    information is lost.
    """
    if isinstance(value, dict):
        return {k: sanitize_for_jsonb(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_jsonb(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def ensure_schema(engine) -> None:
    """Create tables/views if they do not exist (idempotent DDL)."""
    for name in ("create_tables.sql", "kpis.sql"):
        sql = (config.ROOT / "sql" / name).read_text()
        with engine.begin() as conn:
            conn.execute(text(sql))


class HappinessConsumer:
    """Consumes happiness-events and runs inference + persistence."""

    def __init__(self, bootstrap: str, topic: str, group: str) -> None:
        self.topic = topic
        self.engine = create_engine(
            config.DATABASE_URL, pool_pre_ping=True, pool_recycle=3600
        )
        ensure_schema(self.engine)

        self.model = joblib.load(config.MODEL_PATH)
        self.metadata = json.loads(config.MODEL_METADATA_PATH.read_text())
        with self.engine.begin() as conn:
            self.model_id = conn.execute(text(SQL_MODEL_UPSERT), self._model_args()).scalar_one()

        self._seed_countries()

        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,   # offsets only after persistence
                "auto.offset.reset": "earliest",
                "client.id": "happiness-consumer",
            }
        )
        self.consumer.subscribe([topic])
        self.group = group
        self.running = True

    # --- startup helpers ----------------------------------------------------
    def _model_args(self) -> dict:
        m = self.metadata
        return {
            "version": m["model_version"],
            "algorithm": m["algorithm"],
            "trained_at": m["trained_at"],
            "mae": m.get("mae"),
            "rmse": m.get("rmse"),
            "r2": m.get("r2"),
        }

    def _seed_countries(self) -> None:
        """Preload dim_country with the regions known from the batch dataset."""
        path = config.PROCESSED_DIR / "unified_happiness.csv"
        if not path.exists():
            print("[warn] unified dataset not found: dim_country starts empty "
                  "(run python -m src.harmonize)", file=sys.stderr)
            return
        df = pd.read_csv(path, usecols=["country", "region"]).drop_duplicates()
        with self.engine.begin() as conn:
            for row in df.itertuples(index=False):
                conn.execute(
                    text(SQL_COUNTRY_UPSERT),
                    {"name": row.country, "region": row.region or "Unknown"},
                )
        print(f"[start] dim_country seeded with {len(df)} countries")

    # --- per-message pipeline ----------------------------------------------
    def process(self, msg) -> str:
        """Process one message; returns the final processing status."""
        raw_text = msg.value().decode("utf-8", errors="replace") if msg.value() else ""
        kts_ms = msg.timestamp()[1] or None
        kts = (
            datetime.fromtimestamp(kts_ms / 1000, tz=timezone.utc)
            if kts_ms
            else None
        )

        # 1) RAW FIRST — before any parsing, validation or prediction
        with self.engine.begin() as conn:
            raw_id = conn.execute(
                text(SQL_RAW_INSERT),
                {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
                    "kts": kts,
                    "raw": raw_text,
                },
            ).scalar_one_or_none()
        if raw_id is None:  # redelivery of an already-processed offset
            print(f"[dup] p{msg.partition()}@{msg.offset()} already stored, skipping")
            return "DUPLICATE"

        # 2) JSON parsing
        try:
            payload = json.loads(raw_text)
        except (json.JSONDecodeError, ValueError) as exc:
            self._finish(raw_id, STATUS_INVALID_SCHEMA, None, None,
                         f"malformed JSON: {exc}")
            return STATUS_INVALID_SCHEMA

        # 3) Event contract
        try:
            validate(payload)
        except ContractError as exc:
            self._finish(raw_id, exc.status, payload.get("event_id"),
                         sanitize_for_jsonb(payload), exc.detail)
            return exc.status

        # 4) Features in the training order + inference
        try:
            started = time.perf_counter()
            X = pd.DataFrame([{f: payload[f] for f in FEATURES}], columns=FEATURES)
            predicted = float(self.model.predict(X)[0])
            latency = time.perf_counter() - started
        except Exception as exc:  # noqa: BLE001 - any prediction failure is recorded
            self._finish(raw_id, STATUS_PREDICTION_ERROR, payload.get("event_id"),
                         sanitize_for_jsonb(payload), f"predict failed: {exc}")
            return STATUS_PREDICTION_ERROR

        # 5) Dimensions + fact + raw status, in ONE transaction
        event_time = datetime.fromisoformat(payload["event_time"])
        actual = float(payload[TARGET_FIELD])
        error = predicted - actual
        with self.engine.begin() as conn:
            country_id = conn.execute(
                text(SQL_COUNTRY_UPSERT),
                {"name": payload["country"], "region": "Unknown"},
            ).scalar_one()
            date_id = conn.execute(
                text(SQL_DATE_UPSERT),
                {
                    "date_id": int(event_time.strftime("%Y%m%d")),
                    "full_date": event_time.date(),
                    "year": event_time.year,
                    "quarter": (event_time.month - 1) // 3 + 1,
                    "month": event_time.month,
                    "day": event_time.day,
                },
            ).scalar_one()
            conn.execute(
                text(SQL_FACT_INSERT),
                {
                    "raw_id": raw_id,
                    "country_id": country_id,
                    "date_id": date_id,
                    "model_id": self.model_id,
                    "observation_year": payload["year"],
                    "actual": round(actual, 3),
                    "predicted": round(predicted, 3),
                    "error": round(error, 3),
                    "abs_error": round(abs(error), 3),
                    "event_time": event_time.replace(tzinfo=None),
                },
            )
            self._finish_in_tx(conn, raw_id, STATUS_VALID, payload.get("event_id"),
                               sanitize_for_jsonb(payload), None)

        observe(STATUS_VALID, latency=latency, abs_error=abs(error))
        return STATUS_VALID

    @staticmethod
    def _finish_in_tx(conn, raw_id: int, status: str, event_id, payload_json, detail) -> None:
        conn.execute(text(SQL_FINISH_RAW), {
            "status": status,
            "event_id": event_id,
            "payload_json": json.dumps(payload_json, default=str) if payload_json is not None else None,
            "detail": detail,
            "raw_id": raw_id,
        })

    def _finish(self, raw_id: int, status: str, event_id, payload_json, detail) -> None:
        """Persist the final raw status in its own transaction + count it."""
        with self.engine.begin() as conn:
            self._finish_in_tx(conn, raw_id, status, event_id, payload_json, detail)
        observe(status)

    def close(self) -> None:
        try:
            self.consumer.close()
        finally:
            self.engine.dispose()


def run(app: HappinessConsumer, max_attempts: int = 5) -> int:
    print(f"[start] topic={app.topic} group={app.group} "
          f"model={app.metadata['model_version']} db={config.DATABASE_URL.split('@')[-1]}")

    while app.running:
        msg = app.consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[kafka error] {msg.error()}", file=sys.stderr)
            continue

        status = None
        attempt = 0
        while app.running:
            try:
                status = app.process(msg)
                break
            except Exception as exc:  # noqa: BLE001 - infrastructure failure
                attempt += 1
                print(f"[infra error] p{msg.partition()}@{msg.offset()} "
                      f"attempt {attempt}/{max_attempts}: {exc}", file=sys.stderr)
                if attempt >= max_attempts:
                    print("[fatal] giving up WITHOUT committing the offset: "
                          "Kafka will redeliver after restart", file=sys.stderr)
                    app.close()
                    return 1
                time.sleep(min(2**attempt, 10))

        if status is None:  # interrupted while retrying (SIGTERM/SIGINT)
            break

        app.consumer.commit(message=msg, asynchronous=False)
        if status != "DUPLICATE":
            print(f"[{status}] p{msg.partition()}@{msg.offset()} "
                  f"key={msg.key().decode() if msg.key() else '-'}")

    app.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--group", default=config.CONSUMER_GROUP)
    parser.add_argument("--metrics-port", type=int, default=config.METRICS_PORT)
    args = parser.parse_args()

    if start_metrics_server(args.metrics_port):
        print(f"[metrics] http://localhost:{args.metrics_port}/metrics")
    else:
        print(f"[warn] metrics port {args.metrics_port} already in use",
              file=sys.stderr)

    app = HappinessConsumer(args.bootstrap, args.topic, args.group)
    signal.signal(signal.SIGTERM, lambda *_: setattr(app, "running", False))
    signal.signal(signal.SIGINT, lambda *_: setattr(app, "running", False))

    try:
        return run(app)
    except KeyboardInterrupt:  # pragma: no cover
        app.close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
