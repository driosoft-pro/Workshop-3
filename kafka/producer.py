#!/usr/bin/env python
"""Kafka producer: streams historical records as simulated real-time events.

Reads ``data/streaming/stream_events.csv`` (the held-out test set produced by
``python -m src.train``) row by row and publishes each one as a JSON event to
the ``happiness-events`` topic, using **country as the Kafka key** so every
event of a country lands on the same partition (per-country ordering).

Usage::

    python kafka/producer.py --delay 0.5                 # valid events only
    python kafka/producer.py --inject-invalid            # + broken events demo
    python kafka/producer.py --limit 10 --run-id r2      # short/idempotent run
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from confluent_kafka import Producer

from src import config
from src.contract import FEATURES, REQUIRED, TARGET_FIELD

STRING_FIELDS = {"event_id", "country", "event_time"}


def on_delivery(err, msg) -> None:
    if err:
        print(f"[ERROR] delivery failed: {err}", file=sys.stderr)
    else:
        key = msg.key().decode() if msg.key() else "<no-key>"
        print(f"[OK] {key} -> p{msg.partition()} @{msg.offset()}")


def _native(value):
    """Convert numpy scalars (from pandas) into plain Python values."""
    return value.item() if hasattr(value, "item") else value


def coerce(field: str, value) -> object:
    """Turn a CSV cell into the JSON type the contract expects.

    * empty cell in a string field      -> ``None``   (JSON null)
    * empty cell in a numeric field     -> ``NaN``    (JSON NaN, rejected later)
    * numeric-looking string            -> ``float`` / ``int`` (year)
    * anything else                     -> kept as-is (e.g. ``"not-a-number"``)
    """
    value = _native(value)
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None if field in STRING_FIELDS else float("nan")
    if isinstance(value, str):
        text = value.strip()
        if field in STRING_FIELDS:
            return text
        try:
            return int(float(text)) if field == "year" else float(text)
        except ValueError:
            return text
    return value


def build_valid_event(row: dict, seq: int, run_id: str, emitted_at: str) -> dict:
    """Map a stream_events.csv row onto the event contract."""
    return {
        "event_id": f"{run_id}E{seq:04d}",
        "country": str(row["country"]),
        "year": int(row["year"]),
        **{field: float(row[field]) for field in FEATURES},
        "actual_happiness_score": float(row["happiness_score"]),
        "event_time": emitted_at,
    }


def build_invalid_event(row: dict, run_id: str, emitted_at: str) -> tuple[dict | None, bytes]:
    """Rebuild a deliberately broken payload from invalid_events.csv.

    The file carries defects declaratively:
      ``_raw``        -> send the raw (malformed) bytes as-is
      ``_drop_field`` -> remove a required field (INVALID_SCHEMA)
      ``_null_field`` -> send JSON null for that field (INVALID_SCHEMA)
      cell values     -> wrong types/values (INVALID_SCHEMA / INVALID_VALUES)
    Returns ``(payload_or_None, serialized_value)``.
    """
    raw = row.get("_raw")
    if isinstance(raw, str) and raw.strip():
        return None, raw.encode("utf-8")

    event = {field: coerce(field, row.get(field)) for field in REQUIRED}

    event_id = event.get("event_id")
    if run_id and isinstance(event_id, str):
        event["event_id"] = f"{run_id}{event_id}"

    drop = row.get("_drop_field")
    if isinstance(drop, str) and drop:
        event.pop(drop, None)

    null_field = row.get("_null_field")
    if isinstance(null_field, str) and null_field:
        event[null_field] = None

    if event.get("event_time") in (None, ""):
        event["event_time"] = emitted_at  # only if the defect is not the date
    return event, json.dumps(event).encode("utf-8")


def produce_file(
    producer: Producer,
    df: pd.DataFrame,
    *,
    topic: str,
    delay: float,
    run_id: str,
    invalid: bool = False,
) -> int:
    emitted_at = datetime.now().isoformat(timespec="seconds")
    sent = 0

    if not invalid:
        for seq, row in enumerate(df.to_dict("records"), start=1):
            event = build_valid_event(row, seq, run_id, emitted_at)
            producer.produce(
                topic,
                key=event["country"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            if delay:
                time.sleep(delay)
        return sent

    for _, row in df.iterrows():
        event, value = build_invalid_event(row.to_dict(), run_id, emitted_at)
        country = (event or {}).get("country") or ""
        key = str(country).encode("utf-8")
        producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
        producer.poll(0)
        sent += 1
        if delay:
            time.sleep(delay)
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default=str(config.STREAM_EVENTS_CSV))
    parser.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--delay", type=float, default=0.5,
                        help="seconds between events (simulates arrival over time)")
    parser.add_argument("--limit", type=int, default=None,
                        help="send only the first N valid events")
    parser.add_argument("--run-id", default="",
                        help="prefix for event_id, e.g. 'r2' when re-running")
    parser.add_argument("--inject-invalid", action="store_true",
                        help="also send the broken events from invalid_events.csv")
    parser.add_argument("--invalid-file", default=str(config.INVALID_EVENTS_CSV))
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "client.id": "happiness-producer",
        }
    )

    df = pd.read_csv(args.file)
    if args.limit:
        df = df.head(args.limit)
    print(f"streaming {len(df)} events from {args.file}")
    print(f"topic={args.topic} bootstrap={args.bootstrap} delay={args.delay}s "
          f"key=country run_id={args.run_id or '-'}")

    sent = produce_file(
        producer, df, topic=args.topic, delay=args.delay, run_id=args.run_id
    )

    if args.inject_invalid:
        invalid_df = pd.read_csv(args.invalid_file)
        print(f"injecting {len(invalid_df)} deliberately broken events "
              f"from {args.invalid_file}")
        sent += produce_file(
            producer, invalid_df, topic=args.topic, delay=args.delay,
            run_id=args.run_id, invalid=True,
        )

    remaining = producer.flush(10)
    if remaining:
        print(f"[ERROR] {remaining} messages were not delivered", file=sys.stderr)
        return 1
    print(f"done: {sent} events sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
