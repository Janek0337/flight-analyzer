"""Standalone Kafka consumer for delayed-flight messages from AirLabs.

This file does not modify the existing `kafka/consumer.py`.
It reads from the Kafka topic that `kafka/airlabs_producer.py` publishes to.

Environment variables:
    KAFKA_BOOTSTRAP   - Kafka bootstrap server, default: localhost:9092
    KAFKA_TOPIC       - topic to subscribe to, default: delays
    KAFKA_GROUP_ID    - consumer group id, default: airlabs-delays-consumer
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "delays")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "airlabs-delays-consumer")


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _decode_message(msg) -> Optional[Dict[str, Any]]:
    try:
        raw = msg.value().decode("utf-8")
        return json.loads(raw)
    except Exception as exc:
        print(f"[-] Failed to decode/parse message: {exc}")
        return None


def _format_delay(record: Dict[str, Any]) -> str:
    flight = record.get("flight_iata") or record.get("flight_number") or record.get("flight_icao") or "unknown"
    dep = record.get("dep_iata") or record.get("departure_iata") or record.get("dep") or "unknown"
    arr = record.get("arr_iata") or record.get("arrival_iata") or record.get("arr") or "unknown"
    delay = (
        _as_int(record.get("delay"))
        or _as_int(record.get("dep_delay"))
        or _as_int(record.get("arrival_delay"))
        or _as_int(record.get("delay_minutes"))
    )
    src = record.get("_source") or "unknown"
    fetched = record.get("_fetched_at")
    date_from = record.get("_airlabs_date_from")
    date_to = record.get("_airlabs_date_to")

    if delay is not None:
        return (
            f"[+] Delayed flight {flight}: {dep} -> {arr}, delay={delay} min "
            f"(source={src}, fetched={fetched}, date_from={date_from}, date_to={date_to})"
        )

    return f"[+] Flight {flight}: {dep} -> {arr} (no numeric delay field) (source={src})"


def main() -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([KAFKA_TOPIC])

    print(f"[+] Subscribed to topic: {KAFKA_TOPIC}")
    print(f"[+] Kafka bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"[+] Consumer group: {KAFKA_GROUP_ID}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[-] Error reading {msg.error()}")
                continue

            record = _decode_message(msg)
            if record is None:
                continue

            print(_format_delay(record))
    except KeyboardInterrupt:
        print("[+] Consumer stopped")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

