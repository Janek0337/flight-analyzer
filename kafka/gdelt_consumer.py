"""Standalone Kafka consumer for GDELT event messages.

This consumer reads from the topic produced by `kafka/gdelt_producer.py`.
It decodes JSON messages and prints a compact summary of each event.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gdelt_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "gdelt-events-consumer")


def _decode_message(msg) -> Optional[Dict[str, Any]]:
    try:
        payload = msg.value().decode("utf-8")
        return json.loads(payload)
    except Exception as exc:
        print(f"[-] Failed to decode/parse message: {exc}")
        return None


def _format_event(record: Dict[str, Any]) -> str:
    payload = record.get("record") if isinstance(record, dict) else record
    if not isinstance(payload, dict):
        return f"[+] GDELT event (raw): {record}"

    event_id = payload.get("GLOBALEVENTID", "unknown")
    event_date = payload.get("SQLDATE") or payload.get("DATEADDED") or "unknown-date"
    actor1 = payload.get("Actor1Name") or payload.get("Actor1Code") or "Actor1"
    actor2 = payload.get("Actor2Name") or payload.get("Actor2Code") or "Actor2"
    action_geo = payload.get("ActionGeo_FullName") or payload.get("ActionGeo_CountryCode") or "unknown-location"
    event_code = payload.get("EventCode", "unknown-event")
    tone = payload.get("AvgTone")
    mentions = payload.get("NumMentions")

    details = [
        f"id={event_id}",
        f"date={event_date}",
        f"event={event_code}",
        f"actors={actor1}->{actor2}",
        f"location={action_geo}",
    ]
    if tone is not None:
        details.append(f"tone={tone}")
    if mentions is not None:
        details.append(f"mentions={mentions}")

    return "[+] GDELT " + " ".join(details)


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

            print(_format_event(record))
    except KeyboardInterrupt:
        print("[+] Consumer stopped")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
