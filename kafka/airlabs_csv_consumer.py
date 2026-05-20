"""Kafka consumer for AirLabs delayed flights that saves all data to CSV.

This file does not modify existing consumers.
It reads from the `delays` topic and writes all fields to a CSV file.

Environment variables:
    KAFKA_BOOTSTRAP   - Kafka bootstrap server, default: localhost:9092
    KAFKA_TOPIC       - topic to subscribe to, default: delays
    KAFKA_GROUP_ID    - consumer group id, default: airlabs-csv-consumer
    CSV_OUTPUT_FILE   - output CSV file path, default: delays.csv
"""

from __future__ import annotations

import csv
import json
import os
import sys
from typing import Any, Dict, Optional

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "delays")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "airlabs-csv-consumer")
CSV_OUTPUT_FILE = os.getenv("CSV_OUTPUT_FILE", "delays.csv")


def _decode_message(msg) -> Optional[Dict[str, Any]]:
    try:
        raw = msg.value().decode("utf-8")
        return json.loads(raw)
    except Exception as exc:
        print(f"[-] Failed to decode/parse message: {exc}")
        return None


def _flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested structures for CSV (keep as-is for now; nested fields become JSON strings)."""
    flat = {}
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            flat[key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[key] = value
    return flat


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
    print(f"[+] Output file: {CSV_OUTPUT_FILE}")

    csv_file = None
    csv_writer = None
    fieldnames = None
    message_count = 0

    try:
        with open(CSV_OUTPUT_FILE, "w", newline="", encoding="utf-8") as csv_file:
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

                flat_record = _flatten_record(record)

                # Initalize CSV writer with fieldnames from first message
                if csv_writer is None:
                    fieldnames = list(flat_record.keys())
                    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    csv_writer.writeheader()
                    print(f"[+] CSV created with {len(fieldnames)} columns")

                # Write the record
                csv_writer.writerow(flat_record)
                message_count += 1

                if message_count % 100 == 0:
                    print(f"[+] Saved {message_count} records to {CSV_OUTPUT_FILE}")
                    csv_file.flush()

    except KeyboardInterrupt:
        print(f"\n[+] Consumer stopped. Saved {message_count} total records.")
    except Exception as exc:
        print(f"[-] Unexpected error: {exc}")
        return 1
    finally:
        consumer.close()

    print(f"[+] Output saved to: {CSV_OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

