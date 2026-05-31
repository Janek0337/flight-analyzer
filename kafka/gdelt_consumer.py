"""Standalone Kafka consumer for GDELT event messages.

This consumer reads from the topic produced by `kafka/gdelt_producer.py`.
It decodes JSON messages and prints a compact summary of each event.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gdelt_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "gdelt-events-consumer")
ES_ENABLED = os.getenv("ES_ENABLED", "false").lower() in ("1", "true", "yes")
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "gdelt-events")
ES_BATCH_SIZE = int(os.getenv("ES_BATCH_SIZE", "100"))


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


def _create_es_client() -> Optional[Any]:
    if not ES_ENABLED:
        return None

    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        raise RuntimeError(
            "Elasticsearch support requires the 'elasticsearch' Python package. "
            "Install it with pip install elasticsearch"
        )

    client = Elasticsearch(hosts=[ES_HOST])
    if not client.ping():
        raise RuntimeError(f"Cannot connect to Elasticsearch at {ES_HOST}")
    return client


def _prepare_document(record: Dict[str, Any]) -> Dict[str, Any]:
    doc = {"indexed_at": int(time.time())}
    if isinstance(record, dict):
        doc.update(record)
        if isinstance(doc.get("record"), dict):
            inner = doc.pop("record")
            doc.update(inner)
    return doc


def _document_id(doc: Dict[str, Any]) -> Optional[str]:
    event_id = doc.get("GLOBALEVENTID")
    return str(event_id) if event_id is not None else None


def _index_to_elasticsearch(client: Any, doc: Dict[str, Any]) -> None:
    try:
        client.index(index=ES_INDEX, id=_document_id(doc), document=doc)
    except Exception as exc:
        print(f"[-] Failed to write document to Elasticsearch: {exc}")


def main() -> int:
    es_client = None
    if ES_ENABLED:
        print(f"[+] Elasticsearch enabled: {ES_HOST} index={ES_INDEX}")
        es_client = _create_es_client()
    else:
        print("[+] Elasticsearch disabled; set ES_ENABLED=true to enable indexing")

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
            if es_client is not None:
                doc = _prepare_document(record)
                _index_to_elasticsearch(es_client, doc)
    except KeyboardInterrupt:
        print("[+] Consumer stopped")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
