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

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "10.198.188.55:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gdelt_raw")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "gdelt-events-documents-consumer")
ES_ENABLED = os.getenv("ES_ENABLED", "true").lower() in ("1", "true", "yes")
ES_HOST = os.getenv("ES_HOST", "https://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "gdelt-events")
ES_BATCH_SIZE = int(os.getenv("ES_BATCH_SIZE", "2000"))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
ES_MAX_RETRIES = int(os.getenv("ES_MAX_RETRIES", "5"))
ES_REQUEST_TIMEOUT = int(os.getenv("ES_REQUEST_TIMEOUT", "30"))
ES_RETRY_ON_TIMEOUT = os.getenv("ES_RETRY_ON_TIMEOUT", "true").lower() in ("1", "true", "yes")


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

    # Prepare connection parameters with authentication if provided
    kwargs = {"hosts": [ES_HOST]}
    if ES_USER and ES_PASSWORD:
        kwargs["basic_auth"] = (ES_USER, ES_PASSWORD)

    # For self-signed certificates, disable SSL verification
    if ES_HOST.startswith("https://"):
        kwargs["verify_certs"] = False

    # Add retry/timeout behavior
    kwargs["max_retries"] = ES_MAX_RETRIES
    kwargs["retry_on_timeout"] = ES_RETRY_ON_TIMEOUT

    client = Elasticsearch(**kwargs)
    if not client.ping():
        raise RuntimeError(f"Cannot connect to Elasticsearch at {ES_HOST}")
    print(f"Created an Elasticsearch connection (retries={ES_MAX_RETRIES}, retry_on_timeout={ES_RETRY_ON_TIMEOUT}, request_timeout={ES_REQUEST_TIMEOUT}s)")
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
    # Robust indexing with retries and exponential backoff
    from elasticsearch import exceptions as es_exceptions

    for attempt in range(1, ES_MAX_RETRIES + 1):
        try:
            client.index(index=ES_INDEX, id=_document_id(doc), document=doc, request_timeout=ES_REQUEST_TIMEOUT)
            doc_id = _document_id(doc)
            print(f"[+] Indexed document to {ES_INDEX} id={doc_id}")
            return
        except Exception as exc:
            # Prefer detailed exception info if available
            exc_type = type(exc).__name__
            print(f"[-] Attempt {attempt}/{ES_MAX_RETRIES} - Failed to write document to Elasticsearch: {exc_type}: {exc}")
            # If this is a non-retryable error, break early
            if isinstance(exc, (es_exceptions.RequestError, es_exceptions.AuthenticationException)):
                print("[-] Non-retryable Elasticsearch error, aborting indexing for this document.")
                break
            if attempt < ES_MAX_RETRIES:
                backoff = 2 ** (attempt - 1)
                print(f"[-] Retrying in {backoff}s...")
                time.sleep(backoff)
            else:
                print("[-] Exhausted retries; giving up on this document.")


def _bulk_index_to_elasticsearch(client: Any, docs: list) -> None:
    """Index a list of prepared documents using the bulk API with retries."""
    try:
        from elasticsearch import helpers, exceptions as es_exceptions
    except ImportError:
        print("[-] Elasticsearch helpers not available; bulk indexing disabled.")
        return

    actions = [
        {"_op_type": "index", "_index": ES_INDEX, "_id": _document_id(d), "_source": d}
        for d in docs
    ]

    for attempt in range(1, ES_MAX_RETRIES + 1):
        try:
            success_count, errors = helpers.bulk(client, actions, request_timeout=ES_REQUEST_TIMEOUT, raise_on_error=False)
            failed = len(errors) if isinstance(errors, list) else 0
            print(f"[+] Bulk indexed {success_count} docs to {ES_INDEX} (failed={failed})")
            if failed:
                print(f"[-] Bulk errors: {errors}")
            return
        except Exception as exc:
            exc_type = type(exc).__name__
            print(f"[-] Bulk attempt {attempt}/{ES_MAX_RETRIES} failed: {exc_type}: {exc}")
            if isinstance(exc, (es_exceptions.RequestError, es_exceptions.AuthenticationException)):
                print("[-] Non-retryable Elasticsearch error during bulk; aborting.")
                break
            if attempt < ES_MAX_RETRIES:
                backoff = 2 ** (attempt - 1)
                print(f"[-] Retrying bulk in {backoff}s...")
                time.sleep(backoff)
            else:
                print("[-] Exhausted bulk retries; giving up on these documents.")


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
        batch: list[Dict[str, Any]] = []
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
                # Use bulk indexing when ES_BATCH_SIZE > 1
                if ES_BATCH_SIZE > 1:
                    batch.append(doc)
                    if len(batch) >= ES_BATCH_SIZE:
                        _bulk_index_to_elasticsearch(es_client, batch)
                        batch.clear()
                else:
                    _index_to_elasticsearch(es_client, doc)
    except KeyboardInterrupt:
        print("[+] Consumer stopped")
    finally:
        # Flush any remaining batched documents
        try:
            if es_client is not None and ES_BATCH_SIZE > 1 and batch:
                print(f"[+] Flushing {len(batch)} remaining documents")
                _bulk_index_to_elasticsearch(es_client, batch)
        except Exception as exc:
            print(f"[-] Error flushing remaining documents: {exc}")
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
