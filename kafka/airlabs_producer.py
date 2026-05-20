"""Pobieranie opóźnionych lotów z AirLabs i publikacja do Kafki.

Domyślnie używa endpointu:
    https://airlabs.co/api/v9/delays?delay=60&type=departures&api_key=...

Konfiguracja przez zmienne środowiskowe:
    AIRLABS_API_KEY   - wymagane
    KAFKA_BOOTSTRAP   - np. localhost:9092
    KAFKA_TOPIC       - domyślnie delays
    AIRLABS_DELAY     - próg opóźnienia w minutach, domyślnie 60
    AIRLABS_TYPE      - departures / arrivals, domyślnie departures
    AIRLABS_LIMIT     - liczba rekordów na stronę, domyślnie 500
    AIRLABS_OFFSET    - offset początkowy, domyślnie 0
    POLL_INTERVAL     - sekundy między pobraniami, domyślnie 60
    MAX_POLL_COUNT    - opcjonalnie: ile razy pobrać dane i zakończyć (do testów)
"""

from __future__ import annotations

import json
import logging
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from typing import Any, Dict, Iterable, List, Optional

from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airlabs-producer")

AIRLABS_URL = os.getenv("AIRLABS_URL", "https://airlabs.co/api/v9/delays")
API_KEY = os.getenv("AIRLABS_API_KEY")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "delays")
DEFAULT_DELAY = int(os.getenv("AIRLABS_DELAY", "60"))
DEFAULT_TYPE = os.getenv("AIRLABS_TYPE", "departures")
DEFAULT_LIMIT = int(os.getenv("AIRLABS_LIMIT", "50"))
DEFAULT_OFFSET = int(os.getenv("AIRLABS_OFFSET", "0"))
AIRLABS_USER_AGENT = os.getenv(
    "AIRLABS_USER_AGENT",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
)
AIRLABS_REFERER = os.getenv("AIRLABS_REFERER", "https://airlabs.co/")
AIRLABS_ORIGIN = os.getenv("AIRLABS_ORIGIN", "https://airlabs.co")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
MAX_POLL_COUNT = int(os.getenv("MAX_POLL_COUNT", "0"))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def _normalize_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("response", "data", "flights", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    if isinstance(payload.get("response"), dict):
        nested = payload["response"]
        for key in ("data", "flights", "result"):
            value = nested.get(key)
            if isinstance(value, list):
                return value

    return []


def _has_more(payload: Dict[str, Any]) -> Optional[bool]:
    candidates = [payload.get("has_more")]

    request_block = payload.get("request")
    if isinstance(request_block, dict):
        candidates.append(request_block.get("has_more"))

    response_block = payload.get("response")
    if isinstance(response_block, dict):
        candidates.append(response_block.get("has_more"))
        nested_request = response_block.get("request")
        if isinstance(nested_request, dict):
            candidates.append(nested_request.get("has_more"))

    for value in candidates:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "y"}:
                return True
            if normalized in {"false", "0", "no", "n"}:
                return False

    return None


def fetch_delays(
    delay_minutes: int,
    delay_type: str,
    limit: int = DEFAULT_LIMIT,
    offset: int = DEFAULT_OFFSET,
) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("Set AIRLABS_API_KEY environment variable")

    params = {
        "delay": delay_minutes,
        "type": delay_type,
        "api_key": API_KEY,
        "limit": limit,
        "offset": offset,
    }

    url = f"{AIRLABS_URL}?{urlencode(params)}"
    parsed = urlparse(url)
    headers = {
        "User-Agent": AIRLABS_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": AIRLABS_REFERER,
        "Origin": AIRLABS_ORIGIN,
        "Host": parsed.netloc,
    }
    request = Request(url, headers=headers, method="GET")
    try:
        with urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = "<unable to read error body>"
        logger.error("AirLabs request failed: url=%s status=%s body=%s", url, getattr(exc, "code", None), body)
        raise


def build_message(
    record: Dict[str, Any],
    delay_minutes: int,
    delay_type: str,
) -> Dict[str, Any]:
    enriched = dict(record)
    enriched["_source"] = "airlabs"
    enriched["_fetched_at"] = int(time.time())
    enriched["_airlabs_delay_threshold"] = delay_minutes
    enriched["_airlabs_type"] = delay_type
    return enriched


def record_key(record: Dict[str, Any]) -> str:
    candidates = (
        record.get("flight_iata"),
        record.get("flight_icao"),
        record.get("flight_number"),
        record.get("dep_iata") or record.get("departure_iata"),
        record.get("arr_iata") or record.get("arrival_iata"),
    )
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return "unknown-flight"


def delivery_report(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug("Delivered message to %s [%s]", msg.topic(), msg.partition())


def publish_batch(batch: Iterable[Dict[str, Any]]) -> None:
    for rec in batch:
        payload = json.dumps(rec, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        producer.produce(
            topic=TOPIC,
            key=record_key(rec),
            value=payload,
            callback=delivery_report,
        )
    producer.flush()


def run_once(
    delay_minutes: int,
    delay_type: str,
    limit: int = DEFAULT_LIMIT,
    start_offset: int = DEFAULT_OFFSET,
) -> int:
    total_published = 0
    offset = max(0, start_offset)

    while True:
        data = fetch_delays(
            delay_minutes=delay_minutes,
            delay_type=delay_type,
            limit=limit,
            offset=offset,
        )
        records = _normalize_items(data)

        if not records:
            logger.info(
                "AirLabs returned no delayed flights for delay=%s type=%s offset=%s",
                delay_minutes,
                delay_type,
                offset,
            )
            break

        delayed_records = [
            build_message(
                record,
                delay_minutes=delay_minutes,
                delay_type=delay_type,
            )
            for record in records
        ]
        logger.info(
            "Publishing %d delayed flights to topic=%s (delay=%s, type=%s, limit=%s, offset=%s)",
            len(delayed_records),
            TOPIC,
            delay_minutes,
            delay_type,
            limit,
            offset,
        )
        publish_batch(delayed_records)
        total_published += len(delayed_records)

        page_has_more = _has_more(data)
        if page_has_more is False:
            break
        if page_has_more is None and len(records) < limit:
            break

        offset += limit

    return total_published


def main_loop(poll_interval: int = POLL_INTERVAL, max_poll_count: int = MAX_POLL_COUNT) -> None:
    if not API_KEY:
        raise RuntimeError("Set AIRLABS_API_KEY environment variable")

    if max_poll_count == 0:
        max_poll_count = 1

    poll_count = 0

    while True:
        try:
            run_once(
                delay_minutes=DEFAULT_DELAY,
                delay_type=DEFAULT_TYPE,
                limit=DEFAULT_LIMIT,
                start_offset=DEFAULT_OFFSET,
            )
            poll_count += 1
            if max_poll_count and poll_count >= max_poll_count:
                logger.info("Reached MAX_POLL_COUNT=%s, exiting", max_poll_count)
                break
        except (HTTPError, URLError) as exc:
            logger.error("HTTP error while fetching delays: %s", exc)
        except Exception:
            logger.exception("Unexpected error")

        time.sleep(poll_interval)


if __name__ == "__main__":
    main_loop()
