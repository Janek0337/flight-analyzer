"""Pobieranie danych GDELT Events i publikacja do Kafki.

Konfiguracja przez zmienne środowiskowe:
    GDELT_SOURCE      - URL lub lokalna ścieżka do pliku GDELT (.csv/.tsv/.gz/.zip)
    KAFKA_BOOTSTRAP   - np. localhost:9092
    KAFKA_TOPIC       - domyślnie gdelt_raw
    POLL_INTERVAL     - sekundy między pobraniami, domyślnie 86400
    MAX_POLL_COUNT    - ile razy pobrać dane i zakończyć (0 = bez limitu)
    MAX_RECORDS       - maksymalna liczba rekordów wysłanych podczas jednego uruchomienia (0 = wszystkie)
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import os
import ssl
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from confluent_kafka import Producer

logger = logging.getLogger("gdelt-producer")
logging.basicConfig(level=logging.INFO)

GDELT_SOURCE = os.getenv("GDELT_SOURCE", "http://data.gdeltproject.org/events/20260530.export.CSV.zip")
GDELT_BASE_URL = os.getenv("GDELT_BASE_URL", "http://data.gdeltproject.org/events")
GDELT_START_DATE = os.getenv("GDELT_START_DATE", "20260101")
GDELT_END_DATE = os.getenv("GDELT_END_DATE", "20260529")
GDELT_INSECURE = os.getenv("GDELT_INSECURE", "false").lower() in ("1", "true", "yes")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gdelt_raw")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "86400"))
MAX_POLL_COUNT = int(os.getenv("MAX_POLL_COUNT", "0"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))

EVENTS_FIELDS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce GDELT Events into Kafka")
    parser.add_argument("--source", default=GDELT_SOURCE, help="URL or local path to a single GDELT file")
    parser.add_argument("--start-date", default=GDELT_START_DATE, help="Start date for historical download (YYYYMMDD)")
    parser.add_argument("--end-date", default=GDELT_END_DATE, help="End date for historical download (YYYYMMDD)")
    parser.add_argument("--base-url", default=GDELT_BASE_URL, help="Base URL for daily GDELT archives")
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Kafka topic to publish to")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL, help="Seconds between polling runs")
    parser.add_argument("--max-polls", type=int, default=MAX_POLL_COUNT, help="How many polling cycles to execute")
    parser.add_argument("--max-records", type=int, default=MAX_RECORDS, help="Max records to send per cycle")
    parser.add_argument("--insecure", action="store_true", default=GDELT_INSECURE, help="Disable SSL certificate verification for downloads")
    parser.add_argument("--dry-run", action="store_true", help="Parse records without sending to Kafka")
    return parser.parse_args()


def download_source(url: str, dest_path: str, insecure: bool = False) -> str:
    if os.path.exists(url):
        return url

    logger.info("Downloading GDELT file from %s", url)
    ctx = None
    if insecure:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

    request = Request(url, headers={"User-Agent": "flight-analyzer-gdelt-producer"})
    with urlopen(request, context=ctx) as response:
        with open(dest_path, "wb") as out_file:
            out_file.write(response.read())
    return dest_path


def open_input(path: str):
    if path.endswith(".zip"):
        zf = zipfile.ZipFile(path)
        file_name = zf.namelist()[0]
        return io.TextIOWrapper(zf.open(file_name, "r"), encoding="utf-8", errors="ignore")
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


def parse_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError as exc:
        raise RuntimeError("Date must be in YYYYMMDD format") from exc


def date_range(start_date: str, end_date: str) -> list[str]:
    start = parse_date(start_date)
    end = parse_date(end_date or start_date)
    if end < start:
        raise RuntimeError("end-date must be the same or later than start-date")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def build_gdelt_url(date_str: str, base_url: str) -> str:
    return f"{base_url.rstrip('/')}/{date_str}.export.CSV.zip"


def resolve_sources(source: str, start_date: str, end_date: str, base_url: str) -> list[str]:
    if start_date:
        if not end_date:
            end_date = start_date
        return [build_gdelt_url(d, base_url) for d in date_range(start_date, end_date)]
    if not source:
        raise RuntimeError("GDELT source is required; set GDELT_SOURCE or use --source")
    return [source]


def normalize_row(row: list[str]) -> dict[str, str]:
    return {key: value for key, value in zip(EVENTS_FIELDS, row)}


def build_payload(record: dict[str, str]) -> bytes:
    event = {
        "source": "gdelt",
        "format": "gdelt_events",
        "record": record,
    }
    return json.dumps(event, ensure_ascii=False).encode("utf-8")


def record_key(record: dict[str, str]) -> bytes:
    key = record.get("GLOBALEVENTID") or record.get("SOURCEURL") or "unknown"
    return str(key).encode("utf-8")


def delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)


def produce_records(producer: Producer | None, source: str, topic: str, max_records: int = 0, dry_run: bool = False, insecure: bool = False) -> int:
    if not source:
        raise RuntimeError("GDELT source is required; set GDELT_SOURCE or use --source")
    if not dry_run and producer is None:
        raise RuntimeError("Producer must be provided unless --dry-run is used")

    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = source
        if source.startswith("http://") or source.startswith("https://"):
            filename = os.path.basename(urlparse(source).path)
            source_path = os.path.join(tmpdir, filename)
            source_path = download_source(source, source_path, insecure=insecure)

        with open_input(source_path) as raw:
            reader = csv.reader(raw, delimiter="\t")
            sent = 0
            for row in reader:
                if not row or len(row) < len(EVENTS_FIELDS):
                    continue

                record = normalize_row(row)
                payload = build_payload(record)
                key = record_key(record)

                if dry_run:
                    logger.info("Dry run: %s", payload.decode("utf-8"))
                else:
                    producer.produce(topic, key=key, value=payload, callback=delivery_report)
                    producer.poll(0)

                sent += 1
                if max_records and sent >= max_records:
                    break

            if not dry_run:
                producer.flush()

    logger.info("Produced %d records to topic %s from source %s", sent, topic, source)
    return sent


def run_once(
    producer: Producer | None,
    sources: list[str],
    topic: str,
    max_records: int,
    dry_run: bool,
    insecure: bool,
) -> None:
    logger.info("Running GDELT producer for %d sources into topic=%s", len(sources), topic)
    try:
        total = 0
        for source in sources:
            if max_records and total >= max_records:
                break
            remaining = max_records - total if max_records else 0
            produced = produce_records(
                producer=producer,
                source=source,
                topic=topic,
                max_records=remaining,
                dry_run=dry_run,
                insecure=insecure,
            )
            total += produced
        logger.info("Total produced %d records", total)
    except (HTTPError, URLError) as err:
        logger.error("HTTP error while fetching GDELT source: %s", err)
    except Exception:
        logger.exception("Unexpected error during GDELT production")


def main_loop(
    producer: Producer | None,
    sources: list[str],
    topic: str,
    interval: int,
    max_poll_count: int,
    max_records: int,
    dry_run: bool,
    insecure: bool,
) -> None:
    if max_poll_count == 0:
        max_poll_count = 1

    poll_count = 0
    while True:
        run_once(
            producer=producer,
            sources=sources,
            topic=topic,
            max_records=max_records,
            dry_run=dry_run,
            insecure=insecure,
        )
        poll_count += 1
        if max_poll_count and poll_count >= max_poll_count:
            logger.info("Reached max poll count %d, exiting", max_poll_count)
            break
        logger.info("Sleeping %s seconds before next poll", interval)
        time.sleep(interval)


if __name__ == "__main__":
    args = parse_args()
    sources = resolve_sources(args.source, args.start_date, args.end_date, args.base_url)
    producer = None if args.dry_run else Producer({"bootstrap.servers": args.bootstrap})
    main_loop(
        producer=producer,
        sources=sources,
        topic=args.topic,
        interval=args.interval,
        max_poll_count=args.max_polls,
        max_records=args.max_records,
        dry_run=args.dry_run,
        insecure=args.insecure,
    )
