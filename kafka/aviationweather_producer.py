import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

API_URL = "https://aviationweather.gov/api/data/dataserver"


BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "weather"



STATIONS_FILE = os.path.join(os.path.dirname(__file__), "stations.txt")
DEFAULT_STATIONS = "KJFK,KLAX,EPWA,EDDF"
HOURS_BEHIND_NOW = 2
POLL_SECONDS = 60
MOST_RECENT_MODE = "constraint"
TIMEOUT_SECONDS = 30


def load_stations():
    
    
    try:
        with open(STATIONS_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return DEFAULT_STATIONS
            
            
            parts = []
            for line in content.replace(',', '\n').splitlines():
                code = line.strip()
                if code:
                    parts.append(code)
            if not parts:
                return DEFAULT_STATIONS
            return ",".join(parts)
    except FileNotFoundError:
        return DEFAULT_STATIONS


def fetch_metars(stations, hours_before_now, timeout_seconds, most_recent_mode):
    # Przygotuj parametry zapytania do API METAR
    params = {
        "requestType": "retrieve",
        "dataSource": "metars",
        "stationString": stations,
        "hoursBeforeNow": hours_before_now,
        "format": "csv",
        "mostRecentForEachStation": most_recent_mode,
    }
    url = f"{API_URL}?{urlencode(params)}"

    with urlopen(url, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8").strip()

    if not body:
        return []

    return list(csv.DictReader(body.splitlines()))


def make_event(record, poll_time):
    # Utwórz zdarzenie (słownik) do wysłania do Kafki
    event = {
        "source": "aviationweather.gov",
        "data_source": "metars",
        "ingested_at": poll_time,
        "station_id": record.get("station_id"),
        "observation_time": record.get("observation_time"),
        "raw_text": record.get("raw_text"),
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "temp_c": record.get("temp_c"),
        "dewpoint_c": record.get("dewpoint_c"),
        "wind_dir_degrees": record.get("wind_dir_degrees"),
        "wind_speed_kt": record.get("wind_speed_kt"),
        "wind_gust_kt": record.get("wind_gust_kt"),
        "visibility_statute_mi": record.get("visibility_statute_mi"),
        "altim_in_hg": record.get("altim_in_hg"),
        "sea_level_pressure_mb": record.get("sea_level_pressure_mb"),
        "flight_category": record.get("flight_category"),
        "metar_type": record.get("metar_type"),
        "elevation_m": record.get("elevation_m"),
        "full_record": record,
    }
    return event


def main():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVER})
    seen_keys = set()

    print("[+] Start Aviation Weather METAR producer")
    print(f"[+] Topic: {TOPIC}")
    

    # Wczytaj listę stacji z pliku lub użyj wartości domyślnej
    STATIONS = load_stations()
    print(f"[+] Stations file: {STATIONS_FILE}")
    print(f"[+] Stations: {STATIONS}")

    while True:
        poll_time = datetime.now(timezone.utc).isoformat()
        try:
            records = fetch_metars(
                stations=STATIONS,
                hours_before_now=HOURS_BEHIND_NOW,
                timeout_seconds=TIMEOUT_SECONDS,
                most_recent_mode=MOST_RECENT_MODE,
            )
        except (HTTPError, URLError, TimeoutError) as exc:
            print(f"[-] HTTP error: {exc}", file=sys.stderr)
            time.sleep(POLL_SECONDS)
            continue
        except Exception as exc:
            print(f"[-] Unexpected error: {exc}", file=sys.stderr)
            time.sleep(POLL_SECONDS)
            continue

        # Wyświetl liczbę pobranych rekordów
        print(f"[+] Fetched {len(records)} records")

        # Przetwórz i wyślij każdy rekord; unikaj duplikatów przez seen_keys
        for record in records:
            key = f"{record.get('station_id')}|{record.get('observation_time')}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            event = make_event(record, poll_time)
            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            producer.produce(TOPIC, key=key.encode("utf-8"), value=payload)
            producer.poll(0)
            print(f"[+] Sent {key}")

        producer.flush()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())