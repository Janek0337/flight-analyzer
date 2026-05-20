from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AIRPORTS_FILE = PROJECT_ROOT / "spark" / "airports.csv"
HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_gusts_10m",
]


@dataclass(frozen=True)
class Airport:
    icao: str
    airport: str
    latitude: float
    longitude: float


DEFAULT_AIRPORTS = [
    Airport("EPWA", "Warsaw Chopin", 52.1657, 20.9671),
    Airport("EDDF", "Frankfurt", 50.0379, 8.5622),
    Airport("LFPG", "Paris Charles de Gaulle", 49.0097, 2.5479),
    Airport("EGLL", "London Heathrow", 51.47, -0.4543),
    Airport("EHAM", "Amsterdam Schiphol", 52.3105, 4.7683),
    Airport("LEMD", "Madrid Barajas", 40.4983, -3.5676),
    Airport("LIRF", "Rome Fiumicino", 41.8003, 12.2389),
    Airport("LOWW", "Vienna", 48.1103, 16.5697),
    Airport("EBBR", "Brussels", 50.9014, 4.4844),
    Airport("LSZH", "Zurich", 47.4647, 8.5492),
    Airport("ESSA", "Stockholm Arlanda", 59.6519, 17.9186),
    Airport("ENGM", "Oslo Gardermoen", 60.1939, 11.1004),
    Airport("EKCH", "Copenhagen", 55.6181, 12.6561),
    Airport("EFHK", "Helsinki Vantaa", 60.3172, 24.9633),
    Airport("LPPT", "Lisbon", 38.7742, -9.1342),
    Airport("LROP", "Bucharest Otopeni", 44.5711, 26.085),
    Airport("LKPR", "Prague Vaclav Havel", 50.1008, 14.26),
    Airport("LHBP", "Budapest Ferenc Liszt", 47.4394, 19.2619),
    Airport("LGAV", "Athens", 37.9364, 23.9445),
    Airport("EIDW", "Dublin", 53.4213, -6.2701),
    Airport("LBSF", "Sofia", 42.6967, 23.4114),
    Airport("LDZA", "Zagreb", 45.7429, 16.0688),
    Airport("LJLA", "Ljubljana", 46.2237, 14.4576),
    Airport("LZIB", "Bratislava", 48.1702, 17.2127),
    Airport("EYVI", "Vilnius", 54.6341, 25.2858),
    Airport("EVRA", "Riga", 56.9236, 23.9711),
    Airport("EETN", "Tallinn", 59.4133, 24.8328),
    Airport("LUKK", "Chisinau", 46.9277, 28.9308),
    Airport("UKBB", "Kyiv Boryspil", 50.345, 30.8947),
    Airport("LTFM", "Istanbul", 41.2753, 28.7519),
    Airport("ELLX", "Luxembourg", 49.6233, 6.2044),
    Airport("LATI", "Tirana", 41.4147, 19.7206),
    Airport("LCLK", "Larnaca", 34.8751, 33.6249),
    Airport("UDYZ", "Yerevan Zvartnots", 40.1473, 44.3959),
    Airport("LQSA", "Sarajevo", 43.8246, 18.3315),
    Airport("LYBE", "Belgrade Nikola Tesla", 44.8184, 20.3091),
    Airport("LWSK", "Skopje", 41.9616, 21.6214),
    Airport("LMML", "Malta", 35.8575, 14.4775),
    Airport("BKPR", "Pristina", 42.5728, 21.0358),
]


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date: {value}. Use YYYY-MM-DD") from exc


def daterange_chunks(start: date, end: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def load_airports(path: Path | None) -> list[Airport]:
    airports_path = path
    if airports_path is None and DEFAULT_AIRPORTS_FILE.exists():
        airports_path = DEFAULT_AIRPORTS_FILE

    if airports_path is None:
        return DEFAULT_AIRPORTS

    if not airports_path.exists():
        raise FileNotFoundError(f"Airports file not found: {airports_path}")

    airports: list[Airport] = []
    with airports_path.open("r", encoding="utf-8") as handle:
        if airports_path.suffix.lower() == ".json":
            data = json.load(handle)
            for row in data:
                airports.append(
                    Airport(
                        icao=row["icao"],
                        airport=row["airport"],
                        latitude=float(row["latitude"]),
                        longitude=float(row["longitude"]),
                    )
                )
        else:
            reader = csv.DictReader(handle)
            for row in reader:
                airports.append(
                    Airport(
                        icao=row["icao"],
                        airport=row["airport"],
                        latitude=float(row["latitude"]),
                        longitude=float(row["longitude"]),
                    )
                )
    if not airports:
        raise ValueError("Airports list is empty")
    return airports


def fetch_hourly_weather(airport: Airport, start: date, end: date, timeout_s: int) -> dict:
    params = {
        "latitude": airport.latitude,
        "longitude": airport.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_FIELDS),
        "timezone": "UTC",
    }
    query = urlencode(params)
    url = f"{ARCHIVE_API_URL}?{query}"

    with urlopen(url, timeout=timeout_s) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def append_rows(output_path: Path, airport: Airport, payload: dict) -> int:
    hourly = payload.get("hourly") or {}
    timestamps = hourly.get("time") or []
    if not timestamps:
        return 0

    fieldnames = ["icao", "airport", "latitude", "longitude", "time", *HOURLY_FIELDS]
    should_write_header = not output_path.exists() or output_path.stat().st_size == 0

    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if should_write_header:
            writer.writeheader()

        count = 0
        for index, ts in enumerate(timestamps):
            row = {
                "icao": airport.icao,
                "airport": airport.airport,
                "latitude": airport.latitude,
                "longitude": airport.longitude,
                "time": ts,
            }
            for metric in HOURLY_FIELDS:
                values = hourly.get(metric) or []
                row[metric] = values[index] if index < len(values) else None
            writer.writerow(row)
            count += 1
    return count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download hourly weather from Open-Meteo")
    parser.add_argument("--start-date", type=parse_date, default=parse_date("2025-01-01"), help="YYYY-MM-DD")
    # Domyślny koniec zakresu ustawiony na 2026-04-04 — zgodnie z dostępnymi delays
    parser.add_argument("--end-date", type=parse_date, default=parse_date("2026-04-04"), help="YYYY-MM-DD")
    parser.add_argument("--output", default="spark/data/weather_hourly_batch.csv")
    parser.add_argument(
        "--airports-file",
        type=Path,
        default=None,
        help="Optional CSV/JSON with columns: icao,airport,latitude,longitude. Replaces spark/airports.csv and code defaults.",
    )
    parser.add_argument("--chunk-days", type=int, default=30)
    parser.add_argument("--retry", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.start_date > args.end_date:
        print("ERROR: start-date cannot be after end-date", file=sys.stderr)
        return 1
    if args.chunk_days <= 0:
        print("ERROR: chunk-days must be > 0", file=sys.stderr)
        return 1

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("START: Wsadowe przetwarzanie pogody")
    print(f"Output: {output_path}")

    if output_path.exists() and not args.overwrite:
        print(f"ERROR: output exists: {output_path}. Use --overwrite to replace.", file=sys.stderr)
        return 1
    if output_path.exists() and args.overwrite:
        output_path.unlink()

    airports = load_airports(args.airports_file)
    if args.airports_file is not None:
        print(f"Airports source: {args.airports_file}")
    elif DEFAULT_AIRPORTS_FILE.exists():
        print(f"Airports source: {DEFAULT_AIRPORTS_FILE}")
    else:
        print("Airports source: built-in defaults")
    print(f"Airports: {len(airports)}")
    # Nie wysyłamy zapytań dla przyszłych dat — API zwraca 400 dla zakresów poza dostępnymi danymi.
    effective_end = min(args.end_date, date.today())
    if effective_end != args.end_date:
        print(f"Date range: {args.start_date} -> {args.end_date} (capped to {effective_end} - today)")
    else:
        print(f"Date range: {args.start_date} -> {args.end_date}")

    total_rows = 0
    total_calls = 0

    for airport in airports:
        print(f"\n[{airport.icao}] {airport.airport}")
        for chunk_start, chunk_end in daterange_chunks(args.start_date, effective_end, args.chunk_days):
            # Dane pobieramy kawałkami, żeby nie robić zbyt dużych requestów do API.
            total_calls += 1
            for attempt in range(1, args.retry + 1):
                try:
                    payload = fetch_hourly_weather(
                        airport=airport,
                        start=chunk_start,
                        end=chunk_end,
                        timeout_s=args.timeout_seconds,
                    )
                    rows = append_rows(output_path, airport, payload)
                    total_rows += rows
                    print(f"  {chunk_start} -> {chunk_end} | rows={rows} | attempt={attempt}")
                    break
                except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
                    if attempt == args.retry:
                        print(f"  FAILED {chunk_start} -> {chunk_end} after {args.retry} attempts: {exc}", file=sys.stderr)
                        return 2
                    # Prosty backoff przy chwilowych błędach sieci.
                    backoff = attempt * 2
                    print(f"  retry {attempt}/{args.retry} for {chunk_start} -> {chunk_end}: {exc}")
                    time.sleep(backoff)

            time.sleep(args.sleep_seconds)

    print("\nDONE")
    print(f"API calls: {total_calls}")
    print(f"Rows written: {total_rows}")
    print(f"Output: {output_path.resolve()}")
    print("DONE: Wsadowe przetwarzanie pogody")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
