import argparse
import sys
import subprocess
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Upload local CSVs to HDFS for batch processing")
    parser.add_argument("--local-flights", default="spark/data/flights")
    parser.add_argument("--local-weather", default="spark/data/weather_hourly_batch.csv")
    parser.add_argument("--hdfs-base", default="hdfs://nn1:9000")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--cleanup", dest="cleanup", action="store_true")
    parser.add_argument("--no-cleanup", dest="cleanup", action="store_false")
    parser.set_defaults(cleanup=True)
    return parser.parse_args()


def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def upload_file(local: Path, hdfs_path: str, overwrite: bool):
    if not local.exists():
        raise FileNotFoundError(f"Local file not found: {local}")

    # Najpierw tworzymy katalog w HDFS, żeby put nie wywalił się na braku ścieżki.
    hdfs_dir = hdfs_path.rsplit("/", 1)[0]
    run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])

    if overwrite:
        run(["hdfs", "dfs", "-put", "-f", str(local), hdfs_path])
    else:
        run(["hdfs", "dfs", "-put", str(local), hdfs_path])


def safe_remove(path: Path):
    try:
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            # try remove dir only if empty
            path.rmdir()
    except Exception as exc:
        print(f"Warning: failed to remove {path}: {exc}")


def main():
    args = parse_args()
    local_flights = Path(args.local_flights)
    local_weather = Path(args.local_weather)

    hdfs_base = args.hdfs_base.rstrip("/")
    flights_targets = {
        "flights_2025.csv": f"{hdfs_base}/bigdata/flight_delay/raw/flights/flights_2025.csv",
        "flights_2026.csv": f"{hdfs_base}/bigdata/flight_delay/raw/flights/flights_2026.csv",
    }
    weather_target = f"{hdfs_base}/bigdata/flight_delay/raw/weather/weather_hourly_6m_8cities.csv"

    print("START: Upload danych do HDFS")
    print(f"Flights dir: {local_flights}")
    print(f"Weather file: {local_weather}")

    for name, hdfs_path in flights_targets.items():
        local_path = local_flights / name
        if local_path.exists():
            print(f"Uploading flights file: {local_path}")
            upload_file(local_path, hdfs_path, args.overwrite)
            if args.cleanup:
                print(f"Removing local flights file: {local_path}")
                safe_remove(local_path)
        else:
            print(f"Warning: missing local flights file {local_path}, skipping")

    # Weather jest jednym plikiem, więc wrzucamy go osobno.
    if local_weather.exists():
        print(f"Uploading weather file: {local_weather}")
        upload_file(local_weather, weather_target, args.overwrite)
        if args.cleanup:
            print(f"Removing local weather file: {local_weather}")
            safe_remove(local_weather)
    else:
        print(f"Warning: missing local weather file {local_weather}, skipping")

    # Jeśli cleanup włączony, spróbuj usunąć katalog z lotami jeśli jest pusty
    if args.cleanup and local_flights.exists() and local_flights.is_dir():
        try:
            local_flights.rmdir()
            print(f"Removed empty flights directory: {local_flights}")
        except OSError:
            # katalog nie jest pusty lub wystąpił błąd — pomijamy
            pass

    print("DONE: Upload danych do HDFS")


if __name__ == "__main__":
    main()
