from __future__ import annotations

import argparse
import bz2
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve


DEFAULT_SOURCES = {
    "2025": "https://www.eurocontrol.int/performance/data/download/csv/ert_dly_ansp_2025.csv.bz2",
    "2026": "https://www.eurocontrol.int/performance/data/download/csv/ert_dly_ansp_2026.csv.bz2",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download EUROCONTROL delay CSV files")
    parser.add_argument("--output-dir", default="spark/data/flights")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--upload-hdfs", action="store_true")
    parser.add_argument("--hdfs-base", default="hdfs://nn1:9000")
    parser.add_argument("--hdfs-target-dir", default="/bigdata/flight_delay/raw/flights")
    return parser.parse_args()


def download_file(url: str, output_path: Path, overwrite: bool) -> None:
    if output_path.exists() and not overwrite:
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, output_path)


def decompress_bz2(bz2_path: Path, csv_path: Path, overwrite: bool) -> None:
    if csv_path.exists() and not overwrite:
        return

    with bz2.open(bz2_path, "rb") as source, csv_path.open("wb") as target:
        shutil.copyfileobj(source, target)


def cleanup_archive(bz2_path: Path) -> None:
    if bz2_path.exists():
        bz2_path.unlink()


def upload_to_hdfs(local_csv: Path, hdfs_uri: str) -> None:
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", str(local_csv), hdfs_uri],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Failed HDFS upload for {local_csv.name}")


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("START: Wsadowe przetwarzanie lotów")
    print(f"Output dir: {output_dir}")

    try:
        for year, url in DEFAULT_SOURCES.items():
            bz2_path = output_dir / f"ert_dly_ansp_{year}.csv.bz2"
            csv_path = output_dir / f"flights_{year}.csv"

            if not args.skip_download:
                print(f"Downloading flights for {year}")
                download_file(url, bz2_path, args.overwrite)

            if not bz2_path.exists():
                raise FileNotFoundError(f"Missing source file: {bz2_path}. Run without --skip-download.")

            print(f"Preparing CSV for {year}")
            decompress_bz2(bz2_path, csv_path, args.overwrite)
            cleanup_archive(bz2_path)

            if args.upload_hdfs:
                hdfs_uri = (
                    f"{args.hdfs_base.rstrip('/')}{args.hdfs_target_dir}/flights_{year}.csv"
                )
                print(f"Uploading flights for {year} to HDFS")
                upload_to_hdfs(csv_path, hdfs_uri)

    except (HTTPError, URLError, OSError, RuntimeError, FileNotFoundError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("DONE: Wsadowe przetwarzanie lotów")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())