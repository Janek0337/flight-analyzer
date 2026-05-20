#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="$ROOT_DIR/.venv"

START_DATE="${1:-2025-01-01}"
# Domyślny koniec zakresu: 4 kwietnia 2026 (mamy delays do tej daty)
END_DATE="${2:-2026-04-04}"
AIRPORTS_FILE="${3:-}"

REQUIRED_PYTHON_MODULES=(
  pyspark
  numpy
)

get_spark_submit_version() {
  if ! command -v spark-submit >/dev/null 2>&1; then
    return 0
  fi

  spark-submit --version 2>&1 | awk '/ version [0-9]+\.[0-9]+\.[0-9]+/ { print $NF; exit }'
}

major_minor() {
  local version="$1"
  echo "$version" | awk -F. '{ print $1 "." $2 }'
}

ensure_python_dependencies() {
  local missing=()
  local needs_install=0

  for module in "${REQUIRED_PYTHON_MODULES[@]}"; do
    if ! "$PYTHON_BIN" -c "import ${module}" >/dev/null 2>&1; then
      missing+=("$module")
    fi
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    needs_install=1
  fi

  local spark_version=""
  local pyspark_version=""
  spark_version="$(get_spark_submit_version || true)"

  if [[ $needs_install -eq 0 ]]; then
    pyspark_version="$("$PYTHON_BIN" -c 'import pyspark; print(pyspark.__version__)')"
    if [[ -n "$spark_version" ]] && [[ -n "$pyspark_version" ]]; then
      if [[ "$(major_minor "$spark_version")" != "$(major_minor "$pyspark_version")" ]]; then
        echo "Detected Spark/PySpark mismatch: spark-submit=$spark_version, pyspark=$pyspark_version"
        needs_install=1
      fi
    fi
  fi

  if [[ $needs_install -eq 0 ]]; then
    return 0
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing Python modules: ${missing[*]}"
  fi
  echo "Preparing virtualenv at $VENV_DIR"

  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    python3 -m venv "$VENV_DIR"
  fi

  PYTHON_BIN="$VENV_DIR/bin/python"
  echo "Installing dependencies from $ROOT_DIR/requirements.txt using $PYTHON_BIN"
  "$PYTHON_BIN" -m pip install -r "$ROOT_DIR/requirements.txt"

  local still_missing=()
  for module in "${REQUIRED_PYTHON_MODULES[@]}"; do
    if ! "$PYTHON_BIN" -c "import ${module}" >/dev/null 2>&1; then
      still_missing+=("$module")
    fi
  done

  if [[ ${#still_missing[@]} -gt 0 ]]; then
    echo "ERROR: missing Python modules after install: ${still_missing[*]}" >&2
    return 1
  fi

  pyspark_version="$("$PYTHON_BIN" -c 'import pyspark; print(pyspark.__version__)')"
  if [[ -n "$spark_version" ]] && [[ -n "$pyspark_version" ]]; then
    if [[ "$(major_minor "$spark_version")" != "$(major_minor "$pyspark_version")" ]]; then
      echo "ERROR: Spark/PySpark version mismatch after install: spark-submit=$spark_version, pyspark=$pyspark_version" >&2
      echo "Update requirements.txt to a compatible pyspark version." >&2
      return 1
    fi
  fi
}

echo "START: Pipeline lotow i pogody"
echo "Date range: $START_DATE -> $END_DATE"

ensure_python_dependencies

if [[ "${PRECHECK_ONLY:-0}" == "1" ]]; then
  echo "DONE: dependency precheck"
  exit 0
fi

if [[ -n "$AIRPORTS_FILE" ]]; then
  echo "Airports file: $AIRPORTS_FILE"
fi

# Pobieramy dane lotów (domyślnie nadpisujemy lokalne pliki).
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/fetch_flights_batch.py" \
  --output-dir "$ROOT_DIR/spark/data/flights" \
  --overwrite

# Pobieramy pogodę w zadanym zakresie dat.
WEATHER_ARGS=(
  --start-date "$START_DATE"
  --end-date "$END_DATE"
  --output "$ROOT_DIR/spark/data/weather_hourly_batch.csv"
)
if [[ -n "$AIRPORTS_FILE" ]]; then
  WEATHER_ARGS+=(--airports-file "$AIRPORTS_FILE")
fi
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/fetch_weather_batch.py" "${WEATHER_ARGS[@]}" --overwrite

# Wrzucamy lokalne CSV do HDFS.
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/upload_to_hdfs.py" \
  --local-flights "$ROOT_DIR/spark/data/flights" \
  --local-weather "$ROOT_DIR/spark/data/weather_hourly_batch.csv" \
  --overwrite

# Liczymy agregacje i finalne features.
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/process_flights.py"
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/process_weather.py"
"$PYTHON_BIN" "$ROOT_DIR/spark/scripts/build_features.py"

# Trenujemy model liniowy na finalnych cechach: opóźnienia + lokalna pogoda.
echo "START: Trening modelu liniowego"
"$PYTHON_BIN" "$ROOT_DIR/model/model.py" \
  --input "hdfs://nn1:9000/bigdata/flight_delay/processed/features/daily_features_parquet" \
  --output "hdfs://nn1:9000/bigdata/flight_delay/models/local_weather_delay_regression"
echo "END: Trening modelu liniowego"

echo "DONE: Pipeline lotow i pogody"
echo "Wyniki sprawdzisz tutaj:"
echo "- Lokalne loty: $ROOT_DIR/spark/data/flights"
echo "- Lokalne pogodowe CSV: $ROOT_DIR/spark/data/weather_hourly_batch.csv"
echo "- HDFS daily delays: hdfs://nn1:9000/bigdata/flight_delay/processed/daily_delays"
echo "- HDFS monthly delays: hdfs://nn1:9000/bigdata/flight_delay/processed/monthly_delays"
echo "- HDFS entity delays: hdfs://nn1:9000/bigdata/flight_delay/processed/entity_delays"
echo "- HDFS weather daily: hdfs://nn1:9000/bigdata/flight_delay/processed/weather_daily"
echo "- HDFS weather europe daily: hdfs://nn1:9000/bigdata/flight_delay/processed/weather_europe_daily"
echo "- HDFS final features: hdfs://nn1:9000/bigdata/flight_delay/processed/features/daily_features_parquet"
echo "- HDFS model: hdfs://nn1:9000/bigdata/flight_delay/models/local_weather_delay_regression"
echo "- Metryki modelu są wypisywane wyżej podczas kroku treningu"
