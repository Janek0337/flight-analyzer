import argparse
import csv
import json
import math
from pathlib import Path

import matplotlib
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = "hdfs://nn1:9000/bigdata/flight_delay/processed/features/daily_features_parquet"
DEFAULT_OUTPUT = REPO_ROOT / "bigdata/flight_delay/analytics/hdfs_weather_delay_basic"

WEATHER_FEATURES = [
    ("feature_temperature", "Srednia temperatura [C]"),
    ("feature_humidity", "Srednia wilgotnosc [%]"),
    ("feature_precipitation", "Suma opadow [mm]"),
    ("feature_rain", "Suma deszczu [mm]"),
    ("feature_snowfall", "Suma opadow sniegu [cm]"),
    ("feature_cloud_cover", "Srednie zachmurzenie [%]"),
    ("feature_wind_speed", "Srednia predkosc wiatru [km/h]"),
    ("feature_wind_gusts", "Maksymalne porywy wiatru [km/h]"),
]

TARGETS = [
    ("delay_per_flight", "Calkowite opoznienie na lot [min]"),
    ("weather_delay_per_flight", "Opoznienie pogodowe na lot [min]"),
]

HEATMAP_COLUMNS = WEATHER_FEATURES + TARGETS


def parse_args():
    parser = argparse.ArgumentParser(
        description="Czyta features z HDFS, liczy ogolne korelacje pogody z opoznieniami i tworzy wykresy 01, 03, 04."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Sciezka HDFS do Parquet z daily features")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Lokalny katalog wynikowy")
    parser.add_argument("--app-name", default="HDFS Weather Delay Basic Correlations", help="Nazwa aplikacji Spark")
    return parser.parse_args()


def clean_number(value):
    if value is None:
        return None
    value = float(value)
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def weighted_corr_expr(x_col, y_col, weight_col="total_flights"):
    w = F.col(weight_col).cast("double")
    x = F.col(x_col).cast("double")
    y = F.col(y_col).cast("double")
    sum_w = F.sum(w)
    avg_x = F.sum(w * x) / sum_w
    avg_y = F.sum(w * y) / sum_w
    cov_xy = F.sum(w * x * y) / sum_w - avg_x * avg_y
    var_x = F.sum(w * x * x) / sum_w - avg_x * avg_x
    var_y = F.sum(w * y * y) / sum_w - avg_y * avg_y
    return F.when((var_x > 0) & (var_y > 0), cov_xy / F.sqrt(var_x * var_y))


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def prepare_features(raw):
    required = {"date", "total_flights", "total_delay", "weather_delay"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise RuntimeError(f"Brak wymaganych kolumn w danych wejsciowych: {', '.join(missing)}")

    features = raw.withColumn("date", F.to_date("date")).withColumn(
        "delay_per_flight",
        F.when(F.col("total_flights") > 0, F.col("total_delay") / F.col("total_flights")),
    ).withColumn(
        "weather_delay_per_flight",
        F.when(F.col("total_flights") > 0, F.col("weather_delay") / F.col("total_flights")),
    )

    existing_weather = [name for name, _ in WEATHER_FEATURES if name in features.columns]
    missing_weather = [name for name, _ in WEATHER_FEATURES if name not in features.columns]
    if missing_weather:
        raise RuntimeError(f"Brak wymaganych kolumn pogodowych: {', '.join(missing_weather)}")

    return features.where(F.col("total_flights") > 0), existing_weather


def query_summary(features):
    row = features.agg(
        F.count("*").alias("row_count"),
        F.sum("total_flights").cast("long").alias("total_flights"),
        F.min("date").cast("string").alias("min_date"),
        F.max("date").cast("string").alias("max_date"),
        (F.sum("total_delay") / F.sum("total_flights")).alias("delay_per_flight"),
        (F.sum("weather_delay") / F.sum("total_flights")).alias("weather_delay_per_flight"),
    ).first()
    return {
        "row_count": int(row["row_count"]),
        "total_flights": int(row["total_flights"]),
        "min_date": row["min_date"],
        "max_date": row["max_date"],
        "delay_per_flight": clean_number(row["delay_per_flight"]),
        "weather_delay_per_flight": clean_number(row["weather_delay_per_flight"]),
    }


def query_correlations(features):
    rows = []
    for feature, feature_label in WEATHER_FEATURES:
        for target, target_label in TARGETS:
            source = features.where(
                F.col(feature).isNotNull()
                & F.col(target).isNotNull()
                & F.col("total_flights").isNotNull()
                & (F.col("total_flights") > 0)
            )
            row = source.agg(
                F.count("*").alias("pair_count"),
                F.corr(feature, target).alias("pearson_correlation"),
                weighted_corr_expr(feature, target).alias("flight_weighted_pearson_correlation"),
            ).first()
            rows.append(
                {
                    "feature": feature,
                    "feature_label": feature_label,
                    "target": target,
                    "target_label": target_label,
                    "pair_count": int(row["pair_count"]),
                    "pearson_correlation": clean_number(row["pearson_correlation"]),
                    "flight_weighted_pearson_correlation": clean_number(row["flight_weighted_pearson_correlation"]),
                }
            )
    return rows


def query_monthly_delays(features):
    rows = (
        features.withColumn("month", F.date_format("date", "yyyy-MM"))
        .groupBy("month")
        .agg(
            (F.sum("total_delay") / F.sum("total_flights")).alias("delay_per_flight"),
            (F.sum("weather_delay") / F.sum("total_flights")).alias("weather_delay_per_flight"),
        )
        .orderBy("month")
        .collect()
    )
    return [
        {
            "month": row["month"],
            "delay_per_flight": clean_number(row["delay_per_flight"]),
            "weather_delay_per_flight": clean_number(row["weather_delay_per_flight"]),
        }
        for row in rows
    ]


def query_correlation_matrix(features):
    rows = []
    for row_name, row_label in HEATMAP_COLUMNS:
        for column_name, column_label in HEATMAP_COLUMNS:
            source = features.where(
                F.col(row_name).isNotNull()
                & F.col(column_name).isNotNull()
                & F.col("total_flights").isNotNull()
                & (F.col("total_flights") > 0)
            )
            value = source.agg(weighted_corr_expr(row_name, column_name).alias("correlation")).first()["correlation"]
            rows.append(
                {
                    "row": row_name,
                    "row_label": row_label,
                    "column": column_name,
                    "column_label": column_label,
                    "correlation": clean_number(value),
                }
            )
    return rows


def short_label(label):
    return label.replace("Srednia ", "").replace("Srednie ", "").replace("Suma ", "")


def generate_charts(output, correlations, monthly_delays, correlation_matrix):
    charts = output / "charts"
    charts.mkdir(parents=True, exist_ok=True)

    total_correlations = [row for row in correlations if row["target"] == "delay_per_flight"]
    labels = [short_label(row["feature_label"]) for row in total_correlations]
    values = [row["flight_weighted_pearson_correlation"] or 0.0 for row in total_correlations]
    colors = ["#2f80ed" if value >= 0 else "#eb5757" for value in values]

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels, values, color=colors)
    ax.axvline(0, color="#333333", linewidth=0.8)
    ax.set_title("Korelacja pogody z calkowitym opoznieniem")
    ax.set_xlabel("Korelacja Pearsona wazona liczba lotow")
    if values:
        min_value = min(values)
        max_value = max(values)
        if min_value == max_value:
            ax.set_xlim(min_value - 0.1, max_value + 0.1)
        else:
            ax.set_xlim(min_value * 1.35 if min_value < 0 else min_value * 0.8, max_value * 1.2 if max_value > 0 else max_value * 0.8)
    ax.bar_label(bars, labels=[f"{value:.3f}" for value in values], padding=4)
    fig.tight_layout()
    fig.savefig(charts / "01_korelacje_pogoda_opoznienie.png", dpi=180)
    plt.close(fig)

    month_labels = [row["month"] for row in monthly_delays]
    total_values = [row["delay_per_flight"] for row in monthly_delays]
    weather_values = [row["weather_delay_per_flight"] for row in monthly_delays]
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(month_labels, total_values, marker="o", label="Calkowite opoznienie")
    ax.plot(month_labels, weather_values, marker="o", label="Komponent pogodowy")
    ax.set_title("Miesieczny przebieg opoznien")
    ax.set_xlabel("Miesiac")
    ax.set_ylabel("Opoznienie [min/lot]")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(charts / "03_opoznienia_miesieczne.png", dpi=180)
    plt.close(fig)

    heatmap_labels = [
        "temperatura",
        "wilgotnosc",
        "opady",
        "deszcz",
        "snieg",
        "zachmurzenie",
        "predkosc wiatru",
        "porywy wiatru",
        "opoznienie calkowite",
        "opoznienie pogodowe",
    ]
    matrix_lookup = {(row["row"], row["column"]): row["correlation"] or 0.0 for row in correlation_matrix}
    matrix = [
        [matrix_lookup[(row_name, column_name)] for column_name, _ in HEATMAP_COLUMNS]
        for row_name, _ in HEATMAP_COLUMNS
    ]

    fig, ax = plt.subplots(figsize=(12, 10))
    image = ax.imshow(matrix, cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(range(len(heatmap_labels)), labels=heatmap_labels, rotation=45, ha="right")
    ax.set_yticks(range(len(heatmap_labels)), labels=heatmap_labels)
    ax.set_title("Macierz korelacji cech pogody i opoznien")
    for i, row_values in enumerate(matrix):
        for j, value in enumerate(row_values):
            ax.text(
                j,
                i,
                f"{value:.2f}",
                ha="center",
                va="center",
                color="white" if abs(value) > 0.55 else "#222222",
                fontsize=8,
            )
    fig.colorbar(image, ax=ax, label="Korelacja Pearsona wazona liczba lotow")
    fig.tight_layout()
    fig.savefig(charts / "04_heatmapa_korelacji.png", dpi=180)
    plt.close(fig)


def main():
    args = parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"START: czytam dane z HDFS: {args.input}")
        raw = spark.read.parquet(args.input)
        features, _ = prepare_features(raw)
        features.cache()
        print(f"Wiersze po filtrze total_flights > 0: {features.count()}")

        summary = query_summary(features)
        correlations = query_correlations(features)
        monthly_delays = query_monthly_delays(features)
        correlation_matrix = query_correlation_matrix(features)

        write_csv(output / "ogolna_korelacja_pogody.csv", correlations)
        write_csv(output / "monthly_delays.csv", monthly_delays)
        write_csv(output / "correlation_matrix.csv", correlation_matrix)
        (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

        generate_charts(output, correlations, monthly_delays, correlation_matrix)

        print("Zapisano wyniki:")
        print(output)
        print(output / "charts" / "01_korelacje_pogoda_opoznienie.png")
        print(output / "charts" / "03_opoznienia_miesieczne.png")
        print(output / "charts" / "04_heatmapa_korelacji.png")
        print("END")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
