import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, dayofweek, dayofyear, month
from pyspark.sql.types import NumericType


HDFS_BASE = "hdfs://nn1:9000"
DEFAULT_INPUT = HDFS_BASE + "/bigdata/flight_delay/processed/features/daily_features_parquet"
DEFAULT_OUTPUT = HDFS_BASE + "/bigdata/flight_delay/models/local_weather_delay_regression"
TARGET_COL = "delay_per_flight"
DATE_COL = "date"


def parse_args():
    parser = argparse.ArgumentParser(description="Train delay regression model")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--label", default=TARGET_COL)
    return parser.parse_args()


def split_by_date(data):
    dates = [row[0] for row in data.select(DATE_COL).distinct().orderBy(DATE_COL).collect()]
    if len(dates) < 5:
        return data.randomSplit([0.8, 0.2], seed=42)

    cutoff_date = dates[max(1, int(len(dates) * 0.8))]
    train = data.filter(col(DATE_COL) < cutoff_date)
    test = data.filter(col(DATE_COL) >= cutoff_date)

    if train.rdd.isEmpty() or test.rdd.isEmpty():
        return data.randomSplit([0.8, 0.2], seed=42)
    return train, test


def get_feature_columns(columns, label_col):
    ignored = {
        DATE_COL,
        label_col,
        "total_delay",
        "other_delay",
        "weather_delay",
        "industrial_delay",
        "atc_delay",
        "delayed_flights",
        "delayed_flights_15",
        "avg_temperature",
        "avg_humidity",
        "avg_dew_point",
        "total_precipitation",
        "total_rain",
        "total_snowfall",
        "avg_cloud_cover",
        "avg_pressure_msl",
        "avg_surface_pressure",
        "avg_wind_speed",
        "max_wind_gusts",
        "min_temperature",
        "max_temperature",
        "avg_temperature_europe",
        "avg_humidity_europe",
        "avg_precipitation_europe",
        "avg_rain_europe",
        "avg_snowfall_europe",
        "avg_cloud_cover_europe",
        "avg_wind_speed_europe",
        "max_wind_gusts_europe",
    }
    return [name for name in columns if name not in ignored]


def build_pipeline(data, feature_columns, label_col):
    numeric_columns = [
        field.name
        for field in data.schema.fields
        if field.name in feature_columns and isinstance(field.dataType, NumericType)
    ]
    text_columns = [name for name in feature_columns if name not in numeric_columns]

    stages = []
    indexed_columns = []

    for name in text_columns:
        indexed_name = f"{name}_indexed"
        stages.append(StringIndexer(inputCol=name, outputCol=indexed_name, handleInvalid="keep"))
        indexed_columns.append(indexed_name)

    imputed_columns = []
    if numeric_columns:
        imputed_columns = [f"{name}_imputed" for name in numeric_columns]
        stages.append(Imputer(inputCols=numeric_columns, outputCols=imputed_columns))

    stages.append(VectorAssembler(inputCols=imputed_columns + indexed_columns, outputCol="features"))
    stages.append(
        LinearRegression(
            featuresCol="features",
            labelCol=label_col,
            predictionCol="prediction",
            maxIter=50,
            regParam=0.1,
            elasticNetParam=0.0,
        )
    )
    return Pipeline(stages=stages)


def evaluate(predictions, label_col):
    rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2").evaluate(predictions)
    return rmse, r2


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("Flight Delay Model").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("START: Trening modelu opoznien")
    print(f"Input: {args.input}")

    data = spark.read.parquet(args.input)

    if args.label not in data.columns:
        raise RuntimeError(f"Missing label column: {args.label}")
    if DATE_COL not in data.columns:
        raise RuntimeError(f"Missing date column: {DATE_COL}")

    data = data.withColumn("day_of_week", dayofweek(col(DATE_COL)))
    data = data.withColumn("month_of_year", month(col(DATE_COL)))
    data = data.withColumn("day_of_month", dayofmonth(col(DATE_COL)))
    data = data.withColumn("day_of_year", dayofyear(col(DATE_COL)))

    feature_columns = get_feature_columns(data.columns, args.label)
    if not feature_columns:
        raise RuntimeError("No feature columns available for training")

    model_data = data.select(DATE_COL, args.label, *feature_columns).na.drop(subset=[args.label])
    train, test = split_by_date(model_data)

    print(f"Training rows: {train.count()}")
    print(f"Test rows: {test.count()}")
    print("Features used:")
    for name in feature_columns:
        print(f" - {name}")

    pipeline = build_pipeline(model_data, feature_columns, args.label)
    model = pipeline.fit(train)
    predictions = model.transform(test)

    rmse, r2 = evaluate(predictions, args.label)

    print("Metrics:")
    print(f"RMSE: {rmse:.4f}")
    print(f"R2: {r2:.4f}")

    print("Sample predictions:")
    predictions.select(DATE_COL, "prediction", args.label).show(20, truncate=False)

    model.write().overwrite().save(args.output)
    print(f"Saved model to: {args.output}")
    print("END: Trening modelu opoznien")

    spark.stop()


if __name__ == "__main__":
    main()
