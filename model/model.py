import argparse
import math
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as spark_avg
from pyspark.sql.functions import col, cos, dayofmonth, dayofweek, dayofyear, lag, lit, month, sin, when
from pyspark.sql.types import NumericType


HDFS_BASE = "hdfs://nn1:9000"
DEFAULT_INPUT = HDFS_BASE + "/bigdata/flight_delay/processed/features/daily_features_parquet"
DEFAULT_OUTPUT = HDFS_BASE + "/bigdata/flight_delay/models/local_weather_delay_regression"
DEFAULT_METRICS_OUTPUT = DEFAULT_OUTPUT + "_metrics"
TARGET_COL = "delay_per_flight"
DATE_COL = "date"


def parse_args():
    parser = argparse.ArgumentParser(description="Train delay regression model")
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--metrics-output", default=DEFAULT_METRICS_OUTPUT)
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
        "weather_station_name",
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


def add_training_features(data, label_col):
    data = data.withColumn("day_of_week", dayofweek(col(DATE_COL)))
    data = data.withColumn("month_of_year", month(col(DATE_COL)))
    data = data.withColumn("day_of_month", dayofmonth(col(DATE_COL)))
    data = data.withColumn("day_of_year", dayofyear(col(DATE_COL)))

    data = data.withColumn("day_of_week_sin", sin(2 * lit(math.pi) * col("day_of_week") / lit(7.0)))
    data = data.withColumn("day_of_week_cos", cos(2 * lit(math.pi) * col("day_of_week") / lit(7.0)))
    data = data.withColumn("day_of_year_sin", sin(2 * lit(math.pi) * col("day_of_year") / lit(365.25)))
    data = data.withColumn("day_of_year_cos", cos(2 * lit(math.pi) * col("day_of_year") / lit(365.25)))

    if "ENTITY_NAME" in data.columns:
        entity_window = Window.partitionBy("ENTITY_NAME").orderBy(DATE_COL)
        rolling_window_7d = entity_window.rowsBetween(-7, -1)
        rolling_window_14d = entity_window.rowsBetween(-14, -1)

        data = data.withColumn("previous_delay_per_flight", lag(col(label_col), 1).over(entity_window))
        data = data.withColumn("rolling_7d_delay_per_flight", spark_avg(col(label_col)).over(rolling_window_7d))
        data = data.withColumn("rolling_14d_delay_per_flight", spark_avg(col(label_col)).over(rolling_window_14d))
        if "total_flights" in data.columns:
            data = data.withColumn("previous_total_flights", lag(col("total_flights"), 1).over(entity_window))

    if "feature_precipitation" in data.columns:
        data = data.withColumn("is_precipitation_day", when(col("feature_precipitation") > 0, 1.0).otherwise(0.0))
    if "feature_rain" in data.columns:
        data = data.withColumn("is_rainy_day", when(col("feature_rain") > 0, 1.0).otherwise(0.0))
    if "feature_snowfall" in data.columns:
        data = data.withColumn("is_snowy_day", when(col("feature_snowfall") > 0, 1.0).otherwise(0.0))
    if "feature_wind_speed" in data.columns:
        data = data.withColumn("is_windy_day", when(col("feature_wind_speed") >= 30, 1.0).otherwise(0.0))
    if "feature_temperature" in data.columns:
        data = data.withColumn("temperature_below_zero", when(col("feature_temperature") < 0, 1.0).otherwise(0.0))

    if "feature_rain" in data.columns and "feature_wind_speed" in data.columns:
        data = data.withColumn("rain_x_wind_speed", col("feature_rain") * col("feature_wind_speed"))
    if "feature_snowfall" in data.columns and "feature_temperature" in data.columns:
        data = data.withColumn("snow_x_below_zero", col("feature_snowfall") * col("temperature_below_zero"))

    return data


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

    encoded_columns = []
    if indexed_columns:
        encoded_columns = [f"{name}_encoded" for name in text_columns]
        stages.append(OneHotEncoder(inputCols=indexed_columns, outputCols=encoded_columns, handleInvalid="keep"))

    imputed_columns = []
    if numeric_columns:
        imputed_columns = [f"{name}_imputed" for name in numeric_columns]
        stages.append(Imputer(inputCols=numeric_columns, outputCols=imputed_columns))

    stages.append(VectorAssembler(inputCols=imputed_columns + encoded_columns, outputCol="features"))
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
    mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2").evaluate(predictions)
    return rmse, mae, r2


def save_metrics(spark, metrics, output_path):
    spark.createDataFrame([metrics]).coalesce(1).write.mode("overwrite").json(output_path)


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

    data = add_training_features(data, args.label)

    feature_columns = get_feature_columns(data.columns, args.label)
    if not feature_columns:
        raise RuntimeError("No feature columns available for training")

    model_data = data.select(DATE_COL, args.label, *feature_columns).na.drop(subset=[args.label])
    train, test = split_by_date(model_data)

    train_rows = train.count()
    test_rows = test.count()

    print(f"Training rows: {train_rows}")
    print(f"Test rows: {test_rows}")
    print("Features used:")
    for name in feature_columns:
        print(f" - {name}")

    pipeline = build_pipeline(model_data, feature_columns, args.label)
    model = pipeline.fit(train)
    predictions = model.transform(test)

    rmse, mae, r2 = evaluate(predictions, args.label)
    baseline_prediction = train.select(spark_avg(col(args.label))).first()[0]
    baseline_predictions = test.withColumn("prediction", lit(baseline_prediction))
    baseline_rmse, baseline_mae, baseline_r2 = evaluate(baseline_predictions, args.label)

    print("Metrics:")
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R2: {r2:.4f}")
    print("Baseline metrics (train mean prediction):")
    print(f"Baseline RMSE: {baseline_rmse:.4f}")
    print(f"Baseline MAE: {baseline_mae:.4f}")
    print(f"Baseline R2: {baseline_r2:.4f}")

    metrics = {
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": args.input,
        "model_output_path": args.output,
        "label": args.label,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "feature_count": len(feature_columns),
        "features": feature_columns,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "baseline_prediction": baseline_prediction,
        "baseline_rmse": baseline_rmse,
        "baseline_mae": baseline_mae,
        "baseline_r2": baseline_r2,
    }
    save_metrics(spark, metrics, args.metrics_output)
    print(f"Saved metrics to: {args.metrics_output}")

    print("Sample predictions:")
    predictions.select(DATE_COL, "prediction", args.label).show(20, truncate=False)

    model.write().overwrite().save(args.output)
    print(f"Saved model to: {args.output}")
    print("END: Trening modelu opoznien")

    spark.stop()


if __name__ == "__main__":
    main()
