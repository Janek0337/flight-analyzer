import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def build_spark_session(app_name="Flight Delay Regression"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark, path):
    return spark.read.parquet(path)


def train_regression_model(data, feature_cols, label_col="delay_per_flight"):
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    lr = LinearRegression(
        featuresCol="features",
        labelCol=label_col,
        predictionCol="prediction",
        maxIter=50,
        regParam=0.1,
        elasticNetParam=0.0
    )

    pipeline = Pipeline(stages=[assembler, lr])
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)

    evaluator_rmse = RegressionEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="rmse"
    )
    evaluator_r2 = RegressionEvaluator(
        labelCol=label_col,
        predictionCol="prediction",
        metricName="r2"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    return model, predictions, rmse, r2


def main():
    parser = argparse.ArgumentParser(description="Train a Spark regression model on flight delay data")
    parser.add_argument(
        "--input",
        default="/bigdata/flight_delay/processed/daily_delays",
        help="Path to the daily delays Parquet data"
    )
    parser.add_argument(
        "--output",
        default="/bigdata/flight_delay/models/delay_regression",
        help="Path to save the trained model"
    )
    parser.add_argument(
        "--label",
        default="delay_per_flight",
        help="Label column to predict"
    )
    args = parser.parse_args()

    spark = build_spark_session()
    data = load_data(spark, args.input)

    feature_cols = [
        "total_flights",
        "weather_delay",
        "industrial_delay",
        "atc_delay",
        "delayed_flights",
        "delayed_flights_15"
    ]

    data = data.select(*(feature_cols + [args.label])).na.drop()

    model, predictions, rmse, r2 = train_regression_model(data, feature_cols, args.label)

    lr_model = model.stages[-1]
    print("Trained Linear Regression model")
    print(f"Features: {feature_cols}")
    print(f"Intercept: {lr_model.intercept}")
    print(f"Coefficients: {lr_model.coefficients}")
    print(f"Test RMSE: {rmse:.4f}")
    print(f"Test R2: {r2:.4f}")

    predictions.select("prediction", args.label).show(20, truncate=False)
    model.write().overwrite().save(args.output)
    print(f"Saved model to {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
