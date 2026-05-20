from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, dayofweek, dayofyear, month
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression


spark = SparkSession.builder.appName("Delay Linear Model Training").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"
FEATURES_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/features/daily_features_parquet"
MODEL_OUTPUT = HDFS_BASE + "/bigdata/flight_delay/models/linear_delay_model"

TARGET_COL = "delay_per_flight"
DATE_COL = "date"


def pick_feature_columns(columns):
    # Pomijamy kolumny, które bezpośrednio zdradzają wynik albo są identyfikatorem czasu.
    excluded = {
        DATE_COL,
        TARGET_COL,
        "total_delay",
        "other_delay",
        "ENTITY_TYPE",
    }
    return [name for name in columns if name not in excluded]


def separate_numeric_categorical(features):
    numeric = [f for f in features if f != "ENTITY_NAME"]
    categorical = [f for f in features if f == "ENTITY_NAME"]
    return numeric, categorical


def split_by_date(frame):
    ordered_dates = [row[0] for row in frame.select(DATE_COL).distinct().orderBy(DATE_COL).collect()]
    if len(ordered_dates) < 5:
        # Dla bardzo małej próbki wracamy do losowego podziału.
        return frame.randomSplit([0.8, 0.2], seed=42)

    cutoff_index = max(1, int(len(ordered_dates) * 0.8))
    cutoff_date = ordered_dates[cutoff_index]
    train = frame.filter(col(DATE_COL) < cutoff_date)
    test = frame.filter(col(DATE_COL) >= cutoff_date)
    if train.rdd.isEmpty() or test.rdd.isEmpty():
        return frame.randomSplit([0.8, 0.2], seed=42)
    return train, test


print("START: Trening modelu liniowego")
print(f"Features: {FEATURES_PATH}")

data = spark.read.parquet(FEATURES_PATH)

if TARGET_COL not in data.columns:
    raise RuntimeError(f"Missing target column: {TARGET_COL}")
if DATE_COL not in data.columns:
    raise RuntimeError(f"Missing date column: {DATE_COL}")

# Dodajemy proste cechy kalendarzowe, żeby model widział sezonowość i dzień tygodnia.
data = data.withColumn("day_of_week", dayofweek(col(DATE_COL)))
data = data.withColumn("month_of_year", month(col(DATE_COL)))
data = data.withColumn("day_of_month", dayofmonth(col(DATE_COL)))
data = data.withColumn("day_of_year", dayofyear(col(DATE_COL)))

feature_columns = pick_feature_columns(data.columns)
feature_columns = [name for name in feature_columns if name != TARGET_COL]

if not feature_columns:
    raise RuntimeError("No feature columns available for training")

training_frame = data.select([DATE_COL, TARGET_COL, *feature_columns]).na.drop(subset=[TARGET_COL])
train, test = split_by_date(training_frame)

print(f"Training rows: {train.count()}")
print(f"Test rows: {test.count()}")
print("Features used:")
for name in feature_columns:
    print(f" - {name}")

numeric_features, categorical_features = separate_numeric_categorical(feature_columns)

# Etapy pipeline'u
stages = []

# Kodowanie kategoryczne dla ENTITY_NAME
if "ENTITY_NAME" in feature_columns:
    string_indexer = StringIndexer(inputCol="ENTITY_NAME", outputCol="ENTITY_NAME_indexed", handleInvalid="skip")
    stages.append(string_indexer)
    categorical_encoded = ["ENTITY_NAME_indexed"]
else:
    categorical_encoded = []

# Imputation dla cech numerycznych
if numeric_features:
    imputer = Imputer(inputCols=numeric_features, outputCols=[f"{name}_imputed" for name in numeric_features])
    stages.append(imputer)
    numeric_imputed = [f"{name}_imputed" for name in numeric_features]
else:
    numeric_imputed = []

# Łączenie wszystkich cech w wektor
assembler_inputs = numeric_imputed + categorical_encoded
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(assembler)

# Linear regression
lr = LinearRegression(featuresCol="features", labelCol=TARGET_COL, predictionCol="prediction")
stages.append(lr)

pipeline = Pipeline(stages=stages)
model = pipeline.fit(train)

predictions = model.transform(test)

rmse = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse").evaluate(predictions)
mae = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="mae").evaluate(predictions)
r2 = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction", metricName="r2").evaluate(predictions)

print("Metrics:")
print(f"RMSE: {rmse}")
print(f"MAE: {mae}")
print(f"R2: {r2}")

# Zapisujemy model, żeby można go było potem użyć do predykcji na nowych danych.
model.write().overwrite().save(MODEL_OUTPUT)

print(f"Saved model to: {MODEL_OUTPUT}")
print("Sample predictions:")
predictions.select(DATE_COL, TARGET_COL, "prediction").show(20, truncate=False)
print("END: Trening modelu liniowego")

spark.stop()