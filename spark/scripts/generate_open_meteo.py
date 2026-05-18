from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("Upload Weather Raw CSV To HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("START: Upload Weather Raw CSV To HDFS")


# ============================================================
# 2. Paths
# ============================================================

# Lokalny plik CSV, który wygenerowałeś skryptem z Open-Meteo
LOCAL_WEATHER_CSV = "file:///home/hadoop/skrypty/weather_hourly_2025_01_01_to_2026_03_31_8cities.csv"

# Docelowy katalog RAW w HDFS
HDFS_BASE = "hdfs://nn1:9000"

OUTPUT_RAW_WEATHER_PATH = HDFS_BASE + "/bigdata/flight_delay/raw/weather/weather_hourly_2025_01_01_to_2026_03_31_8cities_spark"


# ============================================================
# 3. Read local CSV
# ============================================================

weather_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(LOCAL_WEATHER_CSV)

print("SCHEMA RAW WEATHER:")
weather_raw.printSchema()

print("SAMPLE RAW WEATHER:")
weather_raw.show(10, truncate=False)


# ============================================================
# 4. Basic validation
# ============================================================

print("LICZBA REKORDÓW:")
print(weather_raw.count())

print("LOTNISKA:")
weather_raw.select("icao", "airport").distinct().show(50, truncate=False)

print("ZAKRES DAT:")
weather_raw.selectExpr("min(time) as start_time", "max(time) as end_time").show(truncate=False)


# ============================================================
# 5. Optional time conversion
# ============================================================

weather_raw_clean = weather_raw \
    .withColumn("time", to_timestamp(col("time")))


# ============================================================
# 6. Save to HDFS RAW as CSV
# ============================================================

weather_raw_clean.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_RAW_WEATHER_PATH)

print("Saved raw weather CSV to HDFS:")
print(OUTPUT_RAW_WEATHER_PATH)


# ============================================================
# 7. Save also as Parquet RAW, optional but useful
# ============================================================

OUTPUT_RAW_WEATHER_PARQUET_PATH = HDFS_BASE + "/bigdata/flight_delay/raw/weather/weather_hourly_2025_01_01_to_2026_03_31_8cities_parquet"

weather_raw_clean.write \
    .mode("overwrite") \
    .parquet(OUTPUT_RAW_WEATHER_PARQUET_PATH)

print("Saved raw weather Parquet to HDFS:")
print(OUTPUT_RAW_WEATHER_PARQUET_PATH)


print("END: Upload Weather Raw CSV To HDFS")

spark.stop()