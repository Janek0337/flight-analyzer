from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    avg,
    sum as spark_sum,
    max as spark_max,
    min as spark_min
)

spark = SparkSession.builder \
    .appName("Weather Daily Processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("START: Weather Daily Processing")

HDFS_BASE = "hdfs://nn1:9000"

WEATHER_PATH = HDFS_BASE + "/bigdata/flight_delay/raw/weather/weather_hourly_6m_8cities.csv"
OUTPUT_WEATHER_DAILY_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/weather_daily"
OUTPUT_WEATHER_EUROPE_DAILY_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/weather_europe_daily"

weather = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(WEATHER_PATH)

print("SCHEMA weather:")
weather.printSchema()

print("Sample weather:")
weather.show(10, truncate=False)

weather_daily = weather \
    .withColumn("date", to_date(col("time"))) \
    .groupBy("icao", "airport", "date") \
    .agg(
        avg("temperature_2m").alias("avg_temperature"),
        avg("relative_humidity_2m").alias("avg_humidity"),
        avg("dew_point_2m").alias("avg_dew_point"),
        spark_sum("precipitation").alias("total_precipitation"),
        spark_sum("rain").alias("total_rain"),
        spark_sum("snowfall").alias("total_snowfall"),
        avg("cloud_cover").alias("avg_cloud_cover"),
        avg("pressure_msl").alias("avg_pressure_msl"),
        avg("surface_pressure").alias("avg_surface_pressure"),
        avg("wind_speed_10m").alias("avg_wind_speed"),
        spark_max("wind_gusts_10m").alias("max_wind_gusts"),
        spark_min("temperature_2m").alias("min_temperature"),
        spark_max("temperature_2m").alias("max_temperature")
    ) \
    .orderBy("icao", "date")

print("=== WEATHER DAILY ===")
weather_daily.show(30, truncate=False)

weather_europe_daily = weather_daily \
    .groupBy("date") \
    .agg(
        avg("avg_temperature").alias("avg_temperature_europe"),
        avg("avg_humidity").alias("avg_humidity_europe"),
        avg("total_precipitation").alias("avg_precipitation_europe"),
        avg("total_rain").alias("avg_rain_europe"),
        avg("total_snowfall").alias("avg_snowfall_europe"),
        avg("avg_cloud_cover").alias("avg_cloud_cover_europe"),
        avg("avg_wind_speed").alias("avg_wind_speed_europe"),
        spark_max("max_wind_gusts").alias("max_wind_gusts_europe")
    ) \
    .orderBy("date")

print("=== WEATHER EUROPE DAILY ===")
weather_europe_daily.show(30, truncate=False)

weather_daily.write.mode("overwrite").parquet(OUTPUT_WEATHER_DAILY_PATH)
weather_europe_daily.write.mode("overwrite").parquet(OUTPUT_WEATHER_EUROPE_DAILY_PATH)

print("Saved:")
print(OUTPUT_WEATHER_DAILY_PATH)
print(OUTPUT_WEATHER_EUROPE_DAILY_PATH)

print("END: Weather Daily Processing")

spark.stop()