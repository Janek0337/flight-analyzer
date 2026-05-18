from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Weather Daily") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"

weather_daily = spark.read.parquet(
    HDFS_BASE + "/bigdata/flight_delay/processed/weather_daily"
)

print("SCHEMA:")
weather_daily.printSchema()

print("SAMPLE:")
weather_daily.show(30, truncate=False)

print("LICZBA REKORDÓW:")
print(weather_daily.count())

print("ZAKRES DAT:")
weather_daily.selectExpr("min(date) as start_date", "max(date) as end_date").show()

print("LOTNISKA:")
weather_daily.select("icao", "airport").distinct().show(50, truncate=False)

spark.stop()