from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

spark = SparkSession.builder \
    .appName("Read Processed Flight Delay Results") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"

daily = spark.read.parquet(HDFS_BASE + "/bigdata/flight_delay/processed/daily_delays")
monthly = spark.read.parquet(HDFS_BASE + "/bigdata/flight_delay/processed/monthly_delays")
entity = spark.read.parquet(HDFS_BASE + "/bigdata/flight_delay/processed/entity_delays")

print("=== DAILY DELAYS ===")
daily.orderBy("date").show(20, truncate=False)

print("=== TOP 20 DAYS BY TOTAL DELAY ===")
daily.orderBy(desc("total_delay")).show(20, truncate=False)

print("=== TOP 20 DAYS BY WEATHER DELAY ===")
daily.orderBy(desc("weather_delay")).show(20, truncate=False)

print("=== TOP 20 DAYS BY INDUSTRIAL DELAY ===")
daily.orderBy(desc("industrial_delay")).show(20, truncate=False)

print("=== MONTHLY DELAYS ===")
monthly.orderBy("year", "month").show(50, truncate=False)

print("=== TOP ENTITIES BY TOTAL DELAY ===")
entity.orderBy(desc("total_delay")).show(20, truncate=False)

spark.stop()