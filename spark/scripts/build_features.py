from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Join Flights and Weather Features") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"
FLIGHTS_DAILY = HDFS_BASE + "/bigdata/flight_delay/processed/daily_delays"
WEATHER_EUROPE_DAILY = HDFS_BASE + "/bigdata/flight_delay/processed/weather_europe_daily"
OUTPUT_FEATURES = HDFS_BASE + "/bigdata/flight_delay/processed/features/daily_features_parquet"

print("START: Budowa features")
print(f"Flights: {FLIGHTS_DAILY}")
print(f"Weather: {WEATHER_EUROPE_DAILY}")

flights = spark.read.parquet(FLIGHTS_DAILY)
weather = spark.read.parquet(WEATHER_EUROPE_DAILY)

# Obie tabele muszą mieć kolumnę date, żeby dało się je połączyć.
if "date" not in flights.columns or "date" not in weather.columns:
    raise RuntimeError("Both inputs must contain 'date' column to join on")

# Zostawiamy wszystkie dni z tabeli lotów i dokładamy pogodę, jeśli istnieje.
features = flights.join(weather, "date", "left")

print("Sample features:")
features.show(20, truncate=False)

features.write.mode("overwrite").parquet(OUTPUT_FEATURES)

print("Saved features to:")
print(OUTPUT_FEATURES)
print("END: Budowa features")

spark.stop()
