from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectFlightDelayData") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

paths = {
    "airports": "hdfs://nn1:9000/bigdata/flight_delay/raw/airports/airports.csv",
    "flights_2025": "hdfs://nn1:9000/bigdata/flight_delay/raw/flights/flights_2025.csv",
    "flights_2026": "hdfs://nn1:9000/bigdata/flight_delay/raw/flights/flights_2026.csv",
    "weather": "hdfs://nn1:9000/bigdata/flight_delay/raw/weather/weather_hourly_6m_8cities.csv"
}




for name, path in paths.items():
    print("\n" + "=" * 80)
    print(f"DATASET: {name}")
    print("=" * 80)

    df = spark.read.option("header", True).option("inferSchema", True).csv(path)

    print("LICZBA REKORDÓW:", df.count())
    print("SCHEMA:")
    df.printSchema()

    print("PRZYKŁADOWE WIERSZE:")
    df.show(5, truncate=False)

spark.stop()