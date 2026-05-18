from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, to_date, when, year, sum as spark_sum


spark = SparkSession.builder.appName("Flight Delay Processing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"
FLIGHTS_2025_PATH = f"{HDFS_BASE}/bigdata/flight_delay/raw/flights/flights_2025.csv"
FLIGHTS_2026_PATH = f"{HDFS_BASE}/bigdata/flight_delay/raw/flights/flights_2026.csv"
OUTPUT_DAILY_PATH = f"{HDFS_BASE}/bigdata/flight_delay/processed/daily_delays"
OUTPUT_MONTHLY_PATH = f"{HDFS_BASE}/bigdata/flight_delay/processed/monthly_delays"
OUTPUT_ENTITY_PATH = f"{HDFS_BASE}/bigdata/flight_delay/processed/entity_delays"

delay_cols = [
    "DLY_ERT_1", "DLY_ERT_A_1", "DLY_ERT_C_1", "DLY_ERT_D_1", "DLY_ERT_E_1",
    "DLY_ERT_G_1", "DLY_ERT_I_1", "DLY_ERT_M_1", "DLY_ERT_N_1", "DLY_ERT_O_1",
    "DLY_ERT_P_1", "DLY_ERT_R_1", "DLY_ERT_S_1", "DLY_ERT_T_1", "DLY_ERT_V_1",
    "DLY_ERT_W_1", "DLY_ERT_NA_1", "FLT_ERT_1_DLY", "FLT_ERT_1_DLY_15",
]

print("START: Przetwarzanie opoznien lotow")
print(f"Input: {FLIGHTS_2025_PATH}")
print(f"Input: {FLIGHTS_2026_PATH}")

flights = spark.read.option("header", True).option("inferSchema", True).csv(FLIGHTS_2025_PATH).unionByName(
    spark.read.option("header", True).option("inferSchema", True).csv(FLIGHTS_2026_PATH)
)

# Braki w kolumnach opóźnień zamieniamy na zero, żeby agregacje nie psuły wyników.
flights = flights.fillna(0, subset=delay_cols).withColumn("date", to_date(col("FLT_DATE"))).withColumn(
    "weather_delay", col("DLY_ERT_W_1") + col("DLY_ERT_D_1")
).withColumn(
    "industrial_delay", col("DLY_ERT_I_1") + col("DLY_ERT_N_1")
).withColumn(
    "atc_delay", col("DLY_ERT_C_1") + col("DLY_ERT_S_1") + col("DLY_ERT_T_1") + col("DLY_ERT_R_1")
).withColumn(
    "delay_per_flight", when(col("FLT_ERT_1") > 0, col("DLY_ERT_1") / col("FLT_ERT_1")).otherwise(0)
)

# Trzy widoki: dzienny, miesięczny i po podmiocie.
daily_delays = flights.groupBy("date").agg(
    spark_sum("FLT_ERT_1").alias("total_flights"),
    spark_sum("DLY_ERT_1").alias("total_delay"),
    spark_sum("weather_delay").alias("weather_delay"),
    spark_sum("industrial_delay").alias("industrial_delay"),
    spark_sum("atc_delay").alias("atc_delay"),
    spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
    spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15"),
).withColumn(
    "delay_per_flight", when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
).orderBy("date")
# Obliczamy "other_delay" jako różnicę między total_delay a sumą zmapowanych komponentów.
daily_delays = daily_delays.withColumn(
    "other_delay",
    col("total_delay") - (col("weather_delay") + col("industrial_delay") + col("atc_delay")),
)

monthly_delays = flights.groupBy(year("FLT_DATE").alias("year"), month("FLT_DATE").alias("month")).agg(
    spark_sum("FLT_ERT_1").alias("total_flights"),
    spark_sum("DLY_ERT_1").alias("total_delay"),
    spark_sum("weather_delay").alias("weather_delay"),
    spark_sum("industrial_delay").alias("industrial_delay"),
    spark_sum("atc_delay").alias("atc_delay"),
    spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
    spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15"),
).withColumn(
    "delay_per_flight", when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
).orderBy("year", "month")
monthly_delays = monthly_delays.withColumn(
    "other_delay",
    col("total_delay") - (col("weather_delay") + col("industrial_delay") + col("atc_delay")),
)

entity_delays = flights.groupBy("ENTITY_NAME", "ENTITY_TYPE").agg(
    spark_sum("FLT_ERT_1").alias("total_flights"),
    spark_sum("DLY_ERT_1").alias("total_delay"),
    spark_sum("weather_delay").alias("weather_delay"),
    spark_sum("industrial_delay").alias("industrial_delay"),
    spark_sum("atc_delay").alias("atc_delay"),
    spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
    spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15"),
).withColumn(
    "delay_per_flight", when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
).orderBy(col("total_delay").desc())
entity_delays = entity_delays.withColumn(
    "other_delay",
    col("total_delay") - (col("weather_delay") + col("industrial_delay") + col("atc_delay")),
)

daily_delays.write.mode("overwrite").parquet(OUTPUT_DAILY_PATH)
monthly_delays.write.mode("overwrite").parquet(OUTPUT_MONTHLY_PATH)
entity_delays.write.mode("overwrite").parquet(OUTPUT_ENTITY_PATH)

print("Saved processed datasets:")
print(OUTPUT_DAILY_PATH)
print(OUTPUT_MONTHLY_PATH)
print(OUTPUT_ENTITY_PATH)
print("END: Przetwarzanie opoznien lotow")

spark.stop()