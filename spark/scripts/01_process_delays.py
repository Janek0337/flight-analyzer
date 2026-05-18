from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, sum as spark_sum, year, month

# ============================================================
# 1. Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("Flight Delay Processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("START: Flight Delay Processing")


# ============================================================
# 2. Input / Output paths
# ============================================================


HDFS_BASE = "hdfs://nn1:9000"

FLIGHTS_2025_PATH = HDFS_BASE + "/bigdata/flight_delay/raw/flights/flights_2025.csv"
FLIGHTS_2026_PATH = HDFS_BASE + "/bigdata/flight_delay/raw/flights/flights_2026.csv"

OUTPUT_DAILY_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/daily_delays"
OUTPUT_MONTHLY_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/monthly_delays"
OUTPUT_ENTITY_PATH = HDFS_BASE + "/bigdata/flight_delay/processed/entity_delays"


# ============================================================
# 3. Read raw data
# ============================================================

flights_2025 = spark.read.option("header", True).option("inferSchema", True).csv(FLIGHTS_2025_PATH)
flights_2026 = spark.read.option("header", True).option("inferSchema", True).csv(FLIGHTS_2026_PATH)

print("SCHEMA flights_2025:")
flights_2025.printSchema()

print("SCHEMA flights_2026:")
flights_2026.printSchema()


# ============================================================
# 4. Union 2025 + 2026
# ============================================================

flights_all = flights_2025.unionByName(flights_2026)

print("Rows after union:", flights_all.count())


# ============================================================
# 5. Replace NULL values in delay columns with 0
# ============================================================

delay_cols = [
    "DLY_ERT_1",
    "DLY_ERT_A_1",
    "DLY_ERT_C_1",
    "DLY_ERT_D_1",
    "DLY_ERT_E_1",
    "DLY_ERT_G_1",
    "DLY_ERT_I_1",
    "DLY_ERT_M_1",
    "DLY_ERT_N_1",
    "DLY_ERT_O_1",
    "DLY_ERT_P_1",
    "DLY_ERT_R_1",
    "DLY_ERT_S_1",
    "DLY_ERT_T_1",
    "DLY_ERT_V_1",
    "DLY_ERT_W_1",
    "DLY_ERT_NA_1",
    "FLT_ERT_1_DLY",
    "FLT_ERT_1_DLY_15"
]

flights_clean = flights_all.fillna(0, subset=delay_cols)


# ============================================================
# 6. Create analytical columns
# ============================================================

flights_features = flights_clean \
    .withColumn("date", to_date(col("FLT_DATE"))) \
    .withColumn("weather_delay", col("DLY_ERT_W_1") + col("DLY_ERT_D_1")) \
    .withColumn("industrial_delay", col("DLY_ERT_I_1") + col("DLY_ERT_N_1")) \
    .withColumn(
        "atc_delay",
        col("DLY_ERT_C_1") +
        col("DLY_ERT_S_1") +
        col("DLY_ERT_T_1") +
        col("DLY_ERT_R_1")
    ) \
    .withColumn(
        "delay_per_flight",
        when(col("FLT_ERT_1") > 0, col("DLY_ERT_1") / col("FLT_ERT_1")).otherwise(0)
    )

print("Sample with new columns:")
flights_features.select(
    "date",
    "ENTITY_NAME",
    "FLT_ERT_1",
    "DLY_ERT_1",
    "weather_delay",
    "industrial_delay",
    "atc_delay",
    "delay_per_flight"
).show(20, truncate=False)


# ============================================================
# 7. Daily aggregation
# ============================================================

daily_delays = flights_features \
    .groupBy("date") \
    .agg(
        spark_sum("FLT_ERT_1").alias("total_flights"),
        spark_sum("DLY_ERT_1").alias("total_delay"),
        spark_sum("weather_delay").alias("weather_delay"),
        spark_sum("industrial_delay").alias("industrial_delay"),
        spark_sum("atc_delay").alias("atc_delay"),
        spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
        spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15")
    ) \
    .withColumn(
        "delay_per_flight",
        when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
    ) \
    .orderBy("date")

print("Daily delays:")
daily_delays.show(20, truncate=False)


# ============================================================
# 8. Monthly aggregation
# ============================================================

monthly_delays = flights_features \
    .groupBy(
        year("FLT_DATE").alias("year"),
        month("FLT_DATE").alias("month")
    ) \
    .agg(
        spark_sum("FLT_ERT_1").alias("total_flights"),
        spark_sum("DLY_ERT_1").alias("total_delay"),
        spark_sum("weather_delay").alias("weather_delay"),
        spark_sum("industrial_delay").alias("industrial_delay"),
        spark_sum("atc_delay").alias("atc_delay"),
        spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
        spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15")
    ) \
    .withColumn(
        "delay_per_flight",
        when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
    ) \
    .orderBy("year", "month")

print("Monthly delays:")
monthly_delays.show(50, truncate=False)


# ============================================================
# 9. Entity aggregation
# ============================================================

entity_delays = flights_features \
    .groupBy("ENTITY_NAME", "ENTITY_TYPE") \
    .agg(
        spark_sum("FLT_ERT_1").alias("total_flights"),
        spark_sum("DLY_ERT_1").alias("total_delay"),
        spark_sum("weather_delay").alias("weather_delay"),
        spark_sum("industrial_delay").alias("industrial_delay"),
        spark_sum("atc_delay").alias("atc_delay"),
        spark_sum("FLT_ERT_1_DLY").alias("delayed_flights"),
        spark_sum("FLT_ERT_1_DLY_15").alias("delayed_flights_15")
    ) \
    .withColumn(
        "delay_per_flight",
        when(col("total_flights") > 0, col("total_delay") / col("total_flights")).otherwise(0)
    ) \
    .orderBy(col("total_delay").desc())

print("Top entities by total delay:")
entity_delays.show(20, truncate=False)


# ============================================================
# 10. Save processed data to HDFS
# ============================================================

daily_delays.write.mode("overwrite").parquet(OUTPUT_DAILY_PATH)
monthly_delays.write.mode("overwrite").parquet(OUTPUT_MONTHLY_PATH)
entity_delays.write.mode("overwrite").parquet(OUTPUT_ENTITY_PATH)

print("Saved processed datasets:")
print(OUTPUT_DAILY_PATH)
print(OUTPUT_MONTHLY_PATH)
print(OUTPUT_ENTITY_PATH)

print("END: Flight Delay Processing")

spark.stop()