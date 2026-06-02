from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct, lit, sum as spark_sum, to_date, when
import os

spark = SparkSession.builder.appName("GDELT Elasticsearch Processing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX = os.getenv("ES_INDEX", "gdelt_raw")
ES_NODES_WAN_ONLY = os.getenv("ES_NODES_WAN_ONLY", "false").lower() in ("1", "true", "yes")
START_DATE = os.getenv("GDELT_START_DATE", "20260101")
END_DATE = os.getenv("GDELT_END_DATE", "20260120")

HDFS_BASE = os.getenv("HDFS_BASE", "hdfs://nn1:9000")
OUTPUT_GDELT_DAILY_PATH = os.path.join(HDFS_BASE, "bigdata/flight_delay/processed/gdelt_daily")
OUTPUT_GDELT_COUNTRY_PATH = os.path.join(HDFS_BASE, "bigdata/flight_delay/processed/gdelt_country_daily")

print("START: Przetwarzanie GDELT z Elasticsearch")
print(f"Elasticsearch host: {ES_HOST}:{ES_PORT}")
print(f"Index: {ES_INDEX}")
print(f"Output daily: {OUTPUT_GDELT_DAILY_PATH}")
print(f"Output country daily: {OUTPUT_GDELT_COUNTRY_PATH}")

es_options = {
    "es.nodes": ES_HOST,
    "es.port": ES_PORT,
    "es.read.metadata": "false",
    "es.index.read.missing.as.empty": "true",
}
if ES_NODES_WAN_ONLY:
    es_options["es.nodes.wan.only"] = "true"

print("Loading raw GDELT documents from Elasticsearch...")
raw = spark.read.format("es").options(**es_options).load(ES_INDEX)
print("Loaded raw Elasticsearch DataFrame")
print(f"Raw columns: {raw.columns}")
raw_count = raw.count()
print(f"Raw row count: {raw_count}")
raw.cache()

gdelt = raw.selectExpr("record.*")
print("Expanded record fields from raw data")
print(f"GDELT columns: {gdelt.columns}")

gdelt_count = gdelt.count()
print(f"GDELT row count after record expansion: {gdelt_count}")

if "SQLDATE" not in gdelt.columns:
    raise RuntimeError("Brak pola SQLDATE w danych GDELT. Sprawdź mapping indeksu Elasticsearch.")

gdelt = gdelt.withColumn("date", to_date(col("SQLDATE"), "yyyyMMdd"))
print("Converted SQLDATE to date column")

if START_DATE:
    gdelt_start = to_date(lit(START_DATE), "yyyyMMdd")
    gdelt = gdelt.filter(col("date") >= gdelt_start)
if END_DATE:
    gdelt_end = to_date(lit(END_DATE), "yyyyMMdd")
    gdelt = gdelt.filter(col("date") <= gdelt_end)

filtered_count = gdelt.count()
print(f"Filtered GDELT rows in date range {START_DATE} - {END_DATE}: {filtered_count}")

# Cechy agregowane dziennie.
print("Starting daily aggregation for GDELT data...")
gdelt_daily = gdelt.groupBy("date").agg(
    spark_sum(when(col("GLOBALEVENTID").isNotNull(), 1).otherwise(0)).alias("gdelt_event_count"),
    countDistinct(col("SOURCEURL")).alias("gdelt_unique_source_count"),
    spark_sum(col("NumMentions").cast("int")).alias("gdelt_total_mentions"),
    spark_sum(col("NumSources").cast("int")).alias("gdelt_total_sources"),
    spark_sum(col("NumArticles").cast("int")).alias("gdelt_total_articles"),
    avg(col("GoldsteinScale").cast("double")).alias("gdelt_avg_goldstein"),
    avg(col("AvgTone").cast("double")).alias("gdelt_avg_tone"),
    spark_sum(when(col("QuadClass").cast("int") == 4, 1).otherwise(0)).alias("gdelt_conflict_event_count"),
    spark_sum(when(col("EventRootCode") == "14", 1).otherwise(0)).alias("gdelt_protest_event_count"),
)

# Cechy dzienne agregowane po kraju akcji (ActionGeo_CountryCode).
print("Starting country-level daily aggregation for GDELT data...")
gdelt_country_daily = gdelt.groupBy("date", "ActionGeo_CountryCode").agg(
    spark_sum(when(col("GLOBALEVENTID").isNotNull(), 1).otherwise(0)).alias("gdelt_event_count"),
    countDistinct(col("SOURCEURL")).alias("gdelt_unique_source_count"),
    spark_sum(col("NumMentions").cast("int")).alias("gdelt_total_mentions"),
    spark_sum(col("NumSources").cast("int")).alias("gdelt_total_sources"),
    spark_sum(col("NumArticles").cast("int")).alias("gdelt_total_articles"),
    avg(col("GoldsteinScale").cast("double")).alias("gdelt_avg_goldstein"),
    avg(col("AvgTone").cast("double")).alias("gdelt_avg_tone"),
    spark_sum(when(col("QuadClass").cast("int") == 4, 1).otherwise(0)).alias("gdelt_conflict_event_count"),
    spark_sum(when(col("EventRootCode") == "14", 1).otherwise(0)).alias("gdelt_protest_event_count"),
).withColumnRenamed("ActionGeo_CountryCode", "action_country")

print("Saving daily GDELT features...")
gdelt_daily.write.mode("append").parquet(OUTPUT_GDELT_DAILY_PATH)
print("Saved daily GDELT features")
print(f"Daily feature rows: {gdelt_daily.count()}")
print("Saving daily GDELT country features...")
gdelt_country_daily.write.mode("overwrite").parquet(OUTPUT_GDELT_COUNTRY_PATH)
print("Saved daily GDELT country features")
print(f"Daily country feature rows: {gdelt_country_daily.count()}")

print("END: Przetwarzanie GDELT z Elasticsearch")
print("Saved:")
print(OUTPUT_GDELT_DAILY_PATH)
print(OUTPUT_GDELT_COUNTRY_PATH)

spark.stop()
