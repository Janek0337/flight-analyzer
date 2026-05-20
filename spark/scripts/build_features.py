from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, lower, trim, when


spark = SparkSession.builder \
    .appName("Build Local Flight Weather Features") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

HDFS_BASE = "hdfs://nn1:9000"
FLIGHTS_DAILY = HDFS_BASE + "/bigdata/flight_delay/processed/daily_delays"
WEATHER_DAILY = HDFS_BASE + "/bigdata/flight_delay/processed/weather_daily"
WEATHER_EUROPE_DAILY = HDFS_BASE + "/bigdata/flight_delay/processed/weather_europe_daily"
OUTPUT_FEATURES = HDFS_BASE + "/bigdata/flight_delay/processed/features/daily_features_parquet"

print("START: Budowa lokalnych features")
print(f"Flights: {FLIGHTS_DAILY}")
print(f"Weather local: {WEATHER_DAILY}")

flights = spark.read.parquet(FLIGHTS_DAILY)
weather = spark.read.parquet(WEATHER_DAILY)
weather_europe = spark.read.parquet(WEATHER_EUROPE_DAILY)

if "date" not in flights.columns or "date" not in weather.columns:
    raise RuntimeError("Flights and weather inputs must contain 'date' column")
if "ENTITY_NAME" not in flights.columns:
    raise RuntimeError("Flights input must contain 'ENTITY_NAME' column")
if "icao" not in weather.columns:
    raise RuntimeError("Weather input must contain 'icao' column")

# EUROCONTROL ERT DLY ANSP jest agregacją po podmiotach kontroli ruchu, a nie po
# pojedynczych lotach. Mapujemy więc podmiot na reprezentatywne lotnisko/stację
# pogodową. To daje lokalne cechy pogodowe dla obszaru zamiast średniej Europy.
entity_key = lower(trim(col("ENTITY_NAME")))
flights_with_station = flights.withColumn(
    "weather_station_icao",
    when(entity_key.contains("pansa") | entity_key.contains("poland"), lit("EPWA"))
    .when(entity_key.contains("dfs") | entity_key.contains("germany"), lit("EDDF"))
    .when(entity_key.contains("dsna") | entity_key.contains("france"), lit("LFPG"))
    .when(entity_key.contains("nats") | entity_key.contains("united kingdom") | entity_key.contains("uk"), lit("EGLL"))
    .when(entity_key.contains("lvnl") | entity_key.contains("netherlands"), lit("EHAM"))
    .when(entity_key.contains("enaire") | entity_key.contains("spain"), lit("LEMD"))
    .when(entity_key.contains("enav") | entity_key.contains("italy"), lit("LIRF"))
    .when(entity_key.contains("austro") | entity_key.contains("austria"), lit("LOWW"))
    .when(entity_key.contains("skeyes") | entity_key.contains("belgocontrol") | entity_key.contains("belgium"), lit("EBBR"))
    .when(entity_key.contains("skyguide") | entity_key.contains("switzerland"), lit("LSZH"))
    .when(entity_key.contains("lfv") | entity_key.contains("sweden"), lit("ESSA"))
    .when(entity_key.contains("avinor") | entity_key.contains("norway"), lit("ENGM"))
    .when(entity_key.contains("naviair") | entity_key.contains("denmark"), lit("EKCH"))
    .when(entity_key.contains("finavia") | entity_key.contains("fintraffic") | entity_key.contains("finland"), lit("EFHK"))
    .when(entity_key.contains("nav portugal") | entity_key.contains("portugal"), lit("LPPT"))
    .when(entity_key.contains("romatsa") | entity_key.contains("romania"), lit("LROP"))
    .when(entity_key.contains("ans cr") | entity_key.contains("czech"), lit("LKPR"))
    .when(entity_key.contains("hungaro") | entity_key.contains("hungary"), lit("LHBP"))
    .when(entity_key.contains("hasa") | entity_key.contains("hasp") | entity_key.contains("greece"), lit("LGAV"))
    .when(entity_key.contains("iaa") | entity_key.contains("ireland"), lit("EIDW"))
    .when(entity_key.contains("bulatsa") | entity_key.contains("bulgaria"), lit("LBSF"))
    .when(entity_key.contains("croatia"), lit("LDZA"))
    .when(entity_key.contains("slovenia"), lit("LJLA"))
    .when(entity_key.contains("slovak"), lit("LZIB"))
    .when(entity_key.contains("oro navigacija") | entity_key.contains("lithuania"), lit("EYVI"))
    .when(entity_key.contains("lgs") | entity_key.contains("latvia"), lit("EVRA"))
    .when(entity_key.contains("eans") | entity_key.contains("estonia"), lit("EETN"))
    .when(entity_key.contains("moldova"), lit("LUKK"))
    .when(entity_key.contains("ukraine"), lit("UKBB"))
    .when(entity_key.contains("turkey") | entity_key.contains("dhmi"), lit("LTFM"))
    .when(entity_key.contains("ana lux") | entity_key.contains("luxembourg"), lit("ELLX"))
    .when(entity_key.contains("albcontrol") | entity_key.contains("albania"), lit("LATI"))
    .when(entity_key.contains("dcac cyprus") | entity_key.contains("cyprus"), lit("LCLK"))
    .when(entity_key.contains("armats") | entity_key.contains("armenia"), lit("UDYZ"))
    .when(entity_key.contains("bhansa") | entity_key.contains("bosnia"), lit("LQSA"))
    .when(entity_key.contains("smatsa") | entity_key.contains("serbia") | entity_key.contains("montenegro"), lit("LYBE"))
    .when(entity_key.contains("m-nav") | entity_key.contains("macedonia"), lit("LWSK"))
    .when(entity_key.contains("mats") | entity_key.contains("malta"), lit("LMML"))
    .when(entity_key.contains("ashna") | entity_key.contains("kosovo"), lit("BKPR"))
)

local_weather = weather.select(
    col("date"),
    col("icao").alias("weather_station_icao"),
    col("airport").alias("weather_station_name"),
    col("avg_temperature"),
    col("avg_humidity"),
    col("avg_dew_point"),
    col("total_precipitation"),
    col("total_rain"),
    col("total_snowfall"),
    col("avg_cloud_cover"),
    col("avg_pressure_msl"),
    col("avg_surface_pressure"),
    col("avg_wind_speed"),
    col("max_wind_gusts"),
    col("min_temperature"),
    col("max_temperature"),
)

europe_weather = weather_europe.select(
    col("date"),
    col("avg_temperature_europe"),
    col("avg_humidity_europe"),
    col("avg_precipitation_europe"),
    col("avg_rain_europe"),
    col("avg_snowfall_europe"),
    col("avg_cloud_cover_europe"),
    col("avg_wind_speed_europe"),
    col("max_wind_gusts_europe"),
)

features = flights_with_station.join(
    local_weather,
    ["date", "weather_station_icao"],
    "left",
).join(
    europe_weather,
    "date",
    "left",
)

features = features.withColumn(
    "weather_match_level",
    when(col("weather_station_name").isNotNull(), lit("entity_station"))
    .when(col("weather_station_icao").isNotNull(), lit("station_missing_weather"))
    .otherwise(lit("europe_fallback")),
)

# Fallback europejski zostaje tylko po to, żeby pipeline nie tracił wierszy. Model
# dostaje flagę `weather_match_level`, więc widać, które rekordy są mniej precyzyjne.
features = features.withColumn("feature_temperature", coalesce(col("avg_temperature"), col("avg_temperature_europe")))
features = features.withColumn("feature_humidity", coalesce(col("avg_humidity"), col("avg_humidity_europe")))
features = features.withColumn("feature_precipitation", coalesce(col("total_precipitation"), col("avg_precipitation_europe")))
features = features.withColumn("feature_rain", coalesce(col("total_rain"), col("avg_rain_europe")))
features = features.withColumn("feature_snowfall", coalesce(col("total_snowfall"), col("avg_snowfall_europe")))
features = features.withColumn("feature_cloud_cover", coalesce(col("avg_cloud_cover"), col("avg_cloud_cover_europe")))
features = features.withColumn("feature_wind_speed", coalesce(col("avg_wind_speed"), col("avg_wind_speed_europe")))
features = features.withColumn("feature_wind_gusts", coalesce(col("max_wind_gusts"), col("max_wind_gusts_europe")))

print("Sample features:")
features.show(20, truncate=False)

print("Weather matching quality:")
features.groupBy("weather_match_level").count().show(truncate=False)

features.write.mode("overwrite").parquet(OUTPUT_FEATURES)

print("Saved features to:")
print(OUTPUT_FEATURES)
print("END: Budowa lokalnych features")

spark.stop()
