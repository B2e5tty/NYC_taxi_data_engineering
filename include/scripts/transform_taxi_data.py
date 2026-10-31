from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

BASE_DIR = Path("/opt/airflow/include/data")

BRONZE_DIR = BASE_DIR / "bronze"
SILVER_DIR = BASE_DIR / "silver"

TIRP_PATH = str(BRONZE_DIR / "yellow_trips")
ZONE_PATH = str(BRONZE_DIR / "zones")

SILVER_TRIP_PATH = str(SILVER_DIR/"yellow_trips")
REJECTED_TRIP_PATH = str(SILVER_DIR/"rejected_trips")

# Initialize Spark
spark = SparkSession.builder.appName("NYC Taxi Silver Transformation").master("local[2]").config("spark.log.level","WARN").getOrCreate()

# Read Bronze data
trips = spark.read.parquet(TIRP_PATH)
zone = spark.read.parquet(ZONE_PATH)

print("Bronze trip records:",trips.count())
print("Taxi zones:",zone.count())

# Standard data types
trips = (
    trips.withColumn("tpep_pickup_datetime",F.col("tpep_pickup_datetime").cast(TimestampType()))
    .withColumn("tpep_dropoff_datetime",F.col("tpep_dropoff_datetime").cast(TimestampType()))
    .withColumn("PULocationID",F.col("PULocationID").cast("integer"))
    .withColumn("DOLocationID",F.col("DOLocationID").cast("integer"))
    .withColumn("trip_distance",F.col("trip_distance").cast("double"))
    .withColumn("fare_amount",F.col("fare_amount").cast("double"))
    .withColumn("total_amount",F.col("total_amount").cast("double"))
)

# Quality Rules
duration_mintues = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60

rules = [
    (F.col("tpep_pickup_datetime").isNotNull(), "missing_pickup_datetime"),
    (F.col("tpep_dropoff_datetime").isNotNull(), "missing_dropoff_datetime"),
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"), "Invalid_time"),
    (duration_mintues.between(0.01,360) ,"Invalid_duration"),
    (F.col("fare_amount").isNotNull() & (F.col("fare_amount")>0), "Invalid_fare_amount"),
    (F.col("total_amount").isNotNull() & (F.col("total_amount")>0), "Invalid_total_amount")
]

valid_condition = F.lit(True)

for condition, reason in rules:
    valid_condition = valid_condition & condition

rejected = (
    trips.filter(~valid_condition)
    .withColumn("rejection_rs",
                F.concat_ws("|",*[F.when(~condition,F.lit(reason)) for condition, reason in rules]))
)


cleaned = trips.filter(valid_condition)

print("Valid recordes: ",{cleaned.count()})
print("Rejected recordes: ",{rejected.count()})

# Analytics column
cleaned = (
    cleaned.withColumn("duration", duration_mintues)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week",F.date_format("tpep_pickup_datetime","EEEE"))
    .withColumn("pickup_year",F.year("tpep_pickup_datetime"))
    .withColumn("pickup_month",F.month("tpep_pickup_datetime"))
)

# zone lookup
zones = (
    zone.withColumn("LocationID",F.col("LocationID").cast("integer"))
)

pickup_zones = zones.select(
    F.col("LocationID").alias("pickup_location_id"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone")
)

dropoff_zones = zones.select(
    F.col("LocationID").alias("dropoff_location_id"),
    F.col("Borough").alias("dropoff_borough"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone")
)

enriched_cleaned = (
    cleaned.join(F.broadcast(pickup_zones),
                 cleaned.PULocationID == pickup_zones["pickup_location_id"],"left")
            .join(F.broadcast(dropoff_zones),
                  cleaned.DOLocationID ==  dropoff_zones["dropoff_location_id"],"left")
            .drop("pickup_location_id", "dropoff_location_id")
)

# writer Silver
enriched_cleaned.write.mode("overwrite").parquet(SILVER_TRIP_PATH)
rejected.write.mode("overwrite").parquet(REJECTED_TRIP_PATH)

# validation
print("\n========== SILVER SUMMARY ==========")

print("Silver records:", enriched_cleaned.count())
print("Rejected records:", rejected.count())

print("\n========== SILVER SCHEMA ==========")

enriched_cleaned.printSchema()

print("\n========== SAMPLE ENRICHED RECORDS ==========")

enriched_cleaned.select(
    "tpep_pickup_datetime",
    "trip_distance",
    "duration",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "total_amount"
).show(10, truncate=False)

print("\nSilver transformation completed.")

spark.stop()






