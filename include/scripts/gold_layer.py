from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path("/opt/airflow/include/data")

SILVER_PATH = str(BASE_DIR / "silver" / "yellow_trips")
GOLD_DIR = BASE_DIR / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)

# Intialize
spark = SparkSession.builder.appName("NYC Taxi analysis").master("local[2]").config("spark.log.level","WARN").getOrCreate()

df = spark.read.parquet(SILVER_PATH)
print("Silver record count:", df.count())

# Print output function
def save_gold_dataset(dataframe, dataset_name):
    output_path = str(GOLD_DIR / dataset_name)

    dataframe.write.mode("overwrite").parquet(output_path)

    print("Saved dataset: ",{dataset_name})

# Analysis
# Daily trip summary
daily_summary =(
    df.groupBy("pickup_date").agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"),2).alias("total_revenue"),
        F.round(F.sum("fare_amount"),2).alias("total_fare"),
        F.round(F.avg("total_amount"),2).alias("avg_total_amount"),
        F.round(F.avg("trip_distance"),2).alias("avg_trip_distance"),
        F.round(F.avg("duration"),2).alias("avg_duration"),
        F.round(F.sum("tip_amount"),2).alias("total_tip"),
        F.sum("passenger_count").alias("total_passengers"),
    )
)

save_gold_dataset(daily_summary, "daily_summary")

# hourly summary
hourly_demand = (
    df.groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("average_trip_amount"),
        F.round( F.avg("trip_distance"), 2).alias("average_distance")
    )
    .orderBy("pickup_hour")
)
save_gold_dataset(hourly_demand, "hourly_summary")

# pickup analysis
pickup_zone_analysis = (
    df.groupBy("pickup_borough","pickup_zone")
    .agg(F.count("*").alias("pickup_trips"),
        F.round(F.sum("total_amount"), 2).alias("associated_trip_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("average_trip_distance")).orderBy(F.desc("pickup_trips"))
)

# payment analysis
payment_analysis = (
    df.withColumn("payment_type",F.col("payment_type").cast("integer"))
    .withColumn("payment_method",
        F.when(F.col("payment_type") == 1, "Credit Card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No Charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided Trip")
         .otherwise("Not Recorded")
    )
    .groupBy("payment_type", "payment_method")
    .agg(
        F.count("*").alias("total_trips"),
        F.round( F.sum("total_amount"), 2).alias("total_revenue"),
        F.round( F.sum("fare_amount"), 2).alias("total_fare"),
        F.round( F.sum("tip_amount"), 2).alias("total_tips"),
        F.round(F.avg("total_amount"), 2).alias("average_trip_amount"))
    .orderBy(F.desc("total_trips"))
)

save_gold_dataset(payment_analysis, "payment_analysis")

# Verify
datasets = {
    "Daily Summary": daily_summary,
    "Hourly Demand": hourly_demand,
    "Pickup Zone Analysis": pickup_zone_analysis,
    "Payment Analysis": payment_analysis
}

for name, dataset in datasets.items():
    print(f"\n========== {name} ==========")
    dataset.show(10, truncate=False)

print("\n========== GOLD LAYER VALIDATION ==========")

print("Silver records:", df.count())

print(
    "Daily summary trips:",
    daily_summary.agg(
        F.sum("total_trips")
    ).first()[0]
)

print(
    "Hourly demand trips:",
    hourly_demand.agg(
        F.sum("total_trips")
    ).first()[0]
)

print(
    "Pickup zone trips:",
    pickup_zone_analysis.agg(
        F.sum("pickup_trips")
    ).first()[0]
)

print(
    "Payment analysis trips:",
    payment_analysis.agg(
        F.sum("total_trips")
    ).first()[0]
)

print("\nGold layer completed successfully.")

spark.stop()