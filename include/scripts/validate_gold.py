
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path("/opt/airflow/include/data")

SILVER_PATH = str(BASE_DIR / "silver" / "yellow_trips")
GOLD_DIR = BASE_DIR / "gold"

spark = SparkSession.builder.appName("NYC Taxi Gold Validation").master("local[2]").config("spark.log.level","WARN").getOrCreate()

# Read Silver
silver = spark.read.parquet(SILVER_PATH)
silver_count = silver.count()

silver_revenue = (silver.agg(F.sum("total_amount")).first()[0] or 0)

# Read Gold datasets
gold_datasets = {
    "daily_trip_summary": ("total_trips", "total_revenue"),
    "hourly_demand": ("total_trips", "total_revenue"),
    "pickup_zone_analysis": ("pickup_trips", "associated_trip_revenue"),
    "dropoff_zone_analysis": ("dropoff_trips", "associated_trip_revenue"),
    "payment_analysis": ("total_trips", "total_revenue"),
}

print("\n========== GOLD VALIDATION ==========")

for dataset_name, (count_column, revenue_column) in gold_datasets.items():
    path = str(GOLD_DIR / dataset_name)
    gold = spark.read.parquet(path)
    gold_count = (
        gold.agg(F.sum(count_column)).first()[0] or 0
    )
    gold_revenue = (
        gold.agg(F.sum(revenue_column)).first()[0] or 0
    )
    print(f"\nDataset: {dataset_name}")
    print(f"Silver count: {silver_count:,}")
    print(f"Gold count: {gold_count:,}")
    print(f"Gold revenue: {gold_revenue:,.2f}")

    if gold_count != silver_count:
        raise ValueError(
            f"Count mismatch in {dataset_name}"
        )

    # Gold revenue columns are rounded to 2 decimals.
    # Allow a small rounding tolerance across the month.
    tolerance = 1.00

    if abs(float(silver_revenue) - float(gold_revenue)) > tolerance:
        raise ValueError(
            f"Revenue mismatch in {dataset_name}: "
            f"Silver={silver_revenue}, Gold={gold_revenue}"
        )

print("\nAll Gold validation checks passed.")

spark.stop()
