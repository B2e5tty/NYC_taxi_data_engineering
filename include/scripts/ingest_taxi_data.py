from pathlib import Path
from urllib.request import urlretrieve
from pyspark.sql import SparkSession

# Raw data

# Create directories for raw and bronze data if they don't exist
BASE_DIR = Path("/opt/airflow/include/data")
RAW_DIR = BASE_DIR / "raw"
BRONZE_DIR = BASE_DIR / "bronze"

TRIP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "trip-data/yellow_tripdata_2025-01.parquet"
)

ZONE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/"
    "misc/taxi_zone_lookup.csv"
)

TRIP_FILE = RAW_DIR / "yellow_tripdata_2025-01.parquet"
ZONE_FILE = RAW_DIR / "taxi_zone_lookup.csv"

# download the files if they don't exist
def download_file(url, destination):
    if destination.exists():
        print(f"Already downloaded: {destination}")
        return
    
    print(f"Downloading: {destination}")
    urlretrieve(url, destination)
    print(f"Download completed: {destination}")

download_file(TRIP_URL, TRIP_FILE)
download_file(ZONE_URL, ZONE_FILE)

# Initialize Spark session
spark = SparkSession.builder.appName("NYC Taxi Data Ingestion").master("local[2]").config("spark.log.level", "WARN").getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# Read source data
trips = spark.read.parquet(str(TRIP_FILE))
zones = (spark.read.options(header=True,inferschema=True).csv(str(ZONE_FILE)))

# Inspect data
print("Trip Data")

trips.printSchema()
trips.show(5, truncate=False)

print("Trip record count:",trips.count())

print("Zones Data")

zones.printSchema()
zones.show(5, truncate=False)

print("Zones recoreded:", zones.count())

# Write Bronze data
trips.write.mode("overwrite").parquet(str(BRONZE_DIR/"yellow_trips"))
zones.write.mode("overwrite").parquet(str(BRONZE_DIR/"zones"))

print("\nBronze ingestion completed successfully.")

spark.stop()

