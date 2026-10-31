from datetime import timedelta
import subprocess
import pendulum
from airflow.sdk import dag, task

SCRIPTS_DIR = "/opt/airflow/include/scripts"
SPARK_SUBMIT = "/usr/local/bin/spark-submit"

def run_spark_job(script_name):
    script_path = f"{SCRIPTS_DIR}/{script_name}"
    command = ["spark-submit", script_path]

    print(f"Starting Spark job: {script_name}")
    result = subprocess.run(
        command,
        check=True,
        text=True
    )
    print(f"Completed Spark job: {script_name}")
    return result.returncode

@dag(
    dag_id="nyc_taxi_end_to_end_pipeline",
    description="NYC Taxi data engineering pipeline using PySpark",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["nyc-taxi", "pyspark", "data-engineering"],
)

def nyc_taxi_pipeline():
    # Task 1: Ingestion and Bronze
    @task
    def ingest_data():
        return run_spark_job("ingest_taxi_data.py")

    # Task 2: Bronze to Silver
    @task
    def transform_data():
        return run_spark_job("transform_taxi_data.py")

    # Task 3: Silver to Gold
    @task
    def build_gold():
        return run_spark_job("build_gold_layer.py")

    # Task 4: Validate Gold
    @task
    def validate_gold():
        return run_spark_job("validate_gold.py")

    ingestion = ingest_data()
    transformation = transform_data()
    gold = build_gold()
    validation = validate_gold()

    ingestion >> transformation >> gold >> validation

nyc_taxi_pipeline()
