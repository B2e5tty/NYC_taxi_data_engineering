# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering project using **Apache Spark (PySpark), Apache Airflow, PostgreSQL, and Power BI** to process and analyze NYC Yellow Taxi trip data.

## Project Overview

The project builds an automated data pipeline that ingests raw NYC taxi trip data, transforms and validates records, generates analytical datasets, and prepares the results for reporting.

## Architecture

**Data Source → Raw → Bronze → Silver → Gold → PostgreSQL → Power BI**

- **Raw:** Downloaded NYC Yellow Taxi trip data and taxi zone lookup.
- **Bronze:** Raw data stored in Parquet format.
- **Silver:** Cleaned, transformed, and validated trip records.
- **Gold:** Aggregated datasets for analytical reporting.
- **PostgreSQL:** Stores processed datasets for querying.
- **Power BI:** Used to build interactive dashboards.

## Technologies

- Python
- Apache Spark (PySpark)
- Apache Airflow 3.2.2
- Docker & Docker Compose
- PostgreSQL
- Power BI
- Parquet

## Project Structure

```text
nyc-taxi-data-engineering/
├── dags/                 # Airflow DAGs
├── logs/                 # Airflow logs
├── plugins/              # Airflow plugins
├── config/               # Configuration files
├── include/
│   ├── data/
│   │   ├── raw/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── scripts/          # PySpark processing scripts
├── tests/                # Data validation tests
├── Dockerfile
├── docker-compose.yaml
├── .env
└── .gitignore
```

## Data Source

NYC Yellow Taxi Trip Records:

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- January 2025 Yellow Taxi data (Parquet)
- Taxi Zone Lookup (CSV)

## Pipeline Processing

1. **Ingestion:** Download and load taxi trip data and zone lookup data.
2. **Bronze:** Store source data in Parquet format.
3. **Silver:** Clean records, apply data quality rules, derive trip attributes, and join taxi zone information.
4. **Gold:** Generate daily, hourly, pickup zone, drop-off zone, and payment analysis datasets.
5. **Validation:** Reconcile Gold aggregations against Silver data.
6. **Orchestration:** Use Airflow to execute and monitor the processing workflow.
7. **Analytics:** Load analytical datasets into PostgreSQL for Power BI reporting.

## Getting Started

Start the project services:

```bash
docker compose up -d
```

Check running services:

```bash
docker compose ps
```

Check the Airflow version:

```bash
docker compose exec airflow-scheduler airflow version
```

Access the Airflow interface:

```text
http://localhost:8081
```

Trigger the `nyc_taxi_end_to_end_pipeline` DAG from the Airflow interface.

## Project Status

- [x] Docker-based Airflow environment
- [x] Raw data ingestion
- [x] Bronze layer creation
- [x] Silver layer transformation and data quality checks
- [x] Gold layer aggregations
- [x] Gold data validation
- [x] Complete Airflow DAG orchestration and execution
- [x] Load Gold datasets into PostgreSQL
- [x] Build Power BI dashboards

## Learning Objectives

- Practice PySpark for large-scale data processing.
- Understand the Medallion Architecture (Bronze, Silver, Gold).
- Build and orchestrate ETL pipelines using Apache Airflow.
- Apply data quality checks and validation.
- Integrate analytical datasets with PostgreSQL and Power BI.
