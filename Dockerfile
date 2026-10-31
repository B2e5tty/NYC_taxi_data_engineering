FROM apache/airflow:3.2.2-python3.11

USER root

# Install Java 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install PySpark, PostgreSQL driver, and the Airflow Spark Provider
RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    psycopg2-binary==2.9.10 \
    apache-airflow-providers-apache-spark