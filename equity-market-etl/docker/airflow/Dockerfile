FROM apache/airflow:2.8.1

USER root

# Install Java (required by SparkSubmitOperator)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Spark provider (THIS FIXES YOUR ERROR)
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark
