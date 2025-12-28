FROM apache/airflow:2.8.1

USER root

# Install Java runtime (needed only if SparkSubmitOperator is used)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

USER airflow
