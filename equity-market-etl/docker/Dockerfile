# Base image: Apache Airflow with Python 3.10
FROM apache/airflow:2.8.1-python3.10

# Switch to root to install system dependencies
USER root

# Install Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java home environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow