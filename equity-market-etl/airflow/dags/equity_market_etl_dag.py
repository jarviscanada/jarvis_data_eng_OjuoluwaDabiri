from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# -------------------------------------------------------------------
# DAG CONFIG
# -------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="equity_market_etl",
    description="Medallion-style batch ETL for equity market data using Spark",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "batch", "etl", "market-data"],
) as dag:

    # -------------------------------------------------------------------
    # BRONZE LAYER — RAW INGESTION
    # -------------------------------------------------------------------

    ingest_companies_bronze = SparkSubmitOperator(
        task_id="ingest_companies_bronze",
        application="/opt/airflow/src/ingestion/ingest_companies.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="ingest_companies_bronze",
        verbose=True,
    )

    ingest_prices_bronze = SparkSubmitOperator(
        task_id="ingest_prices_bronze",
        application="/opt/airflow/src/ingestion/ingest_prices.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="ingest_prices_bronze",
        verbose=True,
    )

    # -------------------------------------------------------------------
    # SILVER LAYER — CLEAN + VALIDATE
    # -------------------------------------------------------------------

    bronze_to_silver_companies = SparkSubmitOperator(
        task_id="bronze_to_silver_companies",
        application="/opt/airflow/src/transformations/bronze_to_silver.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="bronze_to_silver_companies",
        verbose=True,
    )

    bronze_to_silver_prices = SparkSubmitOperator(
        task_id="bronze_to_silver_prices",
        application="/opt/airflow/src/transformations/bronze_to_silver.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="bronze_to_silver_prices",
        verbose=True,
    )

    silver_quality_checks = SparkSubmitOperator(
        task_id="silver_data_quality_checks",
        application="/opt/airflow/src/validation/data_quality_checks.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="silver_quality_checks",
        verbose=True,
    )

    # -------------------------------------------------------------------
    # GOLD LAYER — ANALYTICS
    # -------------------------------------------------------------------

    create_gold_enriched = SparkSubmitOperator(
        task_id="create_gold_enriched",
        application="/opt/airflow/src/transformations/silver_to_gold.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="create_gold_enriched",
        verbose=True,
    )

    create_gold_analytics = SparkSubmitOperator(
        task_id="create_gold_analytics",
        application="/opt/airflow/src/transformations/silver_to_gold.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        name="create_gold_analytics",
        verbose=True,
    )

    # -------------------------------------------------------------------
    # DAG DEPENDENCIES
    # -------------------------------------------------------------------

    ingest_companies_bronze >> bronze_to_silver_companies
    ingest_prices_bronze >> bronze_to_silver_prices

    bronze_to_silver_companies >> silver_quality_checks
    bronze_to_silver_prices >> silver_quality_checks

    silver_quality_checks >> create_gold_enriched >> create_gold_analytics

