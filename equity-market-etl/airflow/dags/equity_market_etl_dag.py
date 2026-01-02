
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import pipeline modules
from src.ingestion.ingest_companies import ingest_companies_to_bronze
from src.ingestion.ingest_prices import ingest_prices_to_bronze
from src.validation.data_quality_checks import (
    bronze_checks, 
    silver_checks, 
    gold_checks
)
from src.transformations.bronze_to_silver import (
    transform_companies_bronze_to_silver,
    transform_prices_bronze_to_silver,
)
from src.transformations.silver_to_gold import (
    create_gold_enriched,
    create_gold_analytics,
)

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# CALLBACK FUNCTIONS
# -------------------------------------------------------------------

def notify_failure(context):
    #log failure
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id")
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    
    error_msg = f"""
    =====================================
    AIRFLOW TASK FAILURE ALERT
    =====================================
    DAG: {dag_id}
    Task: {task_id}
    Run ID: {run_id}
    Execution Date: {execution_date}
    Error: {exception}
    =====================================
    """
    
    logger.error(error_msg)


def pipeline_start_notification(**context):
    #pipe start log
    logger.info("="*60)
    logger.info("EQUITY MARKET ETL PIPELINE STARTED")
    logger.info(f"Execution Date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    logger.info("="*60)


def pipeline_completion_notification(**context):
    #pipe comp log
    logger.info("="*60)
    logger.info("✓ EQUITY MARKET ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"Execution Date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    logger.info("All layers processed and validated")
    logger.info("Data ready for analysis")
    logger.info("="*60)


# -------------------------------------------------------------------
# DAG CONFIGURATION
# -------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,  # Set to True and add email if needed
    "email_on_retry": False,
    "retries": 3,  # Increased from 2 for better resilience
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------

with DAG(
    dag_id="equity_market_etl_pipeline",
    description="Medallion-style batch ETL for equity market data (Bronze→Silver→Gold)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["equity", "market-data", "batch", "etl", "medallion"],
    doc_md="""
    # Equity Market ETL Pipeline
    
    ## Architecture Layers
    
    ### Bronze Layer (Raw Ingestion)
    - Preserves source data exactly as received
    - Enables reprocessing and auditability
    - No business logic applied
    
    ### Silver Layer (Cleaned & Validated)
    - Enforces data types and formats
    - Validates business rules
    - Removes duplicates
    - Trusted analytical foundation
    
    ### Gold Layer (Analytics-Ready)
    - Enriched with business metadata
    - Pre-computed metrics and aggregations
    - Optimized for query performance
    - Ready for dashboards and reports
    
    ## Data Quality
    Progressive validation at each layer transition with fail-fast approach.
    
    ## Schedule
    Runs daily at midnight (UTC)
    """,
) as dag:
    
    # ===================================================================
    # Task 1: Pipeline Start Notification
    # ===================================================================
    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=pipeline_start_notification,
        provide_context=True,
        doc_md="Log pipeline start and execution metadata",
    )
    
    # ===================================================================
    # BRONZE LAYER - RAW INGESTION (Parallel Execution)
    # ===================================================================
    
    ingest_companies_bronze = PythonOperator(
        task_id="ingest_companies_to_bronze",
        python_callable=ingest_companies_to_bronze,
        doc_md="""
        Ingest company reference data (dimension table) to Bronze layer.
        - Reads: data/raw/company_info_and_logos/companies.csv
        - Writes: data/bronze/companies/ (Parquet)
        - Preserves raw structure with minimal transformation
        """,
    )
    
    ingest_prices_bronze = PythonOperator(
        task_id="ingest_prices_to_bronze",
        python_callable=ingest_prices_to_bronze,
        doc_md="""
        Ingest daily OHLCV price data from individual ticker CSV files.
        - Reads: data/raw/us_stocks_etfs/*.csv
        - Writes: data/bronze/prices/ (Parquet, partitioned by ticker)
        - Extracts ticker from filename
        """,
    )
    
    # ===================================================================
    # BRONZE VALIDATION
    # ===================================================================
    
    validate_bronze = PythonOperator(
        task_id="validate_bronze_layer",
        python_callable=bronze_checks,
        doc_md="""
        Structural validation of Bronze layer:
        - Required columns exist
        - Datasets are not empty
        - Files are readable
        
        Fails pipeline if Bronze data is malformed.
        """,
    )
    
    # ===================================================================
    # SILVER LAYER - CLEAN & VALIDATE (Parallel Execution)
    # ===================================================================
    
    bronze_to_silver_companies = PythonOperator(
        task_id="transform_companies_to_silver",
        python_callable=transform_companies_bronze_to_silver,
        doc_md="""
        Clean and standardize company reference data:
        - Normalize column names
        - Trim whitespace
        - Convert market cap to numeric
        - Remove duplicates
        
        Writes: data/silver/companies/ (Parquet)
        """,
    )
    
    bronze_to_silver_prices = PythonOperator(
        task_id="transform_prices_to_silver",
        python_callable=transform_prices_bronze_to_silver,
        doc_md="""
        Clean and validate price data:
        - Parse dates properly
        - Cast prices to double, volume to long
        - Remove nulls and duplicates
        - Enforce (ticker, date) uniqueness
        
        Writes: data/silver/prices/ (Parquet, partitioned by ticker)
        """,
    )
    
    # ===================================================================
    # SILVER VALIDATION
    # ===================================================================
    
    validate_silver = PythonOperator(
        task_id="validate_silver_layer",
        python_callable=silver_checks,
        doc_md="""
        Business rule validation of Silver layer:
        - No nulls in critical fields (date, ticker, close, volume)
        - All prices and volumes non-negative
        - High >= Low (market rule)
        - No duplicate (ticker, date) combinations
        
        These checks ensure data is financially valid.
        """,
    )
    
    # ===================================================================
    # GOLD LAYER - ANALYTICS-READY
    # ===================================================================
    
    create_gold_enriched = PythonOperator(
        task_id="create_gold_enriched_prices",
        python_callable=create_gold_enriched,
        doc_md="""
        Create enriched price dataset:
        - Join prices with company metadata (sector, industry)
        - Calculate daily returns using adjusted close
        - Calculate intraday metrics (range, gap)
        - Add derived fields
        
        Writes: data/gold/prices_enriched/ (Parquet, partitioned by ticker)
        Enables sector/industry analysis and stock screening.
        """,
    )
    
    create_gold_analytics = PythonOperator(
        task_id="create_gold_analytics_tables",
        python_callable=create_gold_analytics,
        doc_md="""
        Create pre-computed analytics tables:
        1. sector_daily_returns - Daily sector performance metrics
        2. stock_performance_summary - Per-stock aggregated statistics
        3. industry_volatility - Industry risk rankings
        
        Writes: data/gold/analytics/ (Parquet)
        Optimized for fast dashboard queries and reporting.
        """,
    )
    
    # ===================================================================
    # GOLD VALIDATION
    # ===================================================================
    
    validate_gold = PythonOperator(
        task_id="validate_gold_layer",
        python_callable=gold_checks,
        doc_md="""
        Analytical validation of Gold layer:
        - Enrichment join succeeded (sector/industry not null)
        - Derived metrics calculated (daily_return exists)
        - No systematic data loss
        
        Ensures analytics outputs are complete and usable.
        """,
    )
    
    # ===================================================================
    # Task 11: Pipeline Completion Notification
    # ===================================================================
    
    complete_pipeline = PythonOperator(
        task_id="complete_pipeline",
        python_callable=pipeline_completion_notification,
        provide_context=True,
        doc_md="Log successful pipeline completion",
    )
    
    # ===================================================================
    # DAG DEPENDENCIES (Execution Flow)
    # ===================================================================
    
    # Start pipeline
    start_pipeline >> [ingest_companies_bronze, ingest_prices_bronze]
    
    # Bronze layer: Ingest in parallel, then validate
    [ingest_companies_bronze, ingest_prices_bronze] >> validate_bronze
    
    # Silver layer: Transform in parallel, then validate
    validate_bronze >> [bronze_to_silver_companies, bronze_to_silver_prices]
    [bronze_to_silver_companies, bronze_to_silver_prices] >> validate_silver
    
    # Gold layer: Create enriched first (needed for analytics), then analytics
    validate_silver >> create_gold_enriched >> create_gold_analytics
    
    # Gold validation
    create_gold_analytics >> validate_gold
    
    # Complete pipeline
    validate_gold >> complete_pipeline