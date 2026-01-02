
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    # Raw data sources
    RAW_PRICES_DIR: str = os.getenv(
        "RAW_PRICES_DIR", 
        "/opt/data/raw/us_stocks_etfs"
    )
    RAW_COMPANIES_CSV: str = os.getenv(
        "RAW_COMPANIES_CSV",
        "/opt/data/raw/company_info_and_logos/companies.csv"
    )
    
    # Bronze layer (raw Parquet)
    BRONZE_COMPANIES_DIR: str = os.getenv(
        "BRONZE_COMPANIES_DIR",
        "/opt/data/bronze/companies"
    )
    BRONZE_PRICES_DIR: str = os.getenv(
        "BRONZE_PRICES_DIR",
        "/opt/data/bronze/prices"
    )
    
    # Silver layer (cleaned Parquet)
    SILVER_COMPANIES_DIR: str = os.getenv(
        "SILVER_COMPANIES_DIR",
        "/opt/data/silver/companies"
    )
    SILVER_PRICES_DIR: str = os.getenv(
        "SILVER_PRICES_DIR",
        "/opt/data/silver/prices"
    )
    
    # Gold layer (analytics-ready)
    GOLD_ENRICHED_DIR: str = os.getenv(
        "GOLD_ENRICHED_DIR",
        "/opt/data/gold/prices_enriched"
    )
    GOLD_ANALYTICS_DIR: str = os.getenv(
        "GOLD_ANALYTICS_DIR",
        "/opt/data/gold/analytics"
    )


@dataclass(frozen=True)
class PipelineConfig:
    """
    Spark and pipeline execution configuration
    OPTIMIZED for 18M+ records with local Spark mode
    """
    
    # Application name
    APP_NAME: str = os.getenv(
        "SPARK_APP_NAME", 
        "equity-market-etl"
    )
    
    # Spark master - LOCAL MODE for better performance with medium data
    SPARK_MASTER: str = os.getenv(
        "SPARK_MASTER", 
        "local[*]"  # Use all available cores
    )
    
    # Shuffle partitions - REDUCED for faster processing
    # Rule: 2-4x number of cores, or data_size_gb * 2
    SHUFFLE_PARTITIONS: str = os.getenv(
        "SPARK_SHUFFLE_PARTITIONS", 
        "8"  # Reduced from 16 - optimal for 2-3GB data
    )
    
    # Enable adaptive query execution
    ADAPTIVE_EXECUTION: str = os.getenv(
        "SPARK_ADAPTIVE_EXECUTION", 
        "true"
    )
    
    # Memory settings - INCREASED for large datasets
    DRIVER_MEMORY: str = os.getenv(
        "SPARK_DRIVER_MEMORY",
        "4g"  # Increased from 2g
    )
    EXECUTOR_MEMORY: str = os.getenv(
        "SPARK_EXECUTOR_MEMORY",
        "4g"  # Increased from 2g
    )
    
    # File optimization
    MAX_RECORDS_PER_FILE: int = int(os.getenv(
        "MAX_RECORDS_PER_FILE",
        "500000"  # ~100MB per file for Parquet
    ))
    
    # Coalesce settings for writes
    NUM_OUTPUT_FILES: int = int(os.getenv(
        "NUM_OUTPUT_FILES",
        "20"  # Reduced from 100+ - fewer, larger files
    ))
    
    # Performance tuning
    DEFAULT_PARALLELISM: str = os.getenv(
        "SPARK_DEFAULT_PARALLELISM",
        "8"  # Match shuffle partitions
    )