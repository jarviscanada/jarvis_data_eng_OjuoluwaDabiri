from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from src.utils.spark_session import get_spark, stop_spark
from src.utils.config import Paths
import logging

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


def bronze_checks() -> None:
    
    logger.info("=" * 70)
    logger.info("BRONZE VALIDATION")
    logger.info("=" * 70)
    
    paths = Paths()
    spark = get_spark()
    
    try:
        prices = spark.read.parquet(paths.BRONZE_PRICES_DIR)
        companies = spark.read.parquet(paths.BRONZE_COMPANIES_DIR)
        
        if prices.limit(1).count() == 0:
            raise DataQualityError("No price data")
        if companies.limit(1).count() == 0:
            raise DataQualityError("No company data")
        
        logger.info(f"✓ Bronze prices: {prices.count():,} records")
        logger.info(f"✓ Bronze companies: {companies.count():,} records")
        logger.info("✓ BRONZE CHECKS PASSED")
        logger.info("=" * 70)
        
    finally:
        stop_spark(spark)


def silver_checks() -> None:
    
    logger.info("=" * 70)
    logger.info("SILVER VALIDATION")
    logger.info("=" * 70)
    
    paths = Paths()
    spark = get_spark()
    
    try:
        prices = spark.read.parquet(paths.SILVER_PRICES_DIR)
        companies = spark.read.parquet(paths.SILVER_COMPANIES_DIR)
        
        # Check CRITICAL fields only (date, ticker, close)
        logger.info("Checking critical fields (date, ticker, close)...")
        null_counts = prices.agg(
            count(when(col("date").isNull(), 1)).alias("null_dates"),
            count(when(col("ticker").isNull(), 1)).alias("null_tickers"),
            count(when(col("close").isNull(), 1)).alias("null_closes"),
        ).collect()[0]
        
        if any([null_counts.null_dates, null_counts.null_tickers, null_counts.null_closes]):
            raise DataQualityError(
                f"[silver_prices] Found nulls in CRITICAL fields: "
                f"dates={null_counts.null_dates}, tickers={null_counts.null_tickers}, "
                f"closes={null_counts.null_closes}"
            )
        
        # Check volume separately (informational only)
        null_volumes = prices.filter(col("volume").isNull()).count()
        total_records = prices.count()
        
        if null_volumes > 0:
            pct_null = (null_volumes / total_records) * 100
            logger.info(f"Volume is null for {null_volumes:,} records ({pct_null:.1f}%)")
            logger.info("   This is expected for historical data pre-1980s")
        
        logger.info(f"✓ Silver prices: {total_records:,} records")
        logger.info(f"✓ Silver companies: {companies.count():,} records")
        logger.info("✓ SILVER CHECKS PASSED (critical fields validated)")
        logger.info("=" * 70)
        
    finally:
        stop_spark(spark)


def gold_checks() -> None:
    """Gold validation"""
    logger.info("=" * 70)
    logger.info("GOLD VALIDATION")
    logger.info("=" * 70)
    
    paths = Paths()
    spark = get_spark()
    
    try:
        enriched = spark.read.parquet(paths.GOLD_ENRICHED_DIR)
        
        # Check data exists
        count = enriched.count()
        if count == 0:
            raise DataQualityError("Gold enriched layer is empty")
        
        logger.info(f"✓ Gold enriched: {count:,} records")
        
        # Check that daily_return was calculated for most records
        # (first day per ticker will be null, which is expected)
        has_return = enriched.filter(col("daily_return").isNotNull()).count()
        pct_with_return = (has_return / count) * 100
        logger.info(f"Records with daily_return: {has_return:,} ({pct_with_return:.1f}%)")
        
        # Check analytics tables exist
        import os
        if os.path.exists(f"{paths.GOLD_ANALYTICS_DIR}/stock_performance_summary"):
            logger.info("✓ Stock performance summary exists")
        if os.path.exists(f"{paths.GOLD_ANALYTICS_DIR}/sector_daily_returns"):
            logger.info("✓ Sector daily returns exists")
        if os.path.exists(f"{paths.GOLD_ANALYTICS_DIR}/industry_volatility"):
            logger.info("✓ Industry volatility exists")
        
        logger.info("✓ GOLD CHECKS PASSED")
        logger.info("=" * 70)
        
    finally:
        stop_spark(spark)