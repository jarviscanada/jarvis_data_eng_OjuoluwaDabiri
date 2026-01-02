from pyspark.sql.functions import (
    col, to_date, trim, regexp_replace, current_timestamp, lit
)
from pyspark.sql.types import DoubleType, LongType
from src.utils.spark_session import get_spark, stop_spark
from src.utils.config import Paths, PipelineConfig
import logging

logger = logging.getLogger(__name__)


def transform_companies_bronze_to_silver() -> None:
    
    logger.info("=" * 70)
    logger.info("SILVER TRANSFORMATION: Company Data (OPTIMIZED)")
    logger.info("=" * 70)
    
    paths = Paths()
    spark = get_spark()
    
    try:
        logger.info(f"Reading Bronze companies: {paths.BRONZE_COMPANIES_DIR}")
        df = spark.read.parquet(paths.BRONZE_COMPANIES_DIR)
        
        # Normalize column names and clean data
        out = df.select(
            trim(col("ticker")).alias("ticker"),
            trim(col("company name")).alias("company_name"),
            trim(col("short name")).alias("short_name"),
            trim(col("industry")).alias("industry"),
            trim(col("sector")).alias("sector"),
            trim(col("exchange")).alias("exchange"),
            regexp_replace(col("market cap"), ",", "").cast(DoubleType()).alias("market_cap"),
            col("website").alias("website"),
        ).withColumn("silver_timestamp", current_timestamp()) \
         .withColumn("layer", lit("silver"))
        
        # Remove duplicates
        out = out.dropDuplicates(["ticker"])
        
        company_count = out.count()
        logger.info(f"Companies processed: {company_count:,}")
        
        # Write (small dataset, no need to coalesce)
        logger.info(f"Writing to Silver: {paths.SILVER_COMPANIES_DIR}")
        out.write.mode("overwrite").parquet(paths.SILVER_COMPANIES_DIR)
        
        logger.info("✓ Company transformation complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error transforming companies: {str(e)}")
        raise
    finally:
        stop_spark(spark)


def transform_prices_bronze_to_silver() -> None:
    logger.info("=" * 70)
    logger.info("SILVER TRANSFORMATION: Price Data (OPTIMIZED)")
    logger.info("=" * 70)
    
    paths = Paths()
    config = PipelineConfig()
    spark = get_spark()
    
    try:
        logger.info(f"Reading Bronze prices: {paths.BRONZE_PRICES_DIR}")
        
        # Read Bronze data
        df = spark.read.parquet(paths.BRONZE_PRICES_DIR)
        
        # Log initial state
        initial_count = df.count()
        logger.info(f"Initial records: {initial_count:,}")
        
        # OPTIMIZATION: Single-pass transformation
        logger.info("Applying transformations in single pass...")
        
        out = df.select(
            # Parse date
            to_date(col("Date"), "yyyy-MM-dd").alias("date"),
            
            # Cast prices to proper types
            col("Open").cast(DoubleType()).alias("open"),
            col("High").cast(DoubleType()).alias("high"),
            col("Low").cast(DoubleType()).alias("low"),
            col("Close").cast(DoubleType()).alias("close"),
            col("Adj Close").cast(DoubleType()).alias("adj_close"),
            
            # Cast volume
            col("Volume").cast(LongType()).alias("volume"),
            
            # Keep ticker
            col("ticker").alias("ticker"),
        )
        
        # Add metadata
        out = out.withColumn("silver_timestamp", current_timestamp()) \
               .withColumn("layer", lit("silver"))
        
        # OPTIMIZATION: Filter nulls efficiently
        logger.info("Removing null values...")
        before_null = out.count()
        out = out.filter(
            col("date").isNotNull() & 
            col("ticker").isNotNull() &
            col("close").isNotNull()
        )
        after_null = out.count()
        logger.info(f"  Removed {before_null - after_null:,} records with null critical fields")
        
        # OPTIMIZATION: Efficient deduplication
        # Use dropDuplicates instead of complex window functions
        logger.info("Removing duplicates...")
        before_dedup = after_null
        out = out.dropDuplicates(["ticker", "date"])
        after_dedup = out.count()
        logger.info(f"  Removed {before_dedup - after_dedup:,} duplicate records")
        
        final_count = after_dedup
        logger.info(f"Final record count: {final_count:,}")
        
        # OPTIMIZATION: Coalesce for efficient writes
        # Calculate optimal number of files
        num_files = max(1, min(config.NUM_OUTPUT_FILES, final_count // config.MAX_RECORDS_PER_FILE))
        logger.info(f"Coalescing to {num_files} output files...")
        
        out = out.coalesce(num_files)
        
        # Write to Silver
        logger.info(f"Writing to Silver: {paths.SILVER_PRICES_DIR}")
        logger.info(f"Expected write time: 5-8 minutes for {final_count:,} records...")
        
        out.write.mode("overwrite") \
           .option("maxRecordsPerFile", config.MAX_RECORDS_PER_FILE) \
           .parquet(paths.SILVER_PRICES_DIR)
        
        logger.info("✓ Price transformation complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error transforming prices: {str(e)}")
        raise
    finally:
        stop_spark(spark)