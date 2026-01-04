from pyspark.sql.functions import input_file_name, regexp_extract, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from src.utils.spark_session import get_spark, stop_spark
from src.utils.config import Paths, PipelineConfig
import logging

logger = logging.getLogger(__name__)


def define_price_schema():
    
    return StructType([
        StructField("Ticker", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Adj Close", DoubleType(), True),
        StructField("Volume", LongType(), True)
    ])


def ingest_prices_to_bronze() -> None:
    
    logger.info("=" * 70)
    logger.info("BRONZE INGESTION: Price Data")
    logger.info("=" * 70)
    
    paths = Paths()
    config = PipelineConfig()
    spark = get_spark()
    
    try:
        logger.info(f"Reading CSV files from: {paths.RAW_PRICES_DIR}")
        schema = define_price_schema()
        
        # Read all CSVs with explicit schema (faster than inferSchema)
        df = spark.read.csv(
            f"{paths.RAW_PRICES_DIR}/*.csv",
            header=True,
            schema=schema,
            mode="PERMISSIVE"  # Handle malformed records
        )
        
        # Add metadata
        df = df.withColumn("source_file", input_file_name()) \
               .withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("layer", lit("bronze"))
        
        # Extract ticker from filename
        df = df.withColumn(
            "ticker",
            regexp_extract(col("source_file"), r"([A-Z]+)\.csv", 1)
        )
        
        # Count before write (triggers computation once)
        record_count = df.count()
        ticker_count = df.select("ticker").distinct().count()
        
        logger.info(f"Records to ingest: {record_count:,}")
        logger.info(f"Unique tickers: {ticker_count:,}")
        
        # OPTIMIZATION: Coalesce to reduce number of output files
        # Target: ~100MB per file = ~500K records
        num_files = max(1, min(config.NUM_OUTPUT_FILES, record_count // config.MAX_RECORDS_PER_FILE))
        logger.info(f"Coalescing to {num_files} output files...")
        
        df = df.coalesce(num_files)
        
        # OPTIMIZATION: Write WITHOUT partitioning in Bronze
        # Partitioning adds overhead; we'll partition in Silver if needed
        logger.info(f"Writing to Bronze: {paths.BRONZE_PRICES_DIR}")
        logger.info("This should take 2-3 minutes...")
        
        df.write.mode("overwrite") \
          .option("maxRecordsPerFile", config.MAX_RECORDS_PER_FILE) \
          .parquet(paths.BRONZE_PRICES_DIR)
        
        logger.info("✓ Bronze price data ingestion complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error during bronze ingestion: {str(e)}")
        raise
    finally:
        stop_spark(spark)