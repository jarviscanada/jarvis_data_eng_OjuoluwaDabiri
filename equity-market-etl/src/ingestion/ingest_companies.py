
from pyspark.sql.functions import current_timestamp, lit
from src.utils.spark_session import get_spark, stop_spark
from src.utils.config import Paths
import logging

logger = logging.getLogger(__name__)

def ingest_companies_to_bronze() -> None:
    logger.info("=" * 70)
    logger.info("BRONZE INGESTION: Company Data (OPTIMIZED)")
    logger.info("=" * 70)
    
    paths = Paths()
    spark = get_spark()
    
    try:
        logger.info(f"Reading CSV: {paths.RAW_COMPANIES_CSV}")
        
        df = spark.read.csv(
            paths.RAW_COMPANIES_CSV,
            header=True,
            inferSchema=True,
            multiLine=True,
            escape='"'
        )
        
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("layer", lit("bronze"))
        
        company_count = df.count()
        logger.info(f"Companies to ingest: {company_count:,}")
        
        logger.info(f"Writing to Bronze: {paths.BRONZE_COMPANIES_DIR}")
        df.write.mode("overwrite").parquet(paths.BRONZE_COMPANIES_DIR)
        
        logger.info("✓ Company ingestion complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error ingesting companies: {str(e)}")
        raise
    finally:
        stop_spark(spark)