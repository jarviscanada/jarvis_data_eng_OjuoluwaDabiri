from pyspark.sql import SparkSession
from src.utils.config import PipelineConfig
import logging

logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    config = PipelineConfig()
    
    logger.info("Creating optimized Spark session...")
    logger.info(f"  Master: {config.SPARK_MASTER}")
    logger.info(f"  Driver Memory: {config.DRIVER_MEMORY}")
    logger.info(f"  Executor Memory: {config.EXECUTOR_MEMORY}")
    
    spark = (
        SparkSession.builder
        .appName(config.APP_NAME)
        .master(config.SPARK_MASTER)
        
        # Memory configuration
        .config("spark.driver.memory", config.DRIVER_MEMORY)
        .config("spark.executor.memory", config.EXECUTOR_MEMORY)
        
        # Shuffle and parallelism optimization
        .config("spark.sql.shuffle.partitions", config.SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", config.DEFAULT_PARALLELISM)
        
        # Adaptive query execution (Spark 3.0+)
        .config("spark.sql.adaptive.enabled", config.ADAPTIVE_EXECUTION)
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        # File handling optimization
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
        .config("spark.sql.files.openCostInBytes", "4194304")  # 4MB
        
        # Arrow optimization for pandas/numpy conversion
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        
        # Compression
        .config("spark.sql.parquet.compression.codec", "snappy")  # Fast compression
        
        # Memory management
        .config("spark.memory.fraction", "0.8")  # 80% for execution/storage
        .config("spark.memory.storageFraction", "0.3")  # 30% of that for caching
        
        # Broadcast join optimization
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
        
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✓ Spark session created successfully")
    
    return spark


def stop_spark(spark: SparkSession) -> None:
    """
    Gracefully stop Spark session
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()
        logger.info("✓ Spark session stopped")