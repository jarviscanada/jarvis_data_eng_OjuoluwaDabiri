
from src.utils.config import PipelineConfig

def get_spark():
    from pyspark.sql import SparkSession
    cfg = PipelineConfig()
    spark = (
        SparkSession.builder
        .appName(cfg.APP_NAME)
        .master(cfg.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", cfg.SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

