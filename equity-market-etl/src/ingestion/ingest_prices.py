
from src.utils.spark_session import get_spark
from src.utils.config import Paths

def ingest_prices_to_bronze() ->None:
    from pyspark.sql.functions import input_file_name, regexp_extract, col
    p = Paths()
    spark = get_spark()

# Read all CSVs under directory
    df = (
        spark.read
        .option("header",True)
        .csv(f"{p.RAW_PRICES_DIR}/*.csv")
    )

# Extract ticker from filename like ".../AAPL.csv"
    df = df.withColumn("source_file", input_file_name())
    df = df.withColumn("ticker", regexp_extract(col("source_file"),r"([^/]+)\.csv$",1))

# Land bronze partitioned by ticker (good scalability)
    (
        df.drop("source_file")
        .write.mode("overwrite")
        .partitionBy("ticker")
        .parquet(p.BRONZE_PRICES_DIR)
    )

    spark.stop()

