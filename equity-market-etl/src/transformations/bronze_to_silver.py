from pyspark.sql.functions import col, to_date, trim, regexp_replace
from pyspark.sql.types import DoubleType, LongType
from src.utils.spark_session import get_spark
from src.utils.config import Paths

def transform_companies_bronze_to_silver() ->None:
    p = Paths()
    spark = get_spark()

    df = spark.read.parquet(p.BRONZE_COMPANIES_DIR)

# Normalize column names manually (because your companies CSV has spaces)
# We’ll select + alias the ones we need for joins/analytics.
    out = (
        df.select(
            col("ticker").alias("ticker"),
            col("company name").alias("company_name"),
            col("short name").alias("short_name"),
            col("industry").alias("industry"),
            col("sector").alias("sector"),
            col("exchange").alias("exchange"),
            col("market cap").alias("market_cap"),
            col("website").alias("website"),
        )
        .withColumn("ticker", trim(col("ticker")))
        .withColumn("company_name", trim(col("company_name")))
        .withColumn("sector", trim(col("sector")))
        .withColumn("industry", trim(col("industry")))
# Convert market cap to numeric if it’s in scientific notation or string
        .withColumn("market_cap", regexp_replace(col("market_cap"),",","").cast(DoubleType()))
        .dropDuplicates(["ticker"])
    )

    out.write.mode("overwrite").parquet(p.SILVER_COMPANIES_DIR)
    spark.stop()

def transform_prices_bronze_to_silver() ->None:
    p = Paths()
    spark = get_spark()

    df = spark.read.parquet(p.BRONZE_PRICES_DIR)

    out = (
        df.select(
            to_date(col("Date")).alias("date"),
            col("Open").cast(DoubleType()).alias("open"),
            col("High").cast(DoubleType()).alias("high"),
            col("Low").cast(DoubleType()).alias("low"),
            col("Close").cast(DoubleType()).alias("close"),
            col("Adj Close").cast(DoubleType()).alias("adj_close"),
            col("Volume").cast(LongType()).alias("volume"),
            col("ticker").alias("ticker"),
        )
        .dropna(subset=["date","ticker"])
        .dropDuplicates(["ticker","date"])
    )

    out.write.mode("overwrite").partitionBy("ticker").parquet(p.SILVER_PRICES_DIR)
    spark.stop()

