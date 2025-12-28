from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, stddev, log
from pyspark.sql.types import DoubleType
from src.utils.spark_session import get_spark
from src.utils.config import Paths

def create_gold_enriched() ->None:
    p = Paths()
    spark = get_spark()

    prices = spark.read.parquet(p.SILVER_PRICES_DIR)
    companies = spark.read.parquet(p.SILVER_COMPANIES_DIR)

# Join prices with company metadata
    enriched = (
        prices.join(companies, on="ticker", how="left")
    )

# Daily returns using adj_close (robust for corporate actions if available)
    w = Window.partitionBy("ticker").orderBy("date")
    enriched = enriched.withColumn("prev_adj_close", lag(col("adj_close"),1).over(w))
    enriched = enriched.withColumn(
"daily_return",
        ((col("adj_close") / col("prev_adj_close")) -1).cast(DoubleType())
    ).drop("prev_adj_close")

    enriched.write.mode("overwrite").partitionBy("ticker").parquet(p.GOLD_ENRICHED_DIR)
    spark.stop()

def create_gold_analytics() ->None:
    """
    Example aggregate tables:
    - sector_daily_avg_return
    - sector_30d_volatility
    """
    p = Paths()
    spark = get_spark()

    df = spark.read.parquet(p.GOLD_ENRICHED_DIR).dropna(subset=["daily_return","sector","date"])

# Sector daily average return
    sector_daily = (
        df.groupBy("date","sector")
        .agg(avg(col("daily_return")).alias("avg_daily_return"))
    )

    sector_daily.write.mode("overwrite").parquet(f"{p.GOLD_ANALYTICS_DIR}/sector_daily_avg_return")

    spark.stop()

