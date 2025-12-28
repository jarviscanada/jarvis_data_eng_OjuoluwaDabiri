from src.utils.spark_session import get_spark
from src.utils.config import Paths


def ingest_companies_to_bronze() -> None:
    from pyspark.sql.functions import lit
    p = Paths()
    spark = get_spark()

    df = (
        spark.read
        .option("header",True)
        .option("multiLine",True)
        .option("escape","\"")
        .csv(p.RAW_COMPANIES_CSV)
    )

    # Keep raw as-is (bronze), but ensure ticker exists
    if "ticker" not in [c.lower() for c in df.columns]:
        # dataset column appears as 'ticker' in your sample; this is defensive
        df = df.withColumn("ticker", lit(None).cast("string"))

    df.write.mode("overwrite").parquet(p.BRONZE_COMPANIES_DIR)
    spark.stop()

