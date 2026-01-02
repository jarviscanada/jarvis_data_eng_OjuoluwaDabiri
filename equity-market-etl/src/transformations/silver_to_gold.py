from pyspark.sql.functions import (
    col, lag, when, current_timestamp, lit, broadcast,
    avg, sum as spark_sum, min as spark_min, max as spark_max,
    count, stddev, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from src.utils.spark_session import get_spark, stop_spark
from src.utils.config import Paths, PipelineConfig
import logging

logger = logging.getLogger(__name__)


def create_gold_enriched() -> None:

    logger.info("=" * 70)
    logger.info("GOLD ENRICHMENT: Prices + Company Data")
    logger.info("=" * 70)
    
    paths = Paths()
    config = PipelineConfig()
    spark = get_spark()
    
    try:
        # Read Silver layers
        logger.info("Reading Silver price data...")
        prices_raw = spark.read.parquet(paths.SILVER_PRICES_DIR)
        
        # FIX: Select only the data columns we need (drop all Silver metadata)
        logger.info("Selecting price data columns (dropping Silver metadata)...")
        prices = prices_raw.select(
            "ticker", "date", "open", "high", "low", "close", 
            "adj_close", "volume"
        )
        
        logger.info("Reading Silver company data...")
        companies = spark.read.parquet(paths.SILVER_COMPANIES_DIR)
        
        price_count = prices.count()
        company_count = companies.count()
        
        logger.info(f"Prices: {price_count:,} records")
        logger.info(f"Companies: {company_count:,} records")
        
        # FIX: Drop metadata columns from companies before join
        # to avoid duplicate column errors
        logger.info("Preparing data for join...")
        companies_for_join = companies.select(
            "ticker",
            "company_name",
            "short_name",
            "industry",
            "sector",
            "exchange",
            "market_cap",
            "website"
        )
        
        # OPTIMIZATION: Broadcast small table (companies)
        logger.info("Joining with broadcast (company table is small)...")
        enriched = prices.join(
            broadcast(companies_for_join),
            on="ticker",
            how="left"
        )
        
        # Calculate derived metrics
        logger.info("Calculating daily returns...")
        window_spec = Window.partitionBy("ticker").orderBy("date")
        
        enriched = enriched.withColumn(
            "prev_adj_close",
            lag("adj_close", 1).over(window_spec)
        )
        
        enriched = enriched.withColumn(
            "daily_return",
            when(
                col("prev_adj_close").isNotNull(),
                ((col("adj_close") - col("prev_adj_close")) / col("prev_adj_close") * 100)
            ).otherwise(None).cast(DoubleType())
        ).drop("prev_adj_close")
        
        # Calculate intraday metrics
        enriched = enriched.withColumn(
            "intraday_range",
            (col("high") - col("low")).cast(DoubleType())
        )
        
        enriched = enriched.withColumn(
            "range_pct",
            ((col("intraday_range") / col("close")) * 100).cast(DoubleType())
        )
        
        # Add Gold layer metadata
        enriched = enriched.withColumn("gold_timestamp", current_timestamp()) \
                           .withColumn("gold_layer", lit("gold"))
        
        # Get final count
        final_count = enriched.count()
        logger.info(f"Enriched records: {final_count:,}")
        
        # OPTIMIZATION: Coalesce before write
        num_files = max(1, min(config.NUM_OUTPUT_FILES, final_count // config.MAX_RECORDS_PER_FILE))
        logger.info(f"Coalescing to {num_files} output files...")
        
        enriched = enriched.coalesce(num_files)
        
        # Write to Gold
        logger.info(f"Writing to Gold: {paths.GOLD_ENRICHED_DIR}")
        logger.info("Expected write time: 8-10 minutes...")
        
        enriched.write.mode("overwrite") \
                .option("maxRecordsPerFile", config.MAX_RECORDS_PER_FILE) \
                .parquet(paths.GOLD_ENRICHED_DIR)
        
        logger.info("✓ Gold enrichment complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error creating gold enriched data: {str(e)}")
        raise
    finally:
        stop_spark(spark)


def create_gold_analytics() -> None:
    
    logger.info("=" * 70)
    logger.info("GOLD ANALYTICS: Pre-computed Tables")
    logger.info("=" * 70)
    
    paths = Paths()
    config = PipelineConfig()
    spark = get_spark()
    
    try:
        # Read enriched Gold data
        logger.info("Reading enriched Gold data...")
        df = spark.read.parquet(paths.GOLD_ENRICHED_DIR)
        
        # 1. Stock Performance Summary
        logger.info("Creating stock performance summary...")
        stock_summary = df.groupBy("ticker", "company_name", "sector", "industry").agg(
            count("date").alias("num_trading_days"),
            avg("close").alias("avg_price"),
            spark_min("close").alias("min_price"),
            spark_max("close").alias("max_price"),
            avg("volume").alias("avg_volume"),
            spark_sum("volume").alias("total_volume"),
            avg("daily_return").alias("avg_daily_return"),
            stddev("daily_return").alias("volatility"),
            spark_max("daily_return").alias("best_day"),
            spark_min("daily_return").alias("worst_day")
        )
        
        # Round metrics
        for col_name in ["avg_price", "min_price", "max_price", "avg_daily_return", 
                        "volatility", "best_day", "worst_day"]:
            stock_summary = stock_summary.withColumn(
                col_name, 
                spark_round(col(col_name), 4)
            )
        
        logger.info("Writing stock performance summary...")
        stock_summary.coalesce(1).write.mode("overwrite") \
            .parquet(f"{paths.GOLD_ANALYTICS_DIR}/stock_performance_summary")
        
        # 2. Sector Daily Returns
        logger.info("Creating sector daily returns...")
        sector_daily = df.groupBy("sector", "date").agg(
            count("ticker").alias("num_stocks"),
            avg("close").alias("avg_price"),
            avg("daily_return").alias("avg_daily_return"),
            stddev("daily_return").alias("return_volatility"),
            spark_sum("volume").alias("total_volume")
        )
        
        # Round metrics
        for col_name in ["avg_price", "avg_daily_return", "return_volatility"]:
            sector_daily = sector_daily.withColumn(
                col_name,
                spark_round(col(col_name), 4)
            )
        
        logger.info("Writing sector daily returns...")
        sector_daily.coalesce(5).write.mode("overwrite") \
            .parquet(f"{paths.GOLD_ANALYTICS_DIR}/sector_daily_returns")
        
        # 3. Industry Volatility
        logger.info("Creating industry volatility rankings...")
        industry_vol = df.groupBy("industry").agg(
            count("ticker").alias("num_stocks"),
            avg("daily_return").alias("avg_return"),
            stddev("daily_return").alias("volatility"),
            spark_min("daily_return").alias("min_return"),
            spark_max("daily_return").alias("max_return")
        )
        
        # Calculate risk-adjusted return
        industry_vol = industry_vol.withColumn(
            "risk_adjusted_return",
            when(
                col("volatility") > 0,
                col("avg_return") / col("volatility")
            ).otherwise(None)
        )
        
        # Round metrics
        for col_name in ["avg_return", "volatility", "min_return", "max_return", "risk_adjusted_return"]:
            industry_vol = industry_vol.withColumn(
                col_name,
                spark_round(col(col_name), 4)
            )
        
        logger.info("Writing industry volatility...")
        industry_vol.coalesce(1).write.mode("overwrite") \
            .parquet(f"{paths.GOLD_ANALYTICS_DIR}/industry_volatility")
        
        logger.info("✓ All analytics tables created")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error creating analytics: {str(e)}")
        raise
    finally:
        stop_spark(spark)