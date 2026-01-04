import pytest
from pyspark.sql.functions import col, lit, current_timestamp, lag, when
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from datetime import date


# ============================================================================
# UNIT TESTS - Bronze Layer
# ============================================================================

class TestBronzeLayer:
    """Unit tests for Bronze layer"""
    
    def test_schema_validation(self, spark, bronze_schema, sample_data):
        """Test Bronze schema is correct"""
        df = spark.createDataFrame(sample_data, bronze_schema)
        
        # Check columns exist
        expected_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        assert df.columns == expected_cols
    
    def test_no_null_in_critical_fields(self, spark, bronze_schema, sample_data):
        """Test ticker and date have no nulls"""
        df = spark.createDataFrame(sample_data, bronze_schema)
        
        null_tickers = df.filter(col("ticker").isNull()).count()
        null_dates = df.filter(col("date").isNull()).count()
        
        assert null_tickers == 0, "Found null tickers"
        assert null_dates == 0, "Found null dates"
    
    def test_positive_prices(self, spark, bronze_schema, sample_data):
        """Test prices are positive"""
        df = spark.createDataFrame(sample_data, bronze_schema)
        
        invalid = df.filter((col("close").isNotNull()) & (col("close") <= 0)).count()
        assert invalid == 0, "Found non-positive prices"


# ============================================================================
# UNIT TESTS - Silver Layer
# ============================================================================

class TestSilverLayer:
    """Unit tests for Silver layer"""
    
    def test_remove_duplicates(self, spark, bronze_schema):
        """Test duplicate removal"""
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),  # Duplicate
            ("GOOGL", date(2024, 1, 1), 140.0, 145.0, 139.0, 144.0, 144.0, 2000000),
        ]
        df = spark.createDataFrame(data, bronze_schema)
        
        # Remove duplicates
        cleaned = df.dropDuplicates(['ticker', 'date'])
        
        assert cleaned.count() == 2, "Should have 2 unique records"
    
    def test_drop_null_values(self, spark, bronze_schema):
        """Test null value removal"""
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),
            ("AAPL", date(2024, 1, 2), 154.0, 158.0, 153.0, None, 157.0, 1200000),  # Null close
        ]
        df = spark.createDataFrame(data, bronze_schema)
        
        # Drop nulls
        cleaned = df.na.drop(subset=['close'])
        
        assert cleaned.count() == 1, "Should drop record with null close"
    
    def test_add_metadata(self, spark, bronze_schema, sample_data):
        """Test metadata addition"""
        df = spark.createDataFrame(sample_data, bronze_schema)
        
        # Add metadata
        with_metadata = df \
            .withColumn("layer", lit("silver")) \
            .withColumn("timestamp", current_timestamp())
        
        assert "layer" in with_metadata.columns
        assert "timestamp" in with_metadata.columns


# ============================================================================
# UNIT TESTS - Gold Layer
# ============================================================================

class TestGoldLayer:
    """Unit tests for Gold layer"""
    
    def test_daily_return_calculation(self, spark, bronze_schema):
        """Test daily return calculation"""
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 150.0, 100.0, 1000000),
            ("AAPL", date(2024, 1, 2), 154.0, 158.0, 153.0, 157.0, 105.0, 1200000),  # 5% return
        ]
        df = spark.createDataFrame(data, bronze_schema)
        
        # Calculate daily return
        window_spec = Window.partitionBy("ticker").orderBy("date")
        df_with_return = df.withColumn(
            "prev_close",
            lag("adj_close", 1).over(window_spec)
        ).withColumn(
            "daily_return",
            when(
                col("prev_close").isNotNull(),
                ((col("adj_close") - col("prev_close")) / col("prev_close") * 100)
            ).otherwise(None).cast(DoubleType())
        )
        
        # Check second day has 5% return
        day2 = df_with_return.filter(col("date") == date(2024, 1, 2)).first()
        assert abs(day2.daily_return - 5.0) < 0.01, "Return should be ~5%"
    
    def test_intraday_range(self, spark, bronze_schema, sample_data):
        """Test intraday range calculation"""
        df = spark.createDataFrame(sample_data, bronze_schema)
        
        # Calculate range
        df_with_range = df.withColumn(
            "intraday_range",
            (col("high") - col("low")).cast(DoubleType())
        )
        
        # First record: 155 - 149 = 6.0
        result = df_with_range.first()
        assert abs(result.intraday_range - 6.0) < 0.01


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestEndToEnd:
    """Integration tests for complete pipeline"""
    
    def test_bronze_to_silver_flow(self, spark, bronze_schema, temp_dir):
        """Test Bronze to Silver transformation"""
        # Create Bronze data with issues
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),  # Dup
            ("AAPL", date(2024, 1, 2), 154.0, 158.0, 153.0, None, 157.0, 1200000),  # Null
            ("GOOGL", date(2024, 1, 1), 140.0, 145.0, 139.0, 144.0, 144.0, 2000000),
        ]
        bronze_df = spark.createDataFrame(data, bronze_schema)
        
        # Save Bronze
        bronze_path = f"{temp_dir}/bronze/prices"
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        
        # Read and transform to Silver
        bronze_read = spark.read.parquet(bronze_path)
        silver_df = bronze_read \
            .dropDuplicates(['ticker', 'date']) \
            .na.drop(subset=['close']) \
            .withColumn("layer", lit("silver"))
        
        # Save Silver
        silver_path = f"{temp_dir}/silver/prices"
        silver_df.write.mode("overwrite").parquet(silver_path)
        
        # Verify
        silver_read = spark.read.parquet(silver_path)
        assert silver_read.count() == 2, "Should have 2 clean records"
        assert "layer" in silver_read.columns
    
    def test_full_pipeline(self, spark, bronze_schema, temp_dir):
        """Test complete Bronze → Silver → Gold pipeline"""
        # Bronze data
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 100.0, 1000000),
            ("AAPL", date(2024, 1, 2), 154.0, 158.0, 153.0, 157.0, 105.0, 1200000),
        ]
        bronze_df = spark.createDataFrame(data, bronze_schema)
        
        # Silver transformation
        silver_df = bronze_df \
            .dropDuplicates(['ticker', 'date']) \
            .na.drop(subset=['close', 'volume'])
        
        # Gold transformation
        window_spec = Window.partitionBy("ticker").orderBy("date")
        gold_df = silver_df.withColumn(
            "prev_close",
            lag("adj_close", 1).over(window_spec)
        ).withColumn(
            "daily_return",
            when(
                col("prev_close").isNotNull(),
                ((col("adj_close") - col("prev_close")) / col("prev_close") * 100)
            ).otherwise(None).cast(DoubleType())
        ).withColumn(
            "intraday_range",
            (col("high") - col("low")).cast(DoubleType())
        ).drop("prev_close")
        
        # Save Gold
        gold_path = f"{temp_dir}/gold/prices"
        gold_df.write.mode("overwrite").parquet(gold_path)
        
        # Verify
        gold_read = spark.read.parquet(gold_path)
        assert gold_read.count() == 2
        assert "daily_return" in gold_read.columns
        assert "intraday_range" in gold_read.columns
        
        # Verify first day has null return
        day1 = gold_read.filter(col("date") == date(2024, 1, 1)).first()
        assert day1.daily_return is None, "First day should have null return"
        
        # Verify second day has calculated return
        day2 = gold_read.filter(col("date") == date(2024, 1, 2)).first()
        assert day2.daily_return is not None, "Second day should have return"
        assert abs(day2.daily_return - 5.0) < 0.01, "Return should be ~5%"
    
    def test_data_quality_improvement(self, spark, bronze_schema):
        """Test that Silver improves quality over Bronze"""
        # Bronze with known issues
        data = [
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),
            ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),  # Dup
            ("AAPL", date(2024, 1, 2), None, None, None, None, None, None),  # Nulls
        ]
        bronze_df = spark.createDataFrame(data, bronze_schema)
        
        # Count issues in Bronze
        bronze_dupes = bronze_df.groupBy("ticker", "date").count() \
            .filter(col("count") > 1).count()
        bronze_nulls = bronze_df.filter(col("close").isNull()).count()
        
        # Silver transformation
        silver_df = bronze_df \
            .dropDuplicates(['ticker', 'date']) \
            .na.drop(subset=['close'])
        
        # Count issues in Silver
        silver_dupes = silver_df.groupBy("ticker", "date").count() \
            .filter(col("count") > 1).count()
        silver_nulls = silver_df.filter(col("close").isNull()).count()
        
        # Verify improvement
        assert bronze_dupes > 0, "Bronze should have duplicates"
        assert bronze_nulls > 0, "Bronze should have nulls"
        assert silver_dupes == 0, "Silver should have no duplicates"
        assert silver_nulls == 0, "Silver should have no nulls"