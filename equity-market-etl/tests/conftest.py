import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType
from datetime import date
import tempfile
import shutil


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("etl-tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def bronze_schema():
    """Bronze layer schema"""
    return StructType([
        StructField("ticker", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ])


@pytest.fixture
def sample_data():
    """Sample test data"""
    return [
        ("AAPL", date(2024, 1, 1), 150.0, 155.0, 149.0, 154.0, 154.0, 1000000),
        ("AAPL", date(2024, 1, 2), 154.0, 158.0, 153.0, 157.0, 157.0, 1200000),
        ("GOOGL", date(2024, 1, 1), 140.0, 145.0, 139.0, 144.0, 144.0, 2000000),
    ]


@pytest.fixture
def temp_dir():
    """Temporary directory for test data"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)