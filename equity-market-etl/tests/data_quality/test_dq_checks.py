import os
import pytest
from src.validation.data_quality_checks import bronze_checks, silver_checks, gold_checks, DataQualityError

@pytest.mark.skipif(not os.path.exists("/opt/data/bronze"), reason="Bronze layer not built yet")
def test_bronze_checks():
    bronze_checks()

@pytest.mark.skipif(not os.path.exists("/opt/data/silver"), reason="Silver layer not built yet")
def test_silver_checks():
    silver_checks()

@pytest.mark.skipif(not os.path.exists("/opt/data/gold"), reason="Gold layer not built yet")
def test_gold_checks():
    gold_checks()

