"""
Unit tests for configuration module
"""

import pytest
from src.utils.config import Paths, PipelineConfig


class TestPaths:
    """Test Paths dataclass"""
    
    def test_paths_initialization(self):
        """Test that Paths can be initialized with defaults"""
        p = Paths()
        assert p.RAW_PRICES_DIR is not None
        assert p.RAW_COMPANIES_CSV is not None
        assert p.BRONZE_COMPANIES_DIR is not None
        assert p.BRONZE_PRICES_DIR is not None
        assert p.SILVER_COMPANIES_DIR is not None
        assert p.SILVER_PRICES_DIR is not None
        assert p.GOLD_ENRICHED_DIR is not None
        assert p.GOLD_ANALYTICS_DIR is not None
    
    def test_paths_format(self):
        """Test that paths have expected format"""
        p = Paths()
        
        # Raw paths should start with /opt/data or be absolute
        assert p.RAW_PRICES_DIR.startswith("/opt/data") or p.RAW_PRICES_DIR.startswith("/")
        
        # Company CSV should end with .csv
        assert p.RAW_COMPANIES_CSV.endswith(".csv")
        
        # Bronze paths should contain 'bronze'
        assert "bronze" in p.BRONZE_PRICES_DIR.lower()
        assert "bronze" in p.BRONZE_COMPANIES_DIR.lower()
        
        # Silver paths should contain 'silver'
        assert "silver" in p.SILVER_PRICES_DIR.lower()
        assert "silver" in p.SILVER_COMPANIES_DIR.lower()
        
        # Gold paths should contain 'gold'
        assert "gold" in p.GOLD_ENRICHED_DIR.lower()
        assert "gold" in p.GOLD_ANALYTICS_DIR.lower()
    
    def test_paths_immutable(self):
        """Test that Paths dataclass is frozen (immutable)"""
        p = Paths()
        
        with pytest.raises(AttributeError):
            p.RAW_PRICES_DIR = "/new/path"


class TestPipelineConfig:
    """Test PipelineConfig dataclass"""
    
    def test_config_initialization(self):
        """Test that PipelineConfig can be initialized"""
        cfg = PipelineConfig()
        assert cfg.APP_NAME is not None
        assert cfg.SPARK_MASTER is not None
        assert cfg.SHUFFLE_PARTITIONS is not None
        assert cfg.ADAPTIVE_EXECUTION is not None
    
    def test_default_values(self):
        """Test default configuration values"""
        cfg = PipelineConfig()
        
        assert cfg.APP_NAME == "equity-market-etl"
        assert cfg.SPARK_MASTER == "local[*]"
        assert cfg.SHUFFLE_PARTITIONS == "8"
        assert cfg.ADAPTIVE_EXECUTION == "true"
    
    def test_config_immutable(self):
        """Test that PipelineConfig is frozen"""
        cfg = PipelineConfig()
        
        with pytest.raises(AttributeError):
            cfg.APP_NAME = "new-app"
    
    def test_shuffle_partitions_numeric(self):
        """Test that shuffle partitions is a valid number string"""
        cfg = PipelineConfig()
        assert cfg.SHUFFLE_PARTITIONS.isdigit()
        assert int(cfg.SHUFFLE_PARTITIONS) > 0


class TestConfigIntegration:
    """Integration tests for configuration"""
    
    def test_paths_and_config_compatibility(self):
        """Test that Paths and PipelineConfig work together"""
        p = Paths()
        cfg = PipelineConfig()
        
        # Should be able to create both without errors
        assert p is not None
        assert cfg is not None
    
    def test_environment_variable_override(self, monkeypatch):
        """Test that environment variables can override defaults"""
        # Set environment variable
        monkeypatch.setenv("SPARK_APP_NAME", "test-app")
        
        # Create new config (should pick up env var)
        from src.utils.config import PipelineConfig
        cfg = PipelineConfig()
        
        assert cfg.APP_NAME == "test-app"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])