"""
Unit tests for configuration module.
"""
import pytest
import os
from unittest.mock import patch
from pipeline.config import DatabaseConfig, PipelineConfig, Config


class TestDatabaseConfig:
    """Test suite for database configuration."""
    
    def test_database_config_defaults(self):
        """Test default database configuration values."""
        config = DatabaseConfig()
        
        assert config.host == 'postgres'
        assert config.port == 5432
        assert config.user == 'airflow'
        assert config.password == 'airflow'
        assert config.database == 'airflow'
    
    def test_database_config_connection_string(self):
        """Test connection string generation."""
        config = DatabaseConfig(
            host='localhost',
            port=5433,
            user='testuser',
            password='testpass',
            database='testdb'
        )
        
        expected = 'postgresql://testuser:testpass@localhost:5433/testdb'
        assert config.connection_string == expected
    
    def test_database_config_custom_values(self):
        """Test custom database configuration."""
        config = DatabaseConfig(
            host='custom-host',
            port=9999,
            user='custom-user',
            password='custom-pass',
            database='custom-db'
        )
        
        assert config.host == 'custom-host'
        assert config.port == 9999


class TestPipelineConfig:
    """Test suite for pipeline configuration."""
    
    def test_pipeline_config_defaults(self):
        """Test default pipeline configuration values."""
        config = PipelineConfig()
        
        assert config.batch_size == 1000
        assert config.max_retries == 3
        assert config.retry_delay_seconds == 60
        assert config.data_retention_days == 90
    
    def test_pipeline_config_custom_values(self):
        """Test custom pipeline configuration."""
        config = PipelineConfig(
            batch_size=500,
            max_retries=5,
            retry_delay_seconds=30,
            data_retention_days=180
        )
        
        assert config.batch_size == 500
        assert config.max_retries == 5
        assert config.retry_delay_seconds == 30
        assert config.data_retention_days == 180


class TestConfig:
    """Test suite for main configuration."""
    
    @patch.dict(os.environ, {
        'DB_HOST': 'test-host',
        'DB_PORT': '5555',
        'DB_USER': 'test-user',
        'DB_PASSWORD': 'test-pass',
        'DB_NAME': 'test-db',
        'BATCH_SIZE': '2000',
        'MAX_RETRIES': '5',
        'ENVIRONMENT': 'production'
    })
    def test_config_from_env(self):
        """Test loading configuration from environment variables."""
        config = Config.from_env()
        
        assert config.database.host == 'test-host'
        assert config.database.port == 5555
        assert config.database.user == 'test-user'
        assert config.pipeline.batch_size == 2000
        assert config.pipeline.max_retries == 5
        assert config.environment == 'production'
    
    @patch.dict(os.environ, {}, clear=True)
    def test_config_from_env_defaults(self):
        """Test that defaults are used when env vars are not set."""
        config = Config.from_env()
        
        assert config.database.host == 'postgres'
        assert config.database.port == 5432
        assert config.pipeline.batch_size == 1000
        assert config.environment == 'development'
