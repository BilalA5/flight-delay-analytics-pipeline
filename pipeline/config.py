"""
Configuration management for the pipeline.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = 'postgres'
    port: int = 5432
    user: str = 'airflow'
    password: str = 'airflow'
    database: str = 'airflow'
    
    @property
    def connection_string(self) -> str:
        """Get SQLAlchemy connection string."""
        return f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'


@dataclass
class PipelineConfig:
    """Pipeline configuration."""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay_seconds: int = 60
    data_retention_days: int = 90


@dataclass
class Config:
    """Main configuration class."""
    database: DatabaseConfig
    pipeline: PipelineConfig
    environment: str = 'development'
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        db_config = DatabaseConfig(
            host=os.getenv('DB_HOST', 'postgres'),
            port=int(os.getenv('DB_PORT', '5432')),
            user=os.getenv('DB_USER', 'airflow'),
            password=os.getenv('DB_PASSWORD', 'airflow'),
            database=os.getenv('DB_NAME', 'airflow')
        )
        
        pipeline_config = PipelineConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay_seconds=int(os.getenv('RETRY_DELAY', '60')),
            data_retention_days=int(os.getenv('DATA_RETENTION_DAYS', '90'))
        )
        
        return cls(
            database=db_config,
            pipeline=pipeline_config,
            environment=os.getenv('ENVIRONMENT', 'development')
        )


# Global config instance
config = Config.from_env()
