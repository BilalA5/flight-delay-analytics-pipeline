"""
Custom exceptions for the pipeline.
"""


class PipelineException(Exception):
    """Base exception for pipeline errors."""
    pass


class DataIngestionError(PipelineException):
    """Raised when data ingestion fails."""
    pass


class DataTransformationError(PipelineException):
    """Raised when data transformation fails."""
    pass


class DataQualityError(PipelineException):
    """Raised when data quality checks fail."""
    pass


class DatabaseConnectionError(PipelineException):
    """Raised when database connection fails."""
    pass


class ConfigurationError(PipelineException):
    """Raised when configuration is invalid."""
    pass
