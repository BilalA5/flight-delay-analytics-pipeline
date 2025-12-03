"""
Unit tests for data quality checks.
"""
import pytest
from unittest.mock import Mock, patch
from pipeline.quality_checks import check_null_values, check_duplicate_records


class TestQualityChecks:
    """Test suite for quality check functions."""
    
    @patch('pipeline.quality_checks.get_db_connection')
    def test_check_null_values_pass(self, mock_conn):
        """Test null check passes with clean data."""
        mock_engine = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1000, 0, 0, 0, 0)
        mock_engine.connect().__enter__().execute.return_value = mock_result
        mock_conn.return_value = mock_engine
        
        result = check_null_values()
        assert result is True
    
    @patch('pipeline.quality_checks.get_db_connection')
    def test_check_null_values_fail(self, mock_conn):
        """Test null check fails with null values."""
        mock_engine = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1000, 5, 0, 0, 0)
        mock_engine.connect().__enter__().execute.return_value = mock_result
        mock_conn.return_value = mock_engine
        
        result = check_null_values()
        assert result is False
    
    @patch('pipeline.quality_checks.get_db_connection')
    def test_check_duplicates_pass(self, mock_conn):
        """Test duplicate check passes with no duplicates."""
        mock_engine = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (0,)
        mock_engine.connect().__enter__().execute.return_value = mock_result
        mock_conn.return_value = mock_engine
        
        result = check_duplicate_records()
        assert result is True
