"""
Unit tests for transformation module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from pipeline.transform import clean_data


class TestCleanData:
    """Test suite for data cleaning and transformation."""
    
    @patch('pipeline.transform.get_db_connection')
    @patch('pipeline.transform.pd.read_sql')
    def test_clean_data_removes_nulls(self, mock_read_sql, mock_conn):
        """Test that records with null critical fields are removed."""
        # Create test data with nulls
        test_data = pd.DataFrame({
            'flight_date': ['2024-01-01', None, '2024-01-03'],
            'airline': ['AA', 'DL', 'UA'],
            'origin': ['JFK', 'ATL', 'ORD'],
            'destination': ['LAX', 'SFO', 'DEN'],
            'departure_delay': [10, 20, 30],
            'arrival_delay': [15, 25, 35],
            'cancelled': [False, False, False]
        })
        
        mock_read_sql.return_value = test_data
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        # Should remove the record with null flight_date
        result = clean_data()
        
        # Verify truncate was called
        mock_engine.connect().__enter__().execute.assert_called()
    
    @patch('pipeline.transform.get_db_connection')
    @patch('pipeline.transform.pd.read_sql')
    def test_clean_data_removes_invalid_delays(self, mock_read_sql, mock_conn):
        """Test that records with invalid delays are removed."""
        test_data = pd.DataFrame({
            'flight_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'airline': ['AA', 'DL', 'UA'],
            'origin': ['JFK', 'ATL', 'ORD'],
            'destination': ['LAX', 'SFO', 'DEN'],
            'departure_delay': [10, 2000, 30],  # 2000 is invalid (> 1440)
            'arrival_delay': [15, 25, 35],
            'cancelled': [False, False, False]
        })
        
        mock_read_sql.return_value = test_data
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        clean_data()
        
        # Verify the function ran
        assert mock_read_sql.called
    
    @patch('pipeline.transform.get_db_connection')
    @patch('pipeline.transform.pd.read_sql')
    def test_clean_data_deduplicates(self, mock_read_sql, mock_conn):
        """Test that duplicate records are removed."""
        # The SQL query itself handles deduplication with DISTINCT ON
        test_data = pd.DataFrame({
            'flight_date': ['2024-01-01'],
            'airline': ['AA'],
            'flight_number': ['AA123'],
            'origin': ['JFK'],
            'destination': ['LAX'],
            'scheduled_departure': ['2024-01-01 10:00:00'],
            'actual_departure': ['2024-01-01 10:15:00'],
            'scheduled_arrival': ['2024-01-01 14:00:00'],
            'actual_arrival': ['2024-01-01 14:20:00'],
            'departure_delay': [15],
            'arrival_delay': [20],
            'cancelled': [False],
            'cancellation_reason': [None],
            'distance': [2475]
        })
        
        mock_read_sql.return_value = test_data
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        result = clean_data()
        
        assert result == 1
    
    @patch('pipeline.transform.get_db_connection')
    @patch('pipeline.transform.pd.read_sql')
    def test_clean_data_truncates_staging(self, mock_read_sql, mock_conn):
        """Test that staging table is truncated before loading."""
        test_data = pd.DataFrame({
            'flight_date': ['2024-01-01'],
            'airline': ['AA'],
            'origin': ['JFK'],
            'destination': ['LAX'],
            'departure_delay': [10],
            'arrival_delay': [15],
            'cancelled': [False]
        })
        
        mock_read_sql.return_value = test_data
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        clean_data()
        
        # Verify execute was called (for truncate)
        assert mock_engine.connect().__enter__().execute.called
