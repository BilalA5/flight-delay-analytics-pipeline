"""
Unit tests for ingestion module.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime
from pipeline.ingest import generate_sample_data, ingest_data


class TestGenerateSampleData:
    """Test suite for sample data generation."""
    
    def test_generate_sample_data_count(self):
        """Test that correct number of records are generated."""
        num_records = 100
        df = generate_sample_data(num_records)
        
        assert len(df) == num_records
        assert isinstance(df, pd.DataFrame)
    
    def test_generate_sample_data_columns(self):
        """Test that all required columns are present."""
        df = generate_sample_data(10)
        
        required_columns = [
            'flight_date', 'airline', 'flight_number', 'origin', 
            'destination', 'scheduled_departure', 'actual_departure',
            'scheduled_arrival', 'actual_arrival', 'departure_delay',
            'arrival_delay', 'cancelled', 'cancellation_reason', 'distance'
        ]
        
        for col in required_columns:
            assert col in df.columns
    
    def test_generate_sample_data_types(self):
        """Test that data types are correct."""
        df = generate_sample_data(10)
        
        assert df['airline'].dtype == 'object'
        assert df['cancelled'].dtype == 'bool'
        assert df['distance'].dtype == 'int64'
    
    def test_cancelled_flights_have_null_delays(self):
        """Test that cancelled flights have null actual times."""
        df = generate_sample_data(1000)
        cancelled = df[df['cancelled'] == True]
        
        if len(cancelled) > 0:
            assert cancelled['actual_departure'].isna().all()
            assert cancelled['actual_arrival'].isna().all()
    
    def test_origin_destination_different(self):
        """Test that origin and destination are always different."""
        df = generate_sample_data(100)
        
        assert (df['origin'] != df['destination']).all()


class TestIngestData:
    """Test suite for data ingestion."""
    
    @patch('pipeline.ingest.get_db_connection')
    @patch('pipeline.ingest.generate_sample_data')
    def test_ingest_data_success(self, mock_generate, mock_conn):
        """Test successful data ingestion."""
        # Setup mocks
        mock_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_generate.return_value = mock_df
        
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        # Execute
        result = ingest_data()
        
        # Verify
        assert result == 1000
        mock_generate.assert_called_once_with(num_records=1000)
    
    @patch('pipeline.ingest.get_db_connection')
    @patch('pipeline.ingest.generate_sample_data')
    def test_ingest_data_calls_to_sql(self, mock_generate, mock_conn):
        """Test that to_sql is called with correct parameters."""
        mock_df = MagicMock()
        mock_generate.return_value = mock_df
        
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        ingest_data()
        
        mock_df.to_sql.assert_called_once()
        call_args = mock_df.to_sql.call_args
        assert call_args[0][0] == 'flights'
        assert call_args[1]['schema'] == 'raw'
        assert call_args[1]['if_exists'] == 'append'
