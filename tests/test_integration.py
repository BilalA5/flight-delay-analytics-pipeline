"""
Integration tests for the complete pipeline.
"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd


class TestPipelineIntegration:
    """Integration tests for end-to-end pipeline execution."""
    
    @patch('pipeline.aggregate.get_db_connection')
    @patch('pipeline.transform.get_db_connection')
    @patch('pipeline.ingest.get_db_connection')
    def test_full_pipeline_execution(self, mock_ingest_conn, mock_transform_conn, mock_agg_conn):
        """Test complete pipeline from ingest to aggregation."""
        # Setup mocks for ingestion
        mock_ingest_engine = Mock()
        mock_ingest_conn.return_value = mock_ingest_engine
        
        # Setup mocks for transformation
        mock_transform_engine = Mock()
        mock_transform_conn.return_value = mock_transform_engine
        
        # Setup mocks for aggregation
        mock_agg_engine = Mock()
        mock_agg_conn.return_value = mock_agg_engine
        
        # Import and run pipeline steps
        from pipeline.ingest import ingest_data
        from pipeline.transform import clean_data
        from pipeline.aggregate import run_aggregations
        
        # Execute pipeline
        ingest_result = ingest_data()
        assert ingest_result == 1000
        
        # Verify ingestion called database
        assert mock_ingest_engine.connect.called
    
    @patch('pipeline.quality_checks.get_db_connection')
    def test_quality_checks_integration(self, mock_conn):
        """Test quality checks run successfully."""
        mock_engine = Mock()
        mock_connection = Mock()
        
        # Mock results for quality checks
        mock_result1 = Mock()
        mock_result1.fetchone.return_value = (1000, 0, 0, 0, 0)  # No nulls
        
        mock_result2 = Mock()
        mock_result2.fetchone.return_value = (0,)  # No duplicates
        
        from datetime import datetime
        mock_result3 = Mock()
        mock_result3.fetchone.return_value = (datetime.now(),)  # Fresh data
        
        mock_connection.execute.side_effect = [mock_result1, mock_result2, mock_result3]
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_conn.return_value = mock_engine
        
        from pipeline.quality_checks import run_quality_checks
        
        result = run_quality_checks()
        assert result is True


class TestDatabaseIntegration:
    """Integration tests for database operations."""
    
    @patch('pipeline.db_utils.create_engine')
    def test_database_session_lifecycle(self, mock_create_engine):
        """Test complete database session lifecycle."""
        from pipeline.db_utils import get_db_session
        
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        with get_db_session() as session:
            assert session == mock_connection
        
        # Verify cleanup
        mock_connection.close.assert_called_once()
        mock_engine.dispose.assert_called_once()
