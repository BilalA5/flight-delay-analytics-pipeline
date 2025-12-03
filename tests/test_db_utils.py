"""
Unit tests for database utilities.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pipeline.db_utils import get_connection_string, get_db_session, execute_query, execute_update


class TestGetConnectionString:
    """Test suite for connection string generation."""
    
    @patch.dict('os.environ', {'DATABASE_URL': 'postgresql://custom:custom@custom:5432/custom'})
    def test_get_connection_string_from_env(self):
        """Test getting connection string from environment."""
        result = get_connection_string()
        assert result == 'postgresql://custom:custom@custom:5432/custom'
    
    @patch.dict('os.environ', {}, clear=True)
    def test_get_connection_string_default(self):
        """Test default connection string."""
        result = get_connection_string()
        assert result == 'postgresql://airflow:airflow@postgres:5432/airflow'


class TestGetDbSession:
    """Test suite for database session context manager."""
    
    @patch('pipeline.db_utils.create_engine')
    def test_get_db_session_yields_connection(self, mock_create_engine):
        """Test that session yields a connection."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        with get_db_session() as session:
            assert session == mock_connection
    
    @patch('pipeline.db_utils.create_engine')
    def test_get_db_session_closes_connection(self, mock_create_engine):
        """Test that connection is closed after use."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        with get_db_session() as session:
            pass
        
        mock_connection.close.assert_called_once()
        mock_engine.dispose.assert_called_once()


class TestExecuteQuery:
    """Test suite for query execution."""
    
    @patch('pipeline.db_utils.get_db_session')
    def test_execute_query_without_params(self, mock_session):
        """Test executing query without parameters."""
        mock_connection = MagicMock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [('row1',), ('row2',)]
        mock_connection.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_connection
        
        result = execute_query("SELECT * FROM test")
        
        assert result == [('row1',), ('row2',)]
        mock_connection.execute.assert_called_once()
    
    @patch('pipeline.db_utils.get_db_session')
    def test_execute_query_with_params(self, mock_session):
        """Test executing query with parameters."""
        mock_connection = MagicMock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [('filtered',)]
        mock_connection.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_connection
        
        params = {'id': 1}
        result = execute_query("SELECT * FROM test WHERE id = :id", params)
        
        assert result == [('filtered',)]


class TestExecuteUpdate:
    """Test suite for update execution."""
    
    @patch('pipeline.db_utils.get_db_session')
    def test_execute_update_returns_rowcount(self, mock_session):
        """Test that execute_update returns number of affected rows."""
        mock_connection = MagicMock()
        mock_result = Mock()
        mock_result.rowcount = 5
        mock_connection.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_connection
        
        result = execute_update("UPDATE test SET value = 1")
        
        assert result == 5
        mock_connection.commit.assert_called_once()
