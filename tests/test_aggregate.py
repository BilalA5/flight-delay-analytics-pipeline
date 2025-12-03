"""
Unit tests for aggregation module.
"""
import pytest
from unittest.mock import Mock, patch
from pipeline.aggregate import aggregate_daily_stats, aggregate_route_performance, run_aggregations


class TestAggregateDailyStats:
    """Test suite for daily statistics aggregation."""
    
    @patch('pipeline.aggregate.get_db_connection')
    def test_aggregate_daily_stats_executes_query(self, mock_conn):
        """Test that daily stats query is executed."""
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        result = aggregate_daily_stats()
        
        assert result is True
        mock_engine.connect().__enter__().execute.assert_called()
    
    @patch('pipeline.aggregate.get_db_connection')
    def test_aggregate_daily_stats_commits(self, mock_conn):
        """Test that transaction is committed."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect().__enter__.return_value = mock_connection
        mock_conn.return_value = mock_engine
        
        aggregate_daily_stats()
        
        mock_connection.commit.assert_called_once()


class TestAggregateRoutePerformance:
    """Test suite for route performance aggregation."""
    
    @patch('pipeline.aggregate.get_db_connection')
    def test_aggregate_route_performance_executes_query(self, mock_conn):
        """Test that route performance query is executed."""
        mock_engine = Mock()
        mock_conn.return_value = mock_engine
        
        result = aggregate_route_performance()
        
        assert result is True
        mock_engine.connect().__enter__().execute.assert_called()
    
    @patch('pipeline.aggregate.get_db_connection')
    def test_aggregate_route_performance_commits(self, mock_conn):
        """Test that transaction is committed."""
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.connect().__enter__.return_value = mock_connection
        mock_conn.return_value = mock_engine
        
        aggregate_route_performance()
        
        mock_connection.commit.assert_called_once()


class TestRunAggregations:
    """Test suite for running all aggregations."""
    
    @patch('pipeline.aggregate.aggregate_route_performance')
    @patch('pipeline.aggregate.aggregate_daily_stats')
    def test_run_aggregations_calls_all(self, mock_daily, mock_route):
        """Test that all aggregation functions are called."""
        mock_daily.return_value = True
        mock_route.return_value = True
        
        result = run_aggregations()
        
        assert result is True
        mock_daily.assert_called_once()
        mock_route.assert_called_once()
    
    @patch('pipeline.aggregate.aggregate_route_performance')
    @patch('pipeline.aggregate.aggregate_daily_stats')
    def test_run_aggregations_order(self, mock_daily, mock_route):
        """Test that aggregations are called in correct order."""
        call_order = []
        
        def daily_side_effect():
            call_order.append('daily')
            return True
        
        def route_side_effect():
            call_order.append('route')
            return True
        
        mock_daily.side_effect = daily_side_effect
        mock_route.side_effect = route_side_effect
        
        run_aggregations()
        
        assert call_order == ['daily', 'route']
