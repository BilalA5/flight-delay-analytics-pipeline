"""
Pytest configuration and shared fixtures.
"""
import pytest
from unittest.mock import Mock
from sqlalchemy import create_engine


@pytest.fixture
def mock_db_engine():
    """Mock database engine for testing."""
    engine = Mock()
    connection = Mock()
    engine.connect.return_value.__enter__.return_value = connection
    return engine


@pytest.fixture
def sample_flight_data():
    """Sample flight data for testing."""
    return {
        'flight_date': '2024-01-01',
        'airline': 'AA',
        'flight_number': 'AA123',
        'origin': 'JFK',
        'destination': 'LAX',
        'scheduled_departure': '2024-01-01 10:00:00',
        'actual_departure': '2024-01-01 10:15:00',
        'scheduled_arrival': '2024-01-01 14:00:00',
        'actual_arrival': '2024-01-01 14:20:00',
        'departure_delay': 15,
        'arrival_delay': 20,
        'cancelled': False,
        'cancellation_reason': None,
        'distance': 2475
    }


@pytest.fixture
def sample_flight_records():
    """Multiple sample flight records."""
    return [
        {
            'flight_date': '2024-01-01',
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'LAX',
            'departure_delay': 15,
            'arrival_delay': 20,
            'cancelled': False
        },
        {
            'flight_date': '2024-01-01',
            'airline': 'DL',
            'flight_number': 'DL456',
            'origin': 'ATL',
            'destination': 'SFO',
            'departure_delay': -5,
            'arrival_delay': 0,
            'cancelled': False
        },
        {
            'flight_date': '2024-01-01',
            'airline': 'UA',
            'flight_number': 'UA789',
            'origin': 'ORD',
            'destination': 'DEN',
            'departure_delay': None,
            'arrival_delay': None,
            'cancelled': True
        }
    ]
