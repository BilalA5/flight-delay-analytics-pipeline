"""
Unit tests for validation schemas.
"""
import pytest
from datetime import datetime, date
from pydantic import ValidationError
from pipeline.schemas import FlightRecord


class TestFlightRecord:
    """Test suite for FlightRecord schema validation."""
    
    def test_valid_flight_record(self):
        """Test that valid flight record passes validation."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'LAX',
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'distance': 2475
        }
        
        record = FlightRecord(**data)
        
        assert record.airline == 'AA'
        assert record.origin == 'JFK'
        assert record.cancelled is False
    
    def test_airport_code_uppercase_validation(self):
        """Test that airport codes must be uppercase."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'jfk',  # lowercase should fail
            'destination': 'LAX',
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'distance': 2475
        }
        
        with pytest.raises(ValidationError) as exc_info:
            FlightRecord(**data)
        
        assert 'uppercase' in str(exc_info.value).lower()
    
    def test_different_airports_validation(self):
        """Test that origin and destination must be different."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'JFK',  # Same as origin
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'distance': 2475
        }
        
        with pytest.raises(ValidationError) as exc_info:
            FlightRecord(**data)
        
        assert 'different' in str(exc_info.value).lower()
    
    def test_delay_range_validation(self):
        """Test that delays must be within valid range."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'LAX',
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'departure_delay': 2000,  # Too large
            'distance': 2475
        }
        
        with pytest.raises(ValidationError) as exc_info:
            FlightRecord(**data)
        
        assert 'delay' in str(exc_info.value).lower()
    
    def test_negative_distance_validation(self):
        """Test that distance must be positive."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'LAX',
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'distance': -100  # Negative
        }
        
        with pytest.raises(ValidationError):
            FlightRecord(**data)
    
    def test_optional_fields(self):
        """Test that optional fields can be None."""
        data = {
            'flight_date': date(2024, 1, 1),
            'airline': 'AA',
            'flight_number': 'AA123',
            'origin': 'JFK',
            'destination': 'LAX',
            'scheduled_departure': datetime(2024, 1, 1, 10, 0),
            'scheduled_arrival': datetime(2024, 1, 1, 14, 0),
            'actual_departure': None,
            'actual_arrival': None,
            'departure_delay': None,
            'arrival_delay': None,
            'cancellation_reason': None,
            'distance': 2475
        }
        
        record = FlightRecord(**data)
        
        assert record.actual_departure is None
        assert record.departure_delay is None
