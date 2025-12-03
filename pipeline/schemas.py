"""
Data validation schemas using Pydantic.
"""
from pydantic import BaseModel, Field, validator
from datetime import datetime, date
from typing import Optional


class FlightRecord(BaseModel):
    """Schema for flight record validation."""
    
    flight_date: date
    airline: str = Field(..., min_length=2, max_length=50)
    flight_number: str = Field(..., min_length=1, max_length=20)
    origin: str = Field(..., min_length=3, max_length=10)
    destination: str = Field(..., min_length=3, max_length=10)
    scheduled_departure: datetime
    actual_departure: Optional[datetime] = None
    scheduled_arrival: datetime
    actual_arrival: Optional[datetime] = None
    departure_delay: Optional[int] = None
    arrival_delay: Optional[int] = None
    cancelled: bool = False
    cancellation_reason: Optional[str] = Field(None, max_length=50)
    distance: int = Field(..., gt=0)
    
    @validator('origin', 'destination')
    def validate_airport_code(cls, v):
        """Validate airport codes are uppercase."""
        if not v.isupper():
            raise ValueError('Airport codes must be uppercase')
        return v
    
    @validator('destination')
    def validate_different_airports(cls, v, values):
        """Ensure origin and destination are different."""
        if 'origin' in values and v == values['origin']:
            raise ValueError('Origin and destination must be different')
        return v
    
    @validator('departure_delay', 'arrival_delay')
    def validate_delay_range(cls, v):
        """Validate delay is within reasonable range."""
        if v is not None and (v < -60 or v > 1440):
            raise ValueError('Delay must be between -60 and 1440 minutes')
        return v
    
    class Config:
        """Pydantic config."""
        validate_assignment = True
