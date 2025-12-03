"""
Data ingestion module for flight delay data.
Fetches data from source and loads into raw schema.
"""
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import random


def get_db_connection():
    """Create database connection."""
    conn_string = os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres:5432/airflow'
    )
    return create_engine(conn_string)


def generate_sample_data(num_records=1000):
    """
    Generate sample flight data for demonstration.
    In production, this would fetch from an API or file source.
    """
    airlines = ['AA', 'DL', 'UA', 'WN', 'B6']
    airports = ['JFK', 'LAX', 'ORD', 'DFW', 'ATL', 'SFO', 'BOS', 'MIA', 'SEA', 'DEN']
    cancellation_reasons = ['Weather', 'Carrier', 'NAS', 'Security', None]
    
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_records):
        flight_date = base_date + timedelta(days=random.randint(0, 30))
        airline = random.choice(airlines)
        origin = random.choice(airports)
        destination = random.choice([a for a in airports if a != origin])
        
        scheduled_dep = flight_date.replace(
            hour=random.randint(6, 22),
            minute=random.choice([0, 15, 30, 45])
        )
        
        cancelled = random.random() < 0.05  # 5% cancellation rate
        
        if cancelled:
            actual_dep = None
            actual_arr = None
            dep_delay = None
            arr_delay = None
            cancellation_reason = random.choice([r for r in cancellation_reasons if r])
        else:
            dep_delay = random.randint(-10, 120) if random.random() < 0.7 else random.randint(-10, 30)
            actual_dep = scheduled_dep + timedelta(minutes=dep_delay)
            
            flight_duration = random.randint(60, 360)
            scheduled_arr = scheduled_dep + timedelta(minutes=flight_duration)
            
            arr_delay = dep_delay + random.randint(-15, 30)
            actual_arr = scheduled_arr + timedelta(minutes=arr_delay)
            cancellation_reason = None
        
        data.append({
            'flight_date': flight_date.date(),
            'airline': airline,
            'flight_number': f'{airline}{random.randint(100, 9999)}',
            'origin': origin,
            'destination': destination,
            'scheduled_departure': scheduled_dep,
            'actual_departure': actual_dep,
            'scheduled_arrival': scheduled_dep + timedelta(minutes=random.randint(60, 360)),
            'actual_arrival': actual_arr,
            'departure_delay': dep_delay,
            'arrival_delay': arr_delay,
            'cancelled': cancelled,
            'cancellation_reason': cancellation_reason,
            'distance': random.randint(200, 3000)
        })
    
    return pd.DataFrame(data)


def ingest_data():
    """Main ingestion function."""
    print("Starting data ingestion...")
    
    # Generate sample data
    df = generate_sample_data(num_records=1000)
    print(f"Generated {len(df)} flight records")
    
    # Connect to database
    engine = get_db_connection()
    
    # Load to raw schema
    df.to_sql(
        'flights',
        engine,
        schema='raw',
        if_exists='append',
        index=False
    )
    
    print(f"Successfully loaded {len(df)} records to raw.flights")
    
    return len(df)


if __name__ == '__main__':
    records_loaded = ingest_data()
    print(f"Ingestion complete: {records_loaded} records")
