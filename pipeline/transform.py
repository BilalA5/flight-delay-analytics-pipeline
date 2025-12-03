"""
Data transformation module.
Cleans and transforms raw data into staging schema.
"""
import os
import pandas as pd
from sqlalchemy import create_engine, text


def get_db_connection():
    """Create database connection."""
    conn_string = os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres:5432/airflow'
    )
    return create_engine(conn_string)


def clean_data():
    """
    Transform raw data:
    - Remove duplicates
    - Handle nulls
    - Validate data types
    - Filter invalid records
    """
    print("Starting data transformation...")
    
    engine = get_db_connection()
    
    # Read from raw schema
    query = """
    SELECT DISTINCT ON (flight_date, airline, flight_number, origin, destination, scheduled_departure)
        flight_date,
        airline,
        flight_number,
        origin,
        destination,
        scheduled_departure,
        actual_departure,
        scheduled_arrival,
        actual_arrival,
        departure_delay,
        arrival_delay,
        cancelled,
        cancellation_reason,
        distance
    FROM raw.flights
    WHERE flight_date IS NOT NULL
        AND airline IS NOT NULL
        AND origin IS NOT NULL
        AND destination IS NOT NULL
    ORDER BY flight_date, airline, flight_number, origin, destination, scheduled_departure, created_at DESC
    """
    
    df = pd.read_sql(query, engine)
    print(f"Read {len(df)} records from raw.flights")
    
    # Data quality checks
    initial_count = len(df)
    
    # Remove records with invalid delays (e.g., > 24 hours)
    df = df[
        (df['departure_delay'].isna()) | 
        ((df['departure_delay'] >= -60) & (df['departure_delay'] <= 1440))
    ]
    
    df = df[
        (df['arrival_delay'].isna()) | 
        ((df['arrival_delay'] >= -60) & (df['arrival_delay'] <= 1440))
    ]
    
    print(f"Removed {initial_count - len(df)} invalid records")
    
    # Truncate staging table
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE staging.flights_clean"))
        conn.commit()
    
    # Load to staging
    df.to_sql(
        'flights_clean',
        engine,
        schema='staging',
        if_exists='append',
        index=False
    )
    
    print(f"Successfully loaded {len(df)} records to staging.flights_clean")
    
    return len(df)


if __name__ == '__main__':
    records_transformed = clean_data()
    print(f"Transformation complete: {records_transformed} records")
