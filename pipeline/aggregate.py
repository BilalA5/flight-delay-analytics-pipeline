"""
Data aggregation module.
Creates analytics tables from staging data.
"""
import os
from sqlalchemy import create_engine, text


def get_db_connection():
    """Create database connection."""
    conn_string = os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres:5432/airflow'
    )
    return create_engine(conn_string)


def aggregate_daily_stats():
    """Aggregate daily airline statistics."""
    print("Aggregating daily airline statistics...")
    
    engine = get_db_connection()
    
    query = """
    INSERT INTO analytics.daily_airline_stats 
        (flight_date, airline, total_flights, cancelled_flights, avg_departure_delay, avg_arrival_delay)
    SELECT 
        flight_date,
        airline,
        COUNT(*) as total_flights,
        SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) as cancelled_flights,
        AVG(CASE WHEN NOT cancelled THEN departure_delay END) as avg_departure_delay,
        AVG(CASE WHEN NOT cancelled THEN arrival_delay END) as avg_arrival_delay
    FROM staging.flights_clean
    GROUP BY flight_date, airline
    ON CONFLICT (flight_date, airline) 
    DO UPDATE SET
        total_flights = EXCLUDED.total_flights,
        cancelled_flights = EXCLUDED.cancelled_flights,
        avg_departure_delay = EXCLUDED.avg_departure_delay,
        avg_arrival_delay = EXCLUDED.avg_arrival_delay,
        created_at = CURRENT_TIMESTAMP
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        print(f"Updated daily airline stats")
    
    return True


def aggregate_route_performance():
    """Aggregate route performance metrics."""
    print("Aggregating route performance...")
    
    engine = get_db_connection()
    
    query = """
    INSERT INTO analytics.route_performance 
        (origin, destination, total_flights, avg_delay, on_time_percentage)
    SELECT 
        origin,
        destination,
        COUNT(*) as total_flights,
        AVG(arrival_delay) as avg_delay,
        ROUND(100.0 * SUM(CASE WHEN arrival_delay <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_percentage
    FROM staging.flights_clean
    WHERE NOT cancelled
    GROUP BY origin, destination
    ON CONFLICT (origin, destination) 
    DO UPDATE SET
        total_flights = EXCLUDED.total_flights,
        avg_delay = EXCLUDED.avg_delay,
        on_time_percentage = EXCLUDED.on_time_percentage,
        created_at = CURRENT_TIMESTAMP
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        conn.commit()
        print(f"Updated route performance")
    
    return True


def run_aggregations():
    """Run all aggregations."""
    print("Starting aggregations...")
    
    aggregate_daily_stats()
    aggregate_route_performance()
    
    print("Aggregations complete")
    
    return True


if __name__ == '__main__':
    run_aggregations()
