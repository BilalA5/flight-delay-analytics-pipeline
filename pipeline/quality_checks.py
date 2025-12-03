"""
Data quality checks module.
Validates data quality metrics and constraints.
"""
import os
from sqlalchemy import create_engine, text
from datetime import datetime


def get_db_connection():
    """Create database connection."""
    conn_string = os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres:5432/airflow'
    )
    return create_engine(conn_string)


def check_null_values():
    """Check for null values in critical columns."""
    print("Checking for null values...")
    
    engine = get_db_connection()
    
    query = """
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN flight_date IS NULL THEN 1 ELSE 0 END) as null_flight_date,
        SUM(CASE WHEN airline IS NULL THEN 1 ELSE 0 END) as null_airline,
        SUM(CASE WHEN origin IS NULL THEN 1 ELSE 0 END) as null_origin,
        SUM(CASE WHEN destination IS NULL THEN 1 ELSE 0 END) as null_destination
    FROM raw.flights
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()
        
        print(f"Total records: {row[0]}")
        print(f"Null flight_date: {row[1]}")
        print(f"Null airline: {row[2]}")
        print(f"Null origin: {row[3]}")
        print(f"Null destination: {row[4]}")
        
        if row[1] > 0 or row[2] > 0 or row[3] > 0 or row[4] > 0:
            print("WARNING: Null values found in critical columns")
            return False
    
    print("✓ No null values in critical columns")
    return True


def check_duplicate_records():
    """Check for duplicate records."""
    print("Checking for duplicates...")
    
    engine = get_db_connection()
    
    query = """
    SELECT COUNT(*) as duplicate_count
    FROM (
        SELECT flight_date, airline, flight_number, origin, destination, scheduled_departure
        FROM raw.flights
        GROUP BY flight_date, airline, flight_number, origin, destination, scheduled_departure
        HAVING COUNT(*) > 1
    ) duplicates
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        count = result.fetchone()[0]
        
        if count > 0:
            print(f"WARNING: Found {count} duplicate record groups")
            return False
    
    print("✓ No duplicate records found")
    return True


def check_data_freshness():
    """Check if data is recent."""
    print("Checking data freshness...")
    
    engine = get_db_connection()
    
    query = """
    SELECT MAX(created_at) as latest_record
    FROM raw.flights
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        latest = result.fetchone()[0]
        
        if latest:
            age_hours = (datetime.now() - latest).total_seconds() / 3600
            print(f"Latest record: {latest} ({age_hours:.1f} hours ago)")
            
            if age_hours > 24:
                print("WARNING: Data is older than 24 hours")
                return False
        else:
            print("WARNING: No data found")
            return False
    
    print("✓ Data is fresh")
    return True


def run_quality_checks():
    """Run all data quality checks."""
    print("=" * 50)
    print("Running Data Quality Checks")
    print("=" * 50)
    
    checks = [
        check_null_values(),
        check_duplicate_records(),
        check_data_freshness()
    ]
    
    if all(checks):
        print("\n✓ All quality checks passed")
        return True
    else:
        print("\n✗ Some quality checks failed")
        return False


if __name__ == '__main__':
    run_quality_checks()
