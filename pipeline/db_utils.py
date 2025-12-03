"""
Database utility functions.
"""
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import os


def get_connection_string():
    """Get database connection string from environment."""
    return os.getenv(
        'DATABASE_URL',
        'postgresql://airflow:airflow@postgres:5432/airflow'
    )


@contextmanager
def get_db_session():
    """
    Context manager for database sessions.
    
    Usage:
        with get_db_session() as session:
            result = session.execute(query)
    """
    engine = create_engine(get_connection_string())
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


def execute_query(query, params=None):
    """
    Execute a SQL query and return results.
    
    Args:
        query: SQL query string
        params: Optional query parameters
    
    Returns:
        Query results
    """
    with get_db_session() as session:
        if params:
            result = session.execute(text(query), params)
        else:
            result = session.execute(text(query))
        return result.fetchall()


def execute_update(query, params=None):
    """
    Execute an UPDATE/INSERT/DELETE query.
    
    Args:
        query: SQL query string
        params: Optional query parameters
    
    Returns:
        Number of affected rows
    """
    with get_db_session() as session:
        if params:
            result = session.execute(text(query), params)
        else:
            result = session.execute(text(query))
        session.commit()
        return result.rowcount
