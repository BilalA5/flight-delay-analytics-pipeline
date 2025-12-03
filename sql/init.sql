-- Initialize database schema for flight delay analytics

-- Create raw data schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw flight data table
CREATE TABLE IF NOT EXISTS raw.flights (
    id SERIAL PRIMARY KEY,
    flight_date DATE,
    airline VARCHAR(50),
    flight_number VARCHAR(20),
    origin VARCHAR(10),
    destination VARCHAR(10),
    scheduled_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    departure_delay INTEGER,
    arrival_delay INTEGER,
    cancelled BOOLEAN,
    cancellation_reason VARCHAR(50),
    distance INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for cleaned data
CREATE TABLE IF NOT EXISTS staging.flights_clean (
    id SERIAL PRIMARY KEY,
    flight_date DATE,
    airline VARCHAR(50),
    flight_number VARCHAR(20),
    origin VARCHAR(10),
    destination VARCHAR(10),
    scheduled_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    departure_delay INTEGER,
    arrival_delay INTEGER,
    cancelled BOOLEAN,
    cancellation_reason VARCHAR(50),
    distance INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Analytics aggregation tables
CREATE TABLE IF NOT EXISTS analytics.daily_airline_stats (
    id SERIAL PRIMARY KEY,
    flight_date DATE,
    airline VARCHAR(50),
    total_flights INTEGER,
    cancelled_flights INTEGER,
    avg_departure_delay NUMERIC(10,2),
    avg_arrival_delay NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(flight_date, airline)
);

CREATE TABLE IF NOT EXISTS analytics.route_performance (
    id SERIAL PRIMARY KEY,
    origin VARCHAR(10),
    destination VARCHAR(10),
    total_flights INTEGER,
    avg_delay NUMERIC(10,2),
    on_time_percentage NUMERIC(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(origin, destination)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_flights_date ON raw.flights(flight_date);
CREATE INDEX IF NOT EXISTS idx_flights_airline ON raw.flights(airline);
CREATE INDEX IF NOT EXISTS idx_flights_route ON raw.flights(origin, destination);
CREATE INDEX IF NOT EXISTS idx_staging_date ON staging.flights_clean(flight_date);
CREATE INDEX IF NOT EXISTS idx_analytics_daily_date ON analytics.daily_airline_stats(flight_date);
