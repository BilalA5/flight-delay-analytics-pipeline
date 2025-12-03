-- Daily airline performance metrics
-- Aggregates flight statistics by date and airline

{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
    flight_date,
    airline,
    COUNT(*) as total_flights,
    SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) as cancelled_flights,
    ROUND(AVG(CASE WHEN NOT cancelled THEN departure_delay END), 2) as avg_departure_delay,
    ROUND(AVG(CASE WHEN NOT cancelled THEN arrival_delay END), 2) as avg_arrival_delay,
    ROUND(100.0 * SUM(CASE WHEN NOT cancelled AND arrival_delay <= 15 THEN 1 ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN NOT cancelled THEN 1 ELSE 0 END), 0), 2) as on_time_percentage,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('stg_flights') }}
GROUP BY flight_date, airline
