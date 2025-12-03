-- Staging model: Clean and deduplicate raw flight data
-- This model serves as the foundation for all analytics

{{ config(
    materialized='view',
    schema='staging'
) }}

WITH deduplicated AS (
    SELECT DISTINCT ON (
        flight_date, 
        airline, 
        flight_number, 
        origin, 
        destination, 
        scheduled_departure
    )
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
        distance,
        created_at
    FROM {{ source('raw', 'flights') }}
    WHERE flight_date IS NOT NULL
        AND airline IS NOT NULL
        AND origin IS NOT NULL
        AND destination IS NOT NULL
    ORDER BY 
        flight_date, 
        airline, 
        flight_number, 
        origin, 
        destination, 
        scheduled_departure,
        created_at DESC
),

cleaned AS (
    SELECT *
    FROM deduplicated
    WHERE 
        -- Filter out invalid delays (> 24 hours)
        (departure_delay IS NULL OR (departure_delay >= -60 AND departure_delay <= 1440))
        AND (arrival_delay IS NULL OR (arrival_delay >= -60 AND arrival_delay <= 1440))
)

SELECT * FROM cleaned
