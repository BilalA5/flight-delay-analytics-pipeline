-- Route performance analysis
-- Aggregates metrics by origin-destination pairs

{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT 
    origin,
    destination,
    COUNT(*) as total_flights,
    ROUND(AVG(arrival_delay), 2) as avg_delay,
    ROUND(100.0 * SUM(CASE WHEN arrival_delay <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_percentage,
    ROUND(AVG(distance), 0) as avg_distance,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('stg_flights') }}
WHERE NOT cancelled
GROUP BY origin, destination
HAVING COUNT(*) >= 5  -- Only routes with at least 5 flights
