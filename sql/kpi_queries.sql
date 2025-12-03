-- KPI Queries for Flight Delay Analytics

-- 1. Overall delay statistics by airline
SELECT 
    airline,
    COUNT(*) as total_flights,
    AVG(departure_delay) as avg_departure_delay,
    AVG(arrival_delay) as avg_arrival_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY arrival_delay) as median_arrival_delay,
    SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) as cancelled_count,
    ROUND(100.0 * SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) / COUNT(*), 2) as cancellation_rate
FROM staging.flights_clean
GROUP BY airline
ORDER BY avg_arrival_delay DESC;

-- 2. Route performance analysis
SELECT 
    origin,
    destination,
    COUNT(*) as total_flights,
    AVG(arrival_delay) as avg_delay,
    ROUND(100.0 * SUM(CASE WHEN arrival_delay <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_percentage
FROM staging.flights_clean
WHERE NOT cancelled
GROUP BY origin, destination
HAVING COUNT(*) >= 10
ORDER BY on_time_percentage ASC
LIMIT 20;

-- 3. Daily trends
SELECT 
    flight_date,
    COUNT(*) as total_flights,
    AVG(departure_delay) as avg_departure_delay,
    AVG(arrival_delay) as avg_arrival_delay,
    SUM(CASE WHEN cancelled THEN 1 ELSE 0 END) as cancelled_flights
FROM staging.flights_clean
GROUP BY flight_date
ORDER BY flight_date DESC
LIMIT 30;

-- 4. Worst performing routes
SELECT 
    origin,
    destination,
    COUNT(*) as flight_count,
    AVG(arrival_delay) as avg_delay,
    MAX(arrival_delay) as max_delay
FROM staging.flights_clean
WHERE NOT cancelled
GROUP BY origin, destination
HAVING COUNT(*) >= 5
ORDER BY avg_delay DESC
LIMIT 10;

-- 5. Cancellation reasons breakdown
SELECT 
    cancellation_reason,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM staging.flights_clean
WHERE cancelled = TRUE
GROUP BY cancellation_reason
ORDER BY count DESC;
