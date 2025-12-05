{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM aviation_project.airlines.SILVER_FLIGHTS_SAMPLE
),

enriched AS (
    SELECT
        base.*,

        -- Use macro for delay category
        {{ delay_category('TOTAL_DELAY') }} AS DELAY_CATEGORY,

        -- Distance Category
        CASE
            WHEN DISTANCE < 500 THEN 'SHORT_HAUL'
            WHEN DISTANCE BETWEEN 500 AND 1500 THEN 'MEDIUM_HAUL'
            ELSE 'LONG_HAUL'
        END AS DISTANCE_CATEGORY,

        -- Day Type
        CASE
            WHEN DAY_OF_WEEK IN (1,7) THEN 'WEEKEND'
            ELSE 'WEEKDAY'
        END AS DAY_TYPE,

        -- Flight Status
        CASE 
            WHEN CANCELLED = 1 THEN 'CANCELLED'
            WHEN DIVERTED = 1 THEN 'DIVERTED'
            ELSE 'COMPLETED'
        END AS FLIGHT_STATUS,

        -- Weather Impact
        CASE 
            WHEN WEATHER_DELAY > 30 THEN 'WEATHER_HIGH'
            WHEN WEATHER_DELAY > 0 THEN 'WEATHER_LOW'
            ELSE 'NO_WEATHER_IMPACT'
        END AS WEATHER_IMPACT,

        -- On-time Flag
        CASE
            WHEN TOTAL_DELAY <= 0 THEN 1 
            ELSE 0
        END AS ON_TIME_FLAG,

        -- Record Date
        TO_DATE(LOAD_TIMESTAMP) AS RECORD_DATE

    FROM base
)

SELECT *
FROM enriched