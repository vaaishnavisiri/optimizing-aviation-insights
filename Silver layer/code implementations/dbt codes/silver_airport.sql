{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM aviation_project.airlines.SILVER_AIRPORTS
),

enhanced AS (
    SELECT
        IATA_CODE,
        CITY,
        STATE,
        COUNTRY,
        LATITUDE,
        LONGITUDE,
        LOAD_TIMESTAMP,

        -- Derived attributes using macros
        {{ airport_size('STATE', 'COUNTRY') }} AS AIRPORT_SIZE_CATEGORY,
        {{ airport_region('STATE') }} AS REGION,
        {{ airport_continent('COUNTRY') }} AS CONTINENT_GROUP,

        -- Validation
        CASE
            WHEN LATITUDE BETWEEN -90 AND 90
             AND LONGITUDE BETWEEN -180 AND 180
             AND CITY IS NOT NULL THEN 'VALID'
            ELSE 'INVALID'
        END AS LOCATION_VALIDATION,

        -- Record year
        YEAR(LOAD_TIMESTAMP) AS RECORD_YEAR

    FROM base
)

SELECT *
FROM enhanced