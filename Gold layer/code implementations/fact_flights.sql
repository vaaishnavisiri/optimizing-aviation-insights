{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('silver_flights_sample') }}
),

join_airline AS (
    SELECT
        s.*,
        a.AIRLINE_SK
    FROM src s
    LEFT JOIN {{ ref('dim_airlines') }} a
        ON s.AIRLINE = a.IATA_CODE
),

join_origin AS (
    SELECT
        f.*,
        o.AIRPORT_SK AS ORIGIN_AIRPORT_SK
    FROM join_airline f
    LEFT JOIN {{ ref('dim_airports') }} o
        ON f.ORIGIN_AIRPORT = o.IATA_CODE
),

join_destination AS (
    SELECT
        f.*,
        d.AIRPORT_SK AS DESTINATION_AIRPORT_SK
    FROM join_origin f
    LEFT JOIN {{ ref('dim_airports') }} d
        ON f.DESTINATION_AIRPORT = d.IATA_CODE
),

join_date AS (
    SELECT
        f.*,
        dd.DATE_SK,
        DATE_FROM_PARTS(f.YEAR, f.MONTH, f.DAY) AS FLIGHT_DATE
    FROM join_destination f
    LEFT JOIN {{ ref('dim_date') }} dd
        ON f.YEAR  = dd.YEAR
       AND f.MONTH = dd.MONTH
       AND f.DAY   = dd.DAY
)

SELECT
    DATE_SK,
    AIRLINE_SK,
    ORIGIN_AIRPORT_SK,
    DESTINATION_AIRPORT_SK,
    DEPARTURE_DELAY,
    ARRIVAL_DELAY,
    TOTAL_DELAY,
    AIR_SYSTEM_DELAY,
    SECURITY_DELAY,
    AIRLINE_DELAY,
    LATE_AIRCRAFT_DELAY,
    WEATHER_DELAY,
    DISTANCE,
    CANCELLED,
    DIVERTED,
    ON_TIME_FLAG,
    LOAD_TIMESTAMP,
    FLIGHT_DATE
FROM join_date