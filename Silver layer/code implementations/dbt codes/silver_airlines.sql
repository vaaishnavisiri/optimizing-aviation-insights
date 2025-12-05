{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM aviation_project.airlines.SILVER_AIRLINES
),

normalized AS (
    SELECT
        IATA_CODE,
        CASE
            WHEN AIRLINE ILIKE '%American Airlines%' THEN 'American Airlines'
            WHEN AIRLINE ILIKE '%United%' THEN 'United Airlines'
            WHEN AIRLINE ILIKE '%Delta%' THEN 'Delta Airlines'
            ELSE INITCAP(AIRLINE)
        END AS AIRLINE_NAME,
        AIRLINE,
        LOAD_TIMESTAMP
    FROM base
),

derived AS (
    SELECT
        *,
        CASE
            WHEN IATA_CODE IN ('AA','UA','DL','WN') THEN 'MAJOR'
            WHEN IATA_CODE IN ('B6','NK','F9','VX') THEN 'LOW_COST'
            ELSE 'REGIONAL'
        END AS AIRLINE_CATEGORY,
        CASE
            WHEN LENGTH(IATA_CODE) = 2 THEN 'VALID'
            ELSE 'INVALID'
        END AS CODE_VALIDATION,
        TO_DATE(LOAD_TIMESTAMP) AS RECORD_DATE
    FROM normalized
)

SELECT *
FROM derived


