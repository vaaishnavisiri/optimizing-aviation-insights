{{ config(materialized='table') }}

WITH src AS (
    SELECT *
    FROM {{ ref('silver_airports') }}
)

SELECT
    {{ generate_sk(["IATA_CODE"]) }} AS AIRPORT_SK,
    IATA_CODE,
    CITY,
    STATE,
    COUNTRY,
    LATITUDE,
    LONGITUDE,
    REGION,
    CONTINENT_GROUP,
    AIRPORT_SIZE_CATEGORY,
    LOCATION_VALIDATION,
    RECORD_YEAR,
    LOAD_TIMESTAMP
FROM src
